import os
import pandas as pd
import lightgbm as lgb
import joblib
from pathlib import Path
from sklearn import logger
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, log_loss, precision_score, recall_score
import re
import numpy as np
import optuna
from collections import defaultdict

class LightGBMModel:
    def __init__(
        self,
        label_col="label",
        model_path="lightgbm_model.pkl",
        chunk_size=200_000
    ):
        self.label_col = label_col
        self.model_path = os.getcwd() + "/machine_learning/models/" + model_path
        self.chunk_size = chunk_size
        self.model = None
        self.label_encoder = LabelEncoder()

        input_path = os.getcwd() + "/machine_learning/data_handler"
        self.data_file = (
            Path(input_path) / "preprocessed_data" / "data_changed_label.csv"
        )
        
        self.test_data_file = None

        self.feature_cols = ["ticker_id", "overall_sentiment_score", "relevance_score", "ticker_sentiment_score", "close_price"]
        self.tuned_params = None

    def count_rows(self):
        total = 0
        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            total += len(chunk)
        return total
    
    def stratified_split_to_files(
        self,
        train_ratio=0.7,
        val_ratio=0.15,
        output_dir="machine_learning/models/datasets",
    ):
        os.makedirs(output_dir, exist_ok=True)

        train_file = Path(output_dir) / "train.csv"
        val_file = Path(output_dir) / "val.csv"
        test_file = Path(output_dir) / "test.csv"

        # remove old files
        for f in [train_file, val_file, test_file]:
            if f.exists():
                f.unlink()

        self.fit_label_encoder()
        label_counts = self.count_labels()

        # quota per label
        quotas = {}
        for label_str, total in label_counts.items():
            label = self.label_encoder.transform([label_str])[0]
            quotas[label] = {
                "train": int(total * train_ratio),
                "val": int(total * val_ratio),
                "test": total - int(total * train_ratio) - int(total * val_ratio),
            }

        used = {
            "train": {l: 0 for l in quotas},
            "val": {l: 0 for l in quotas},
            "test": {l: 0 for l in quotas},
        }

        first_write = {"train": True, "val": True, "test": True}

        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            chunk = chunk.drop(columns=["ticker", "date"], errors="ignore")
            chunk["label_encoded"] = self.label_encoder.transform(
                chunk[self.label_col].astype(str)
            )

            for label in quotas:
                df_l = chunk[chunk["label_encoded"] == label]

                if df_l.empty:
                    continue

                df_l = df_l.sample(frac=1, random_state=42)  # shuffle within label

                # -------- TRAIN --------
                remain = quotas[label]["train"] - used["train"][label]
                if remain > 0:
                    take = min(len(df_l), remain)
                    part = df_l.iloc[:take]
                    part.to_csv(
                        train_file,
                        mode="a",
                        index=False,
                        header=first_write["train"],
                    )
                    first_write["train"] = False
                    used["train"][label] += take
                    df_l = df_l.iloc[take:]

                # -------- VAL --------
                remain = quotas[label]["val"] - used["val"][label]
                if remain > 0 and not df_l.empty:
                    take = min(len(df_l), remain)
                    part = df_l.iloc[:take]
                    part.to_csv(
                        val_file,
                        mode="a",
                        index=False,
                        header=first_write["val"],
                    )
                    first_write["val"] = False
                    used["val"][label] += take
                    df_l = df_l.iloc[take:]

                # -------- TEST --------
                remain = quotas[label]["test"] - used["test"][label]
                if remain > 0 and not df_l.empty:
                    take = min(len(df_l), remain)
                    part = df_l.iloc[:take]
                    part.to_csv(
                        test_file,
                        mode="a",
                        index=False,
                        header=first_write["test"],
                    )
                    first_write["test"] = False
                    used["test"][label] += take

            # stop early if done
            if all(
                used[s][l] >= quotas[l][s]
                for s in ["train", "val", "test"]
                for l in quotas
            ):
                break

        print("[INFO] Stratified split done")
        print("Train:", used["train"])
        print("Val:", used["val"])
        print("Test:", used["test"])



    # --------------------------------------------------
    # 1️⃣ Scan label trước (RAM nhỏ)
    # --------------------------------------------------
    def fit_label_encoder(self):
        labels = []
        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            labels.extend(chunk[self.label_col].astype(str).unique())

        self.label_encoder.fit(labels)
        print("[INFO] LabelEncoder fitted:", self.label_encoder.classes_)


    def _init_stratified_quota(self, total_per_label, ratios):
        """
        ratios = dict(train=0.7, val=0.15, test=0.15)
        """
        quotas = {}
        for label, total in total_per_label.items():
            quotas[label] = {
                "train": int(total * ratios["train"]),
                "val": int(total * ratios["val"]),
                "test": total - int(total * ratios["train"]) - int(total * ratios["val"]),
            }
        return quotas
    
    def count_labels(self):
        counter = defaultdict(int)
        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            for l in chunk[self.label_col].astype(str):
                counter[l] += 1
        return counter


    # --------------------------------------------------
    # 2️⃣ Train theo chunk
    # --------------------------------------------------
    def train(self, params=None, num_boost_round=500):
        self.fit_label_encoder()
        if params is None:
            params = {
                "objective": "multiclass",
                "num_class": len(self.label_encoder.classes_),
                "metric": ["multi_logloss", "multi_error"],
                "learning_rate": 0.05,
                "num_leaves": 31,
                "max_depth": -1,
                "verbose": -1,
                "class_weight": "balanced",
                "categorical_feature": ["name:ticker_id"] #non continuous feature
            }

        train_file = "machine_learning/models/datasets/train.csv"
        val_file = "machine_learning/models/datasets/val.csv"

        booster = None

        train_set = lgb.Dataset(
            pd.read_csv(train_file, chunksize=self.chunk_size)
            .get_chunk(self.chunk_size)[self.feature_cols],
            label=None,
        )
        
        for i, chunk in enumerate(pd.read_csv(train_file, chunksize=self.chunk_size)):
            chunk = chunk.drop(columns=["ticker", "date"], errors="ignore")
            chunk["label_encoded"] = self.label_encoder.transform(
                chunk[self.label_col].astype(str)
            )

            X = chunk[self.feature_cols]
            y = chunk["label_encoded"]

            train_set = lgb.Dataset(X, y, free_raw_data=False)

            if i == 0:
                val_df = pd.read_csv(val_file)
                val_df = val_df.drop(columns=["ticker", "date"], errors="ignore")
                val_df["label_encoded"] = self.label_encoder.transform(
                    val_df[self.label_col].astype(str)
                )
                val_set = lgb.Dataset(
                    val_df[self.feature_cols],
                    val_df["label_encoded"],
                    free_raw_data=False,
                )

            booster = lgb.train(
                params,
                train_set,
                num_boost_round=num_boost_round,
                init_model=booster,
                valid_sets=[val_set],
                callbacks=[
                    lgb.early_stopping(50),
                    lgb.log_evaluation(50),
                ],
            )

        print(f"[INFO] Trained chunk {i + 1}")

        self.model = booster
        self.save_model()



    # --------------------------------------------------
    # 3️⃣ Evaluate (sample-based, RAM an toàn)
    # --------------------------------------------------
    def evaluate(self, export_inference_result=False):
        self.load_model()

        test_file = "machine_learning/models/datasets/test.csv"

        y_true_all, y_pred_all, y_proba_all = [], [], []

        for chunk in pd.read_csv(test_file, chunksize=self.chunk_size):
            chunk = chunk.drop(columns=["ticker", "date"], errors="ignore")
            chunk["label_encoded"] = self.label_encoder.transform(
                chunk[self.label_col].astype(str)
            )

            X = chunk[self.feature_cols]
            y_true = chunk["label_encoded"].values

            proba = self.model.predict(X)
            y_pred = proba.argmax(axis=1)

            y_true_all.extend(y_true)
            y_pred_all.extend(y_pred)
            y_proba_all.extend(proba)

        results = {
            "log_loss": log_loss(y_true_all, y_proba_all),
            "precision_macro": precision_score(y_true_all, y_pred_all, average="macro"),
            "recall_macro": recall_score(y_true_all, y_pred_all, average="macro"),
            "confusion_matrix": confusion_matrix(y_true_all, y_pred_all),
            "classification_report": classification_report(
                y_true_all,
                y_pred_all,
                target_names=self.label_encoder.classes_,
                zero_division=0,
            ),
            "classification_report": classification_report(
                y_true_all,
                y_pred_all,
                target_names=self.label_encoder.classes_,
                output_dict=True,
                zero_division=0,
            ),
            "confusion_matrix": confusion_matrix(y_true_all, y_pred_all),
            "y_true": y_true_all,
            "y_pred": y_pred_all,
            "y_proba": y_proba_all,
            "labels": self.label_encoder.classes_,
            "data_file": test_file,
            "chunk_size": self.chunk_size,
        }
        
        if export_inference_result:
            #delete existing file if exists then export to results/inference_results.csv by copying test_data.csv file and adding label_predicted columns
            inference_file = os.getcwd() + "/machine_learning/models/results/inference_results.csv"
            if os.path.exists(inference_file):
                os.remove(inference_file)

            df_iter = pd.read_csv(test_file, chunksize=self.chunk_size)
            first_write = True
            for chunk in df_iter:
                if not chunk.empty:
                    chunk["label_predicted"] = self.label_encoder.inverse_transform(y_pred_all)
                    chunk.to_csv(
                        inference_file,
                        mode='a',
                        index=False,
                        header=first_write
                    )
                    first_write = False
                print(f"[INFO] inference result file created in chunk: {inference_file}")
        return results



    # --------------------------------------------------
    # 4️⃣ Infer
    # --------------------------------------------------
    def infer(self, df: pd.DataFrame):
        pass

    # --------------------------------------------------
    # 5️⃣ Save / Load
    # --------------------------------------------------
    def save_model(self):
        versions = self._get_all_model_versions()
        next_version = 1 if not versions else versions[-1][0] + 1
        version_str = f"{next_version:03d}"

        base = Path(self.model_path)
        save_path = base.with_name(f"{base.stem}_v{version_str}.pkl")
        joblib.dump(
            {
                "model": self.model,
                "label_encoder": self.label_encoder,
                "feature_cols": self.feature_cols,
                "version": next_version,
            },
            save_path,
        )
        print(f"[INFO] Model saved to {save_path}")

    def load_model(self, version: int | None = None):
        versions = self._get_all_model_versions()
        
        if not versions:
            raise FileNotFoundError("No saved model found.")

        if version is None:
            # Load latest
            selected_version, model_path = versions[-1]
        else:
            matches = [v for v in versions if v[0] == version]
            if not matches:
                raise ValueError(f"Model version v{version:03d} not found.")
            selected_version, model_path = matches[0]
        
        data = joblib.load(model_path)
        self.model = data["model"]
        self.label_encoder = data["label_encoder"]
        self.feature_cols = data["feature_cols"]
        
        print(f"[INFO] Model v{selected_version:03d} loaded from {model_path}")
        
    def _get_all_model_versions(self):
        model_dir = Path(self.model_path).parent
        base_name = Path(self.model_path).stem

        pattern = re.compile(rf"{base_name}_v(\d+)\.pkl")

        versions = []
        for f in model_dir.glob(f"{base_name}_v*.pkl"):
            match = pattern.match(f.name)
            if match:
                versions.append((int(match.group(1)), f))

        return sorted(versions, key=lambda x: x[0])
    
    def tune_hyperparameters(
        self,
        n_trials=50,
        timeout=None,
        num_boost_round=500,
    ):
        self.fit_label_encoder()

        train_df = pd.read_csv("machine_learning/models/datasets/train.csv")
        val_df = pd.read_csv("machine_learning/models/datasets/val.csv")

        for df in [train_df, val_df]:
            df.drop(columns=["ticker", "date"], errors="ignore", inplace=True)
            df["label_encoded"] = self.label_encoder.transform(
                df[self.label_col].astype(str)
            )

        train_set = lgb.Dataset(
            train_df[self.feature_cols],
            train_df["label_encoded"],
            free_raw_data=False,
        )
        val_set = lgb.Dataset(
            val_df[self.feature_cols],
            val_df["label_encoded"],
            free_raw_data=False,
        )

        def objective(trial):
            params = {
                "objective": "multiclass",
                "num_class": len(self.label_encoder.classes_),
                "metric": ["multi_logloss", "multi_error"],

                # 🔽 trainable params
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
                "num_leaves": trial.suggest_int("num_leaves", 16, 128),
                "max_depth": trial.suggest_int("max_depth", 3, 12),
                "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 20, 200),
                "feature_fraction": trial.suggest_float("feature_fraction", 0.6, 1.0),
                "bagging_fraction": trial.suggest_float("bagging_fraction", 0.6, 1.0),
                "bagging_freq": 1,
                "lambda_l1": trial.suggest_float("lambda_l1", 1e-4, 10.0, log=True),
                "lambda_l2": trial.suggest_float("lambda_l2", 1e-4, 10.0, log=True),

                # fixed
                "class_weight": "balanced",
                "verbose": -1,
                "feature_pre_filter": False,
            }

            booster = lgb.train(
                params,
                train_set,
                num_boost_round=num_boost_round,
                valid_sets=[val_set],
                callbacks=[lgb.early_stopping(50, first_metric_only=True), lgb.log_evaluation(period=50)],
            )

            return booster.best_score["valid_0"]["multi_logloss"]

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=n_trials, timeout=timeout)

        # 👉 Gán vào self để dùng cho train()
        self.tuned_params = {
            **study.best_params,
            "objective": "multiclass",
            "num_class": len(self.label_encoder.classes_),
            "metric": ["multi_logloss", "multi_error"],
            "class_weight": "balanced",
            "verbose": -1,
        }

        print("[INFO] Optuna best score:", study.best_value)
        print("[INFO] Optuna best params:", self.tuned_params)

        return self.tuned_params