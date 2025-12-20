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
            Path(input_path) / "preprocessed_data" / "data.csv"
        )

        self.feature_cols = None
        self.tuned_params = None

    def count_rows(self):
        total = 0
        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            total += len(chunk)
        return total

    def compute_row_splits(self):
        total_rows = self.count_rows()

        self.train_end = int(total_rows * 0.7)
        self.val_end = int(total_rows * 0.85)

        print("[INFO] Total rows:", total_rows)
        print("[INFO] Train end row:", self.train_end)
        print("[INFO] Val end row:", self.val_end)


    # --------------------------------------------------
    # 1️⃣ Scan label trước (RAM nhỏ)
    # --------------------------------------------------
    def fit_label_encoder(self):
        labels = []
        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            labels.extend(chunk[self.label_col].astype(str).unique())

        self.label_encoder.fit(labels)
        print("[INFO] LabelEncoder fitted:", self.label_encoder.classes_)

    def load_validation_set(self):
        X_val_all = []
        y_val_all = []

        row_cursor = 0

        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            start = row_cursor
            end = row_cursor + len(chunk)
            row_cursor = end

            # Nếu chunk nằm hoàn toàn ngoài validation → skip
            if end <= self.train_end or start >= self.val_end:
                continue

            # Cắt đúng phần giao với [train_end, val_end)
            cut_start = max(0, self.train_end - start)
            cut_end = min(len(chunk), self.val_end - start)
            chunk = chunk.iloc[cut_start:cut_end]

            chunk = chunk.drop(columns=["ticker", "date"], errors="ignore")

            chunk["label_encoded"] = self.label_encoder.transform(
                chunk[self.label_col].astype(str)
            )

            X_val_all.append(chunk[self.feature_cols])
            y_val_all.append(chunk["label_encoded"])

        X_val = pd.concat(X_val_all, ignore_index=True)
        y_val = pd.concat(y_val_all, ignore_index=True)

        print("[INFO] Validation rows:", len(X_val))

        return lgb.Dataset(X_val, y_val, free_raw_data=True)

    # --------------------------------------------------
    # 2️⃣ Train theo chunk
    # --------------------------------------------------
    def train(self, params=None, num_boost_round=500):
        self.fit_label_encoder()
        if params is None:
            params = {
                "objective": "multiclass",
                "num_class": len(self.label_encoder.classes_),
                "metric": ["multi_error"],
                "learning_rate": 0.05,
                "num_leaves": 31,
                "max_depth": -1,
                "verbose": -1,
                "class_weight": "balanced",
            }

        self.compute_row_splits()

        booster = None
        row_cursor = 0

        val_dataset = None

        for i, chunk in enumerate(
            pd.read_csv(self.data_file, chunksize=self.chunk_size)
        ):
            logger.info(chunk.info())
            start = row_cursor
            end = row_cursor + len(chunk)
            row_cursor = end

            if start >= self.train_end:
                break

            if end > self.train_end:
                chunk = chunk.iloc[: self.train_end - start]

            chunk = chunk.drop(columns=["ticker", "date"], errors="ignore")

            chunk["label_encoded"] = self.label_encoder.transform(
                chunk[self.label_col].astype(str)
            )

            if self.feature_cols is None:
                self.feature_cols = [
                    c for c in chunk.columns
                    if c not in [self.label_col, "label_encoded"]
                ]

            if val_dataset is None:
                val_dataset = self.load_validation_set()

            X = chunk[self.feature_cols]
            y = chunk["label_encoded"]

            booster = lgb.train(
                params,
                lgb.Dataset(X, y),
                num_boost_round=num_boost_round,
                init_model=booster,
                valid_sets=[val_dataset],
                valid_names=["val"],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=50),
                    lgb.log_evaluation(50),
                ],
            )

            print(f"[INFO] Trained chunk {i + 1}")

        self.model = booster
        self.save_model()



    # --------------------------------------------------
    # 3️⃣ Evaluate (sample-based, RAM an toàn)
    # --------------------------------------------------
    def evaluate(self):
        self.load_model()

        y_true_all = []
        y_pred_all = []
        y_proba_all = []

        row_cursor = 0

        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            start = row_cursor
            end = row_cursor + len(chunk)
            row_cursor = end

            # Skip train + val
            if end <= self.val_end:
                continue

            if start < self.val_end:
                chunk = chunk.iloc[self.val_end - start :]

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

        # 👉 convert sang numpy
        y_true_all = np.array(y_true_all)
        y_pred_all = np.array(y_pred_all)
        y_proba_all = np.array(y_proba_all)

        results = {
            "log_loss": log_loss(y_true_all, y_proba_all),
            "precision_macro": precision_score(
                y_true_all, y_pred_all, average="macro", zero_division=0
            ),
            "recall_macro": recall_score(
                y_true_all, y_pred_all, average="macro", zero_division=0
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
            "data_file": self.data_file,
            "chunk_size": self.chunk_size,
        }

        return results



    # --------------------------------------------------
    # 4️⃣ Infer
    # --------------------------------------------------
    def infer(self, df: pd.DataFrame):
        self.load_model()

        df = df.drop(columns=["ticker", "date"], errors="ignore")
        X = df[self.feature_cols]

        preds = self.model.predict(X).argmax(axis=1)
        return self.label_encoder.inverse_transform(preds)

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
    
    def load_tuning_data(self, max_rows=300_000):
        X_train, y_train = [], []
        X_val, y_val = [], []

        train_ratio = 0.7
        val_ratio = 0.15

        self.fit_label_encoder()
        self.compute_row_splits()

        total_rows = self.count_rows()
        usable_rows = min(max_rows, total_rows)

        train_cap = int(usable_rows * train_ratio)
        val_cap = int(usable_rows * val_ratio)

        print(
            f"[INFO] Tuning rows: {usable_rows} "
            f"(train={train_cap}, val={val_cap})"
        )

        row_cursor = 0
        train_rows = 0
        val_rows = 0

        for chunk in pd.read_csv(self.data_file, chunksize=self.chunk_size):
            start = row_cursor
            end = row_cursor + len(chunk)
            row_cursor = end

            chunk = chunk.drop(columns=["ticker", "date"], errors="ignore")
            chunk["label_encoded"] = self.label_encoder.transform(
                chunk[self.label_col].astype(str)
            )

            if self.feature_cols is None:
                self.feature_cols = [
                    c for c in chunk.columns
                    if c not in [self.label_col, "label_encoded"]
                ]

            # -----------------------
            # TRAIN PART: [0, train_end)
            # -----------------------
            train_start = max(start, 0)
            train_end = min(end, self.train_end)

            if train_start < train_end and train_rows < train_cap:
                cut_start = train_start - start
                cut_end = min(train_end - start, cut_start + (train_cap - train_rows))

                X_train.append(chunk[self.feature_cols].iloc[cut_start:cut_end])
                y_train.append(chunk["label_encoded"].iloc[cut_start:cut_end])
                train_rows += (cut_end - cut_start)

            # -----------------------
            # VALIDATION PART: [train_end, val_end)
            # -----------------------
            val_start = max(start, self.train_end)
            val_end = min(end, self.val_end)

            if val_start < val_end and val_rows < val_cap:
                cut_start = val_start - start
                cut_end = min(val_end - start, cut_start + (val_cap - val_rows))

                X_val.append(chunk[self.feature_cols].iloc[cut_start:cut_end])
                y_val.append(chunk["label_encoded"].iloc[cut_start:cut_end])
                val_rows += (cut_end - cut_start)

            if train_rows >= train_cap and val_rows >= val_cap:
                break

        if train_rows == 0 or val_rows == 0:
            raise RuntimeError(
                f"Invalid tuning split: train={train_rows}, val={val_rows}"
            )

        return (
            lgb.Dataset(pd.concat(X_train), pd.concat(y_train)),
            lgb.Dataset(pd.concat(X_val), pd.concat(y_val)),
        )
    
    def tune_hyperparameters(
        self,
        n_trials=50,
        timeout=None,
        num_boost_round=500,
    ):
        train_set, val_set = self.load_tuning_data()

        def objective(trial):
            params = {
                "objective": "multiclass",
                "num_class": len(self.label_encoder.classes_),
                "metric": "multi_error",

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
                callbacks=[lgb.early_stopping(50), lgb.log_evaluation(period=50)],
            )

            return booster.best_score["valid_0"]["multi_error"]

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=n_trials, timeout=timeout)

        # 👉 Gán vào self để dùng cho train()
        self.tuned_params = {
            **study.best_params,
            "objective": "multiclass",
            "num_class": len(self.label_encoder.classes_),
            "metric": ["multi_error"],
            "class_weight": "balanced",
            "verbose": -1,
        }

        print("[INFO] Optuna best score:", study.best_value)
        print("[INFO] Optuna best params:", self.tuned_params)

        return self.tuned_params