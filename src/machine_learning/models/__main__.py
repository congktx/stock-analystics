from machine_learning.models.lightgbm_model import LightGBMModel
from machine_learning.models.visualization import Visualizer
import pandas as pd

def main():
    model = LightGBMModel(
        label_col="label",
        model_path="lightgbm_model.pkl",
        chunk_size=200_000
    )
    model.stratified_split_to_files(train_ratio=0.7, val_ratio=0.15)
    model.tune_hyperparameters(n_trials=10)
    # print("\n=== TRAIN MODEL ===")
    model.train(params=model.tuned_params)

    # model.train()
    print("\n=== EVALUATE MODEL ===")
    # results = model.evaluate(test_data_file="data_changed_label.csv", val_end=701106, export_inference_result=False)
    # model.load_model()
    results = model.evaluate()
    viz = Visualizer(results)
    viz.plot_label_distribution()
    viz.plot_confusion_matrix()
    viz.plot_prf()
    viz.plot_prf_mean()
    viz.plot_confidence()
    viz.plot_label_distribution_full_data()

    # model.load_model()
    # print("\n=== PREDICT SAMPLE DATA ===")
    # sample_data_df = pd.read_csv("./machine_learning/data_handler/preprocessed_data/data.csv", nrows=10)
    # predictions = model.infer(sample_data_df)
    # print("Predictions:", predictions)

if __name__ == "__main__":
    main()