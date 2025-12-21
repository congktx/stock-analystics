from machine_learning.models.lightgbm_model import LightGBMModel
from machine_learning.models.visualization import Visualizer
import pandas as pd

def main():
    model = LightGBMModel(
        label_col="label",
        model_path="lightgbm_model.pkl",
        chunk_size=200_000
    )
    # model.tune_hyperparameters(n_trials=50)
    # print("\n=== TRAIN MODEL ===")
    # model.train(params=model.tuned_params)

    print("\n=== EVALUATE MODEL ===")
    results = model.evaluate(test_data_file="test_data.csv", val_end=0)
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