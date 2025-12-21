from machine_learning.data_handler import Preprocessor

# Main script to orchestrate the machine learning pipeline
def main():
    # Steps to be implemented:
    # 1. Load data using data_handler.connector
    preprocessor = Preprocessor()
    # preprocessor.run()
    preprocessor.plot_preprocessed_data()
    # 2. Preprocess data using data_handler.preprocessor & dump preprocessed data
    # 3. Train model using models
    # 4. Evaluate model performance
    pass

if __name__ == "__main__":
    main()