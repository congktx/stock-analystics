from machine_learning.data_handler import Preprocessor

import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


# Main script to orchestrate the machine learning pipeline
def main():
    # Steps to be implemented:
    # 1. Load data using data_handler.connector
    preprocessor = Preprocessor()
    preprocessor.run()
    # 2. Preprocess data using data_handler.preprocessor & dump preprocessed data
    # 3. Train model using models
    # 4. Evaluate model performance
    pass

if __name__ == "__main__":
    main()