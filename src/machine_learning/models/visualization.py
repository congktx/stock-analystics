from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd


class Visualizer:
    def __init__(self, results: dict):
        self.r = results
        
    def plot_label_distribution_full_data(self):
        data_file = self.r["data_file"]
        chunk_size = self.r["chunk_size"]
        labels = self.r["labels"]

        label_counter = Counter()

        for chunk in pd.read_csv(data_file, chunksize=chunk_size):
            label_counter.update(chunk["label"].values)

        counts = [label_counter.get(label, 0) for label in labels]

        plt.figure(figsize=(6, 4))
        sns.barplot(x=labels, y=counts)
        plt.xticks(rotation=30)
        plt.title("Label Distribution (ALL DATA)")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.show()
        
    def plot_confusion_matrix(self):
        cm = self.r["confusion_matrix"]
        labels = self.r["labels"]

        plt.figure(figsize=(7, 6))
        sns.heatmap(
            cm,
            annot=True,
            fmt="d",
            cmap="Blues",
            xticklabels=labels,
            yticklabels=labels,
        )
        plt.xlabel("Predicted")
        plt.ylabel("True")
        plt.title("Confusion Matrix")
        plt.tight_layout()
        plt.show()

    def plot_label_distribution(self):
        y_true = self.r["y_true"]
        labels = self.r["labels"]

        plt.figure(figsize=(6, 4))
        sns.countplot(x=y_true, order=range(len(labels)))
        plt.xticks(range(len(labels)), labels, rotation=30)

        plt.title("Label Distribution (TEST)")
        plt.tight_layout()
        plt.show()

    def plot_prf(self):
        report = self.r["classification_report"]
        labels = self.r["labels"]

        precision = [report.get(l, {}).get("precision", 0) for l in labels]
        recall = [report.get(l, {}).get("recall", 0) for l in labels]
        f1 = [report.get(l, {}).get("f1-score", 0) for l in labels]

        x = np.arange(len(labels))

        plt.figure(figsize=(8, 4))
        plt.bar(x - 0.2, precision, width=0.2, label="Precision")
        plt.bar(x, recall, width=0.2, label="Recall")
        plt.bar(x + 0.2, f1, width=0.2, label="F1")
        plt.xticks(x, labels, rotation=30)
        plt.legend()
        plt.title("Precision / Recall / F1 per class")
        plt.tight_layout()
        plt.show()
        
    def plot_prf_mean(self):
        report = self.r["classification_report"]
        labels = self.r["labels"]

        precision = np.mean([report.get(l, {}).get("precision", 0) for l in labels])
        recall = np.mean([report.get(l, {}).get("recall", 0) for l in labels])
        f1 = np.mean([report.get(l, {}).get("f1-score", 0) for l in labels])

        plt.figure(figsize=(5, 4))
        plt.bar(["Precision", "Recall", "F1"], [precision, recall, f1])
        plt.ylim(0, 1)
        plt.title("Mean Precision / Recall / F1 (per-class average)")
        plt.tight_layout()
        plt.show()


    def plot_confidence(self):
        y_proba = self.r["y_proba"]

        if y_proba.ndim != 2:
            raise ValueError("y_proba must be 2D (n_samples, n_classes)")
        max_proba = y_proba.max(axis=1)

        plt.figure(figsize=(6, 4))
        plt.hist(max_proba, bins=30)
        plt.xlabel("Max predicted probability")
        plt.ylabel("Count")
        plt.title("Prediction confidence distribution")
        plt.tight_layout()
        plt.show()
