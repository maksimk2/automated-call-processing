# Managing the bias-variance trade-off at scale

Welcome to the companion repository for the blog **"Managing the bias-variance trade-off at scale"**. This repository contains all the necessary code to replicate the experiments and analyses discussed in the blog, including the generation of synthetic data and the training of machine learning models.

All the experiments have been executed on Databricks using DBR 15.3 ML.

---

## Contents

### 1. **Synthetic Dataset Generation**
The logic to create the synthetic dataset used in the experiments is provided in the notebook:
- **`synthetic_data_generator`**: This notebook outlines the process for generating the synthetic dataset, which serves as the foundation for the experiments. It includes the creation of group-specific data distributions and target variables.
- **`synthetic_polynomial_groups_binary.csv`**: csv created running the logic in the **`synthetic_data_generator`** notebook.

### 2. **Machine Learning Experiments**
The experiments, results, and analyses are detailed in the notebook:
- **`ml_training_experiments`**: This notebook contains the end-to-end pipeline for training machine learning models. It includes:
  - Hyperparameter optimisation for a global model.
  - Group-based training with shared and optimised hyperparameters.
  - Evaluation metrics, visualisations, and comparative analyses.

---

## Getting Started

To get started, clone the repository and install the necessary dependencies:
```bash
git clone <repository_url>


