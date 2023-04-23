# ZENML

## [ZENML](https://zenml.io/home) is a tool to create pipeline for *training/inference*. This tool follows bridge pattern and provide multiple interfaces which is selectable.

### **LOCAL RUN**

* run *bash .github/workflows/local/mlflow.sh*
    - To run ZENML we have to run [MLFLOW](https://mlflow.org/) first as ZENML uses MLFLOW are experiment tracker.
* run *bash .github/workflows/local/zenml.sh*
    - This script will bring up all the underlying tech stack to run the ZENML on your local
