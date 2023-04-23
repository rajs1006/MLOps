printf "Enter the country name : "
read -r COUNTRY

printf "Enter the color : "
read -r COLOR

echo "Start the MLFLow first by running 'bach mlflow.sh'"
echo "'"
printf "Enter the MLFLOW tracking url, default(0.0.0.0:7009) : "
read -r MLFLOWURL
MLFLOWURL=${MLFLOWURL:='0.0.0.0:7009'}

{ 
    zenml clean
} || {
    zenml clean
}

echo ""
zenml data-validator register ${COUNTRY}_data_validator --flavor=evidently

echo ""
# Register the MLflow experiment tracker
zenml experiment-tracker register ${COUNTRY}_${COLOR}_tracker --flavor=mlflow --tracking_uri=http://${MLFLOWURL} --tracking_token=7c85840a-d2d0-11ed-b5cc-3ff910b282b4

echo ""
zenml orchestrator register ${COUNTRY}_${COLOR}_orchestrator --flavor=local
# --flavor=local
# --flavor=airflow --local=True

## Model registry is not being used as models are rgeistrede autmatically
# zenml model-registry register ${COUNTRY}_model_registry --flavor=mlflow

echo ""
zenml artifact-store register ${COUNTRY}_${COLOR}_artifact_store --flavor=s3 --path=s3://bayaga/${COUNTRY}

echo ""
# Register the MLflow model deployer
zenml model-deployer register ${COUNTRY}_${COLOR}_deployer --flavor=mlflow


echo ""
zenml image-builder register ${COUNTRY}_${COLOR}_builder --flavor=local

# zenml feature-store register ida_feast_store --flavor=feast --feast_repo="s3://bayaga/registry.pb"

echo ""
# Create a new stack that includes an MLflow experiment
zenml stack register ${COUNTRY}_${COLOR}_deployment_stack -e ${COUNTRY}_${COLOR}_tracker \
    -a ${COUNTRY}_${COLOR}_artifact_store -o ${COUNTRY}_${COLOR}_orchestrator -d ${COUNTRY}_${COLOR}_dev \
    -dv ${COUNTRY}_data_validator -i ${COUNTRY}_${COLOR}_builder
#     -r ${COUNTRY}_model_registry
#-f ida_feast_store

echo ""
zenml stack up

echo ""
zenml up

echo ""
zenml stack set ${COUNTRY}_${COLOR}_deployment_stack