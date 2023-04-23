from src.services.ireland.steps.train_steps import *
from src.services.ireland.steps.common_step import fetchData
from src.services.ireland.pipelines.training.train_pipeline import train_deploy_pipeline
from zenml.integrations.mlflow.steps import (
    MLFlowDeployerParameters,
    mlflow_model_deployer_step,
)


train_pipeline = train_deploy_pipeline(
    dates=getDates(),
    importer=fetchData(),
    filterData=filteredData(),
    transformer=STLTransform(),
    prepareTarget=prepareSpreadTrend(),
    prepareFeats=prepareFeatures(),
    splitter=trainTestSplit(),
    modelAndMetadataTrend=getTrendModelAndMetadata(),
    modelAndMetadataDay=getDayModelAndMetadata(),
    trendTrainer=trainModel().configure("trendTrainer"),
    dayTrainer=trainModel().configure("dayTrainer"),
    deploymentDecision=deployment_decision(),
    trendModelDeployer=mlflow_model_deployer_step(
        MLFlowDeployerParameters(timeout=500, run_name="trend_classifier")
    ).configure("trendModelDeployer"),
    dayModelDeployer=mlflow_model_deployer_step(
        MLFlowDeployerParameters(timeout=500, run_name="day_classifier")
    ).configure("dayModelDeployer"),
)
