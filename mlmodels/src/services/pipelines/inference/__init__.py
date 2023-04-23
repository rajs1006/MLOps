from src.services.ireland.steps.predict_steps import *
from src.services.ireland.steps.common_step import fetchData
from src.services.ireland.pipelines.inference.predict_pipeline import (
    inference_pipeline,
)


# Initialize an inference pipeline run
predict_pipeline = inference_pipeline(
    dates=getDates(),
    filter=filteredData(),
    importer=fetchData(),
    prepare=prepareFeatures(),
    prediction_service_loader=prediction_service_loader(),
    predictor=predictor(),
    publisher=publishResultAndReport(),
)
