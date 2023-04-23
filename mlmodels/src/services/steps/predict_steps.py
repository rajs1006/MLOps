import warnings

from datetime import datetime, timedelta
import mlflow
import numpy as np
import pandas as pd
from absl import logging as absl_logging
from src.common.utils import findOutliers, hourlySample
from zenml.client import Client
from zenml.services import BaseService
from zenml.steps import Output, step
from evidently.metric_preset import (
    DataQualityPreset,
    DataDriftPreset,
    ClassificationPreset,
)
from evidently.report import Report
from zenml.integrations.mlflow.flavors.mlflow_experiment_tracker_flavor import (
    MLFlowExperimentTrackerSettings,
)

np.random.seed(0)
warnings.filterwarnings("ignore")
absl_logging.set_verbosity(-10000)


mlflow.set_tracking_uri("http://0.0.0.0:7009")


@step
def getDates() -> Output(stDate=str, endDate=str):
    stDate = "2022-07-01"
    endDate = "2023-02-20"

    # stDate = (datetime.now() + timedelta(days=1))).strftime('%Y-%m-%d')
    # endDate = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d 23:00:00')

    return stDate, endDate


@step
def filteredData(
    dataConcatenated: pd.DataFrame, _stDate: str
) -> Output(targetSpreadWOOutlier=pd.DataFrame, features=pd.DataFrame):
    data = dataConcatenated.loc[_stDate:]

#     data_quality_report = TestSuite(tests=[
#         TestNumberOfColumnsWithMissingValues(),
#         TestNumberOfRowsWithMissingValues(),
#         TestNumberOfConstantColumns(),
#         TestNumberOfDuplicatedRows(),
#         TestNumberOfDuplicatedColumns(),
#         DataQualityTestPreset(),
# #         DataStabilityTestPreset()
#     ])
#     data_quality_report.run(reference_data=None, current_data=data)
#     data_quality_report.save_html("data_quality_report.html")
#     mlflow.log_artifact(
#         "data_quality_report.html",
#         "data",
#     )

    targetSpread = data["price_diff"].dropna().asfreq("h")
    features = data.loc[
        targetSpread.index, ~data.columns.isin(["price_diff", "ie_actual_wnd"])
    ].asfreq("h")

    targetSpreadWOOutlier = findOutliers(targetSpread).to_frame()

    return targetSpreadWOOutlier, features


@step
def prepareFeatures(
    features: pd.DataFrame, _stDate: str, _endDate: str
) -> Output(X_test=pd.DataFrame, X_day_test=pd.DataFrame):

    features = features.dropna()
    dayFeatures = features.resample("D", axis=0).sum()

    X_test = features.loc[_stDate:_endDate, :]
    X_day_test = dayFeatures.loc[_stDate:_endDate, :]

    return X_test, X_day_test


@step(enable_cache=False)
def prediction_service_loader() -> Output(
    trendService=BaseService, dayService=BaseService
):
    """Load the model service of our train_evaluate_deploy_pipeline."""
    client = Client()
    model_deployer = client.active_stack.model_deployer

    trendServices = model_deployer.find_model_server(
        pipeline_name="ireland_train_pipeline",
        pipeline_step_name="trendModelDeployer",
        running=True,
    )

    dayServices = model_deployer.find_model_server(
        pipeline_name="ireland_train_pipeline",
        pipeline_step_name="dayModelDeployer",
        running=True,
    )

    trendService = trendServices[0]
    dayService = dayServices[0]

    return trendService, dayService


@step
def predictor(
    trendService: BaseService,
    dayService: BaseService,
    data: pd.DataFrame,
    dayData: pd.DataFrame,
) -> Output(predictions=list, true=list):
    """Run a inference request against a prediction service"""

    trendService.start(timeout=100)  # should be a NOP if already started
    trendPrediction = trendService.predict(data.to_numpy())

    dayService.start(timeout=100)  # should be a NOP if already started
    dayPrediction = dayService.predict(dayData.to_numpy())

    predictions = [trendPrediction, dayPrediction]
    true = [data, dayData]

    return predictions, true


@step(
    experiment_tracker="ireland_green_tracker",
    settings={
        "experiment_tracker.mlflow": MLFlowExperimentTrackerSettings(
            experiment_name="ireland_pipeline",
            nested=True,
            tags={"mlflow.runName": "inferences"},
        )
    },
)
def publishResultAndReport(
    target: pd.Series, features: pd.DataFrame, preds: list, trues: list
) -> pd.DataFrame:

    targetSign = np.sign(target.mask(target <= 0, -1))

    index = trues[0].index
    predTrend = pd.DataFrame(preds[0], index=index, columns=["prediction"])
    finalReportDFTrend = pd.concat(
        [
            targetSign.to_frame("target").loc[index, :],
            predTrend,
            features.loc[index, :],
        ],
        axis=1,
    )
    reportTrend = Report(
        metrics=[DataQualityPreset(), DataDriftPreset(), ClassificationPreset()]
    )
    reportTrend.run(
        reference_data=finalReportDFTrend.loc[
            finalReportDFTrend.index < "2022-12-27", :
        ],
        current_data=finalReportDFTrend.loc[
            finalReportDFTrend.index >= "2022-12-27", :
        ],
    )

    mlflow.log_dict(predTrend.to_dict("records"), "results/trend/predictions.json")
    mlflow.log_dict(
        finalReportDFTrend.loc[finalReportDFTrend.index < "2022-12-27", :].to_dict(
            "records"
        ),
        "results/trend/reference_data.json",
    )
    mlflow.log_dict(
        finalReportDFTrend.loc[finalReportDFTrend.index >= "2022-12-27", :].to_dict(
            "records"
        ),
        "results/trend/current_data.json",
    )
    mlflow.log_dict(reportTrend.as_dict(), "reports/trend/prediction_report.yml")
    reportTrend.save_html("trend_prediction_report.html")
    mlflow.log_artifact(
        "trend_prediction_report.html",
        "reports/trend",
    )

    predDay = hourlySample(
        pd.DataFrame(preds[1], index=trues[1].index, columns=["prediction"])
    )
    finalReportDFDay = pd.concat(
        [
            targetSign.to_frame("target").loc[index, :],
            predDay,
            features.loc[index, :],
        ],
        axis=1,
    )
    reportDay = Report(
        metrics=[DataQualityPreset(), DataDriftPreset(), ClassificationPreset()]
    )
    reportDay.run(
        reference_data=finalReportDFDay.loc[finalReportDFDay.index < "2022-12-27", :],
        current_data=finalReportDFDay.loc[finalReportDFDay.index >= "2022-12-27", :],
    )

    mlflow.log_dict(predDay.to_frame().to_dict("records"), "results/day/predictions.json")
    mlflow.log_dict(
        finalReportDFDay.loc[finalReportDFDay.index < "2022-12-27", :].to_dict(
            "records"
        ),
        "results/day/reference_data.json",
    )
    mlflow.log_dict(
        finalReportDFDay.loc[finalReportDFDay.index >= "2022-12-27", :].to_dict(
            "records"
        ),
        "results/day/current_data.json",
    )
    mlflow.log_dict(reportDay.as_dict(), "reports/day/prediction_report.yml")
    reportTrend.save_html("day_prediction_report.html")
    mlflow.log_artifact(
        "day_prediction_report.html",
        "reports/day",
    )

    df = pd.concat(
        [
            predTrend.rename(columns={"prediction": "trend_prediction"}),
            predDay.to_frame().rename(columns={"prediction": "day_prediction"}),
        ],
        axis=1,
    )

    mlflow.log_dict(df.to_dict("records"), "predictions.json")

    return df
