import warnings

import mlflow
import numpy as np
import pandas as pd
from absl import logging as absl_logging
from imblearn.ensemble import BalancedBaggingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from src.common.utils import findOutliers, hourlySample
from statsmodels.tsa.seasonal import STL
from zenml.integrations.mlflow.flavors.mlflow_experiment_tracker_flavor import (
    MLFlowExperimentTrackerSettings,
)
from zenml.steps import Output, step

np.random.seed(0)
warnings.filterwarnings("ignore")
absl_logging.set_verbosity(-10000)


mlflow.set_tracking_uri("http://0.0.0.0:7009")


@step
def getDates() -> Output(stDate=str, endDate=str):

    stDate = "2020-01-01"
    endDate = "2022-06-30"

    return stDate, endDate


@step
def filteredData(
    dataConcatenated: pd.DataFrame, _stDate: str
) -> Output(targetSpreadWOOutlier=pd.DataFrame, features=pd.DataFrame):
    data = dataConcatenated.loc[_stDate:][:-1]

    targetSpread = data["price_diff"].dropna().asfreq("h")
    features = data.loc[
        targetSpread.index, ~data.columns.isin(["price_diff", "ie_actual_wnd"])
    ].asfreq("h")

    targetSpreadWOOutlier = findOutliers(targetSpread).to_frame()

    return targetSpreadWOOutlier, features


@step
def STLTransform(
    targetSpreadWOOutlier: pd.Series,
) -> Output(targetSpreadWOOutlierTrend=pd.Series):

    s = targetSpreadWOOutlier.copy().asfreq(freq="H")
    s.index = pd.to_datetime(s.index)

    stl_result = STL(s).fit()
    targetSpreadWOOutlierTrend = stl_result.trend
    stl_result.plot()

    return targetSpreadWOOutlierTrend


@step
def prepareSpreadTrend(
    targetSpreadWOOutlier: pd.Series,
    targetSpreadWOOutlierTrend: pd.Series,
) -> Output(
    targetSpreadSign=pd.DataFrame,
    targetSpreadTrendSign=pd.DataFrame,
    targetSpreadDaySign=pd.DataFrame,
):

    targetSpreadReal = (
        targetSpreadWOOutlier.to_frame()
        .copy()
        .rename(columns={"price_diff": "spread_real"})
    )
    targetSpreadSign = (
        np.sign(targetSpreadReal.mask(targetSpreadReal <= 0, -1))
        .rename(columns={"spread_real": "spread_sign"})
        .astype(int)
    )

    # trend
    targetSpreadTrendReal = targetSpreadWOOutlierTrend.copy().to_frame(
        "spread_trend_real"
    )
    targetSpreadTrendSign = (
        np.sign(targetSpreadTrendReal.mask(targetSpreadTrendReal <= 0, -1))
        .rename(columns={"spread_trend_real": "spread_trend_sign"})
        .astype(int)
    )

    targetSpreadDayReal = (
        targetSpreadWOOutlier.to_frame()
        .groupby([targetSpreadWOOutlier.index.date])
        .sum()
        .rename(columns={"price_diff": "spread_day_real"})
    )
    targetSpreadDayReal.index = pd.to_datetime(targetSpreadDayReal.index)
    targetSpreadDaySign = (
        np.sign(targetSpreadDayReal.mask(targetSpreadDayReal <= 0, -1))
        .rename(columns={"spread_day_real": "spread_day_sign"})
        .astype(int)
    )

    return (
        targetSpreadSign,
        targetSpreadTrendSign,
        targetSpreadDaySign,
    )


@step
def prepareFeatures(
    features: pd.DataFrame,
) -> Output(
    featuresFiltered=pd.DataFrame,
    dayFeatures=pd.DataFrame,
    featureName=list,
    dayFeatureName=list,
):
    featuresFiltered = features.dropna()
    featureName = list(featuresFiltered.columns)
    dayFeatures = featuresFiltered.resample("D", axis=0).sum()
    dayFeatureName = list(dayFeatures.columns)

    return featuresFiltered, dayFeatures, featureName, dayFeatureName


@step
def trainTestSplit(
    features: pd.DataFrame,
    dayFeatures: pd.DataFrame,
    featureName: list,
    dayFeatureName: list,
    targetSpreadSign: pd.DataFrame,
    targetSpreadTrendSign: pd.DataFrame,
    targetSpreadDaySign: pd.DataFrame,
    _stDate: str,
    _endDate: str,
) -> Output(
    X_train=pd.DataFrame,
    y_train=pd.DataFrame,
    y_trend_train=pd.DataFrame,
    X_day_train=pd.DataFrame,
    y_train_day_sign=pd.DataFrame,
):

    X_train = features.loc[_stDate:_endDate, featureName]

    y_train = targetSpreadSign.loc[_stDate:_endDate]
    y_trend_train = targetSpreadTrendSign.loc[_stDate:_endDate]

    X_day_train = dayFeatures.loc[_stDate:_endDate, dayFeatureName]
    y_train_day_sign = targetSpreadDaySign.loc[_stDate:_endDate]

    return (
        X_train,
        y_train,
        y_trend_train,
        X_day_train,
        y_train_day_sign,
    )


@step
def getTrendModelAndMetadata() -> Output(modl=tuple, day=bool):
    modl = (
        "trend_classifier",
        BalancedBaggingClassifier(
            estimator=LogisticRegression(
                random_state=42, solver="lbfgs", max_iter=1000
            ),
            random_state=0,
        ),
    )
    day = False

    return modl, day


@step
def getDayModelAndMetadata() -> Output(modl=tuple, day=bool):
    modl = (
        "day_classifier",
        BalancedBaggingClassifier(
            estimator=LogisticRegression(
                random_state=42, solver="lbfgs", max_iter=1000
            ),
            random_state=0,
        ),
    )
    day = True

    return modl, day


@step(
    enable_cache=False,
    experiment_tracker="ireland_green_tracker",
    settings={
        "experiment_tracker.mlflow": MLFlowExperimentTrackerSettings(
            experiment_name="ireland_pipeline",
            nested=True,
            tags={"mlflow.runName": "train_{{date}}_{{time}}"},
        )
    },
)
def trainModel(
    X: pd.DataFrame, Y: pd.DataFrame, modl: tuple, dayType: bool
) -> Output(finalTrain=pd.DataFrame, model=Pipeline):

    finalTrain = pd.DataFrame(index=Y.index)

    name, clf = modl

    p = [("transform", StandardScaler()), ("model", clf)]
    model = Pipeline(p)

    mlflow.set_tag("mlflow.runName", name)
    mlflow.sklearn.autolog(registered_model_name=f"{name}")

    model.fit(X.to_numpy(), Y.squeeze().to_numpy())

    score = model.score(X.to_numpy(), Y.squeeze().to_numpy())
    trainPred = model.predict(X.to_numpy())

    mlflow.log_metric("training score", score)

    if dayType:
        finalTrain["day_pred"] = hourlySample(
            pd.DataFrame(trainPred.astype(int), index=Y.index)
        )
    else:
        finalTrain["pred"] = trainPred.astype(int)

    return finalTrain, model


@step
def deployment_decision() -> bool:
    return True
