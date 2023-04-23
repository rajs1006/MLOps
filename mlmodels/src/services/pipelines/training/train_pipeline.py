from zenml.pipelines import pipeline


@pipeline(enable_cache=False, name="ireland_train_pipeline")
def train_deploy_pipeline(
    dates,
    importer,
    filterData,
    transformer,
    prepareTarget,
    prepareFeats,
    splitter,
    modelAndMetadataTrend,
    modelAndMetadataDay,
    trendTrainer,
    dayTrainer,
    deploymentDecision,
    trendModelDeployer,
    dayModelDeployer,
):
    """Train and deploy a model with MLflow."""
    _stDate, _endDate = dates()
    data = importer(_stDate, _endDate)
    target, features = filterData(data, _stDate)
    trendTarget = transformer(target)

    (
        targetSpreadSign,
        targetSpreadTrendSign,
        targetSpreadDaySign,
    ) = prepareTarget(target, trendTarget)

    (features, dayFeatures, featureName, dayFeatureName) = prepareFeats(features)

    (X_train, _, y_trend_train, X_day_train, y_train_day_sign,) = splitter(
        features,
        dayFeatures,
        featureName,
        dayFeatureName,
        targetSpreadSign,
        targetSpreadTrendSign,
        targetSpreadDaySign,
        _stDate,
        _endDate,
    )

    modl, day = modelAndMetadataTrend()
    trendTrainPred, trendModel = trendTrainer(
        X=X_train, Y=y_trend_train, modl=modl, dayType=day
    )

    modl, day = modelAndMetadataDay()
    dayTrainPred, dayModel = dayTrainer(
        X=X_day_train, Y=y_train_day_sign, modl=modl, dayType=day
    )

    shouldDeploy = deploymentDecision()
    trendModelDeployer(shouldDeploy, trendModel)
    dayModelDeployer(shouldDeploy, dayModel)
