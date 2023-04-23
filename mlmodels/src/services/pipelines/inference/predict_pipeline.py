from zenml.pipelines import pipeline


@pipeline(
    enable_cache=False,
)
def inference_pipeline(
    dates, importer, filter, prepare, prediction_service_loader, predictor, publisher
):
    """Basic inference pipeline."""

    (_stDate, _endDate) = dates()
    data = importer(_stDate, _endDate)

    target, features = filter(data, _stDate)

    X_test, X_day_test = prepare(features, _stDate, _endDate)

    trendService, dayService = prediction_service_loader()
    predictions, trues = predictor(trendService, dayService, X_test, X_day_test)

    _ = publisher(target, features, predictions, trues)
