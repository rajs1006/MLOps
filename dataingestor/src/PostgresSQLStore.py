from datetime import datetime, timedelta

import yaml
from feast import Entity, FeatureService, FeatureView
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

PARAMS_ = {
    "timezone": "'Europe/Dublin'",
    "startDate": "2019-03-16",
    "startHour": 23,
    "endDate": datetime.now().strftime("%Y-%m-%d"),
    "hourLag": 1,
    "splitProductIndex": 14,
    "id_": 22,
    "name_": "wind",
}


def loadYaml(resourceFile: str):
    """
    This method loads the yaml file.

    Args:
        resourceFile : path of resource file;

    Returns:
        Dict: simplenamespace dict of resource variables;
    """

    with open(resourceFile) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


queries = loadYaml("../../resources/queries.yaml")


# --------------------------------------------------
spread = Entity(name="spread", join_keys=["epoch_time"])

target_spread_source = PostgreSQLSource(
    name="spread",
    query=queries["SPREAD"].format(**PARAMS_),
    timestamp_field="delivery_start",
)

target_spread_fv = FeatureView(
    name="target_spread",
    entities=[spread],
    ttl=timedelta(days=1460),
    source=target_spread_source,
)

# --------------------------------------------------
actual_fund = Entity(name="actual_fund", join_keys=["epoch_time"])

feature_actual_fund_source = PostgreSQLSource(
    name="actual_fund",
    query=queries["ACTUAL_FUNDS_QUERY"].format(**PARAMS_),
    timestamp_field="delivery_start",
)

feature_actual_fund_fv = FeatureView(
    name="feature_actual_fund",
    entities=[actual_fund],
    ttl=timedelta(days=1460),
    online=True,
    source=feature_actual_fund_source,
)

# --------------------------------------------------
lags = Entity(name="lags", join_keys=["epoch_time"])

feature_lag_source = PostgreSQLSource(
    name="lag",
    query=queries["LAGS"].format(**PARAMS_),
    timestamp_field="delivery_start",
)

feature_lag_fund_fv = FeatureView(
    name="feature_lag_fund",
    entities=[lags],
    ttl=timedelta(days=1460),
    online=True,
    source=feature_lag_source,
)


# --------------------------------------------------
featureService = FeatureService(
    name="ireland_data_service",
    features=[
        target_spread_fv,
        feature_actual_fund_fv,
        feature_lag_fund_fv,
    ],
)
