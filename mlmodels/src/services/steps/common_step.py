import warnings

import numpy as np
import pandas as pd
from absl import logging as absl_logging
from feast import FeatureStore
from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import RegistryConfig, RepoConfig
from zenml.steps import Output, step

np.random.seed(0)
warnings.filterwarnings("ignore")
absl_logging.set_verbosity(-10000)


@step(enable_cache=False)
def fetchData(_stDate: str, _endDate: str) -> Output(dataFrame=pd.DataFrame):

    repoConfig = RepoConfig(
        registry=RegistryConfig(path="s3://bayaga/registry.pb"),
        project="IDA",
        provider="local",
        online_store=RedisOnlineStoreConfig(connection_string="localhost:6379"),
        entity_key_serialization_version=2,
    )
    store = FeatureStore(config=repoConfig)

    dd = [
        {"epoch_time": int(pd.Timestamp(d.strftime("%Y-%m-%d %H:%M:%S")).timestamp())}
        for d in pd.date_range(start=_stDate, end=_endDate, freq="H")
    ]

    ## NOTE : This function is not returning columns in same sequence everytime so we need to be careful about this
    # I have raised a ticket with them and update as soon as there is an update
    batch_features = store.get_online_features(
        entity_rows=dd,
        features=store.get_feature_service("ireland_feature_service", allow_cache=True),
    ).to_df()

    df = batch_features.copy()

    df.set_index("epoch_time", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s")
    df.index.name = None

    dataFrame = df.astype(float)

    # drop the first NaN value as that might be because of shift
    dataFrame = dataFrame.iloc[dataFrame.notnull().all(axis=1).argmax() :]
    dataFrame = dataFrame.groupby(by=dataFrame.index, as_index=True).mean()

    dataFrame = dataFrame.reindex(
        pd.date_range(start=dataFrame.index.min(), end=dataFrame.index.max(), freq="H"),
        fill_value=np.nan,
    )
    dataFrame = dataFrame.interpolate(method="bfill", axis=0)

    # Add fetures explicitly to keep consistancy between the inference and training features
    dataFrame = dataFrame.loc[
        :,
        [
            "day",
            "weekday",
            "year_day",
            "week",
            "month",
            "quarter",
            "lag1",
            "lag2",
            "lag3",
            "lag4",
            "lag5",
            "lag6",
            "lag7",
            "lag30",
        ],
    ]

    return dataFrame
