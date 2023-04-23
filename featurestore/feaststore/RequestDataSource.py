""" Custom class to incorporate offline store for feast

This class is a custom implementation of modules need to integrate new data source 
specific to fetching data from DataLeaks <https://dataleaks.rke2-cnvx.com/index.html>.

This class supports integeration with FEAST <https://docs.feast.dev/>.

"""

__author__ = "Sourabh Raj"
__date__ = "2023/04/20"
__maintainer__ = "developer"


import json
import warnings
from datetime import datetime
from io import StringIO
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow
import requests
from feast import FileSource
from feast.data_source import DataSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.file import FileOfflineStore
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.infra.registry.registry import Registry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage
from feast.value_type import ValueType
from pydantic.typing import Literal
from urllib3 import disable_warnings, exceptions

from .Utils import _append_url, _changeRequestDates


class RequestOfflineStoreConfig(FeastConfigBaseModel):
    """Custom offline store config for local (file-based) store"""

    type: Literal[
        "feaststore.RequestDataSource.RequestOfflineStore"
    ] = "feaststore.RequestDataSource.RequestOfflineStore"

    # contains the domain of the url
    uri: str


@dask.delayed
def __readURIData(r: requests.Response, timestamp_field: str):
    """
    Configures delayed object of pandas dataframe

    Args:
        r (requests.Response): Response from the url
        timestamp_field (str): name of column which contains timestamp

    Returns:
        Dataframe : Delayed object of pandas dataframe
    """

    df = pd.read_csv(StringIO(r.text), sep=";", parse_dates=[timestamp_field])
    if df.empty:
        raise Exception("No data found !")
    df["epoch_time"] = (
        df[timestamp_field].apply(lambda x: x.timestamp()).astype("int64")
    )

    return df


def _readData(url: str, params: dict, timestamp_field: str):
    """
    Read dealyed data from pandas

    Args:
        url (str): url to fetch the data
        params (dict): Parameters to pass along the get request
        timestamp_field (str): name of column which contains timestamp

    Returns:
        _type_: Dask delayed data
    """

    with warnings.catch_warnings():
        disable_warnings(exceptions.InsecureRequestWarning)
        r = requests.get(url, params=params, verify=False)

    return dd.from_delayed(__readURIData(r, timestamp_field))


def pandas_type_to_feast_value_type(type_str: str) -> ValueType:
    """
    Maps the data types of pandas with feast

    Args:
        type_str (str): Pandas data type

    Returns:
        ValueType: feast data type
    """
    type_map: Dict[str, ValueType] = {
        "float64": ValueType.FLOAT,
        "int64": ValueType.INT64,
    }

    value = (
        type_map[type_str.lower()]
        if type_str.lower() in type_map
        else ValueType.UNKNOWN
    )

    if value == ValueType.UNKNOWN:
        print("unknown type:", type_str)

    return value


class CustomRequestApiOptions:
    """
    Class to contain the custom  parameters
    """

    def __init__(
        self,
        path: Optional[str],
        params: Optional[dict],
        columnTypeMappings: Optional[list[tuple]],
    ):
        self.path = path
        self.params = params or ""
        self.columnTypeMappings = columnTypeMappings or ""

    @classmethod
    def from_proto(cls, custom_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(custom_options_proto.configuration.decode("utf8"))
        postgres_options = cls(
            path=config["params"],
            name=config["params"],
            columnTypeMappings=config["columnTypeMappings"],
        )

        return postgres_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        custom_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "path": self.path,
                    "params": self.params,
                    "columnTypeMappings": self.columnTypeMappings,
                }
            ).encode()
        )
        return custom_options_proto


class CustomRequestDataSource(FileSource):
    """Custom data source class for local files"""

    def __init__(
        self,
        path: str,
        params: Optional[dict] = None,
        columnTypeMappings: Optional[list[tuple]] = None,
        name: Optional[str] = "",
        timestamp_field: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
    ):

        self._custom_options = CustomRequestApiOptions(
            path=path, params=params, columnTypeMappings=columnTypeMappings
        )

        super(CustomRequestDataSource, self).__init__(
            path=path,
            name=name,
            timestamp_field=timestamp_field,
            field_mapping=field_mapping,
        )

    @property
    def path(self):
        """
        Returns the file path of this feature data source.
        """
        return self._custom_options.path

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a `CustomFileDataSource` object from a DataSource proto, by
        parsing the CustomSourceOptions which is encoded as a binary json string.
        """

        assert data_source.HasField("custom_options")

        custom_source_options = json.loads(
            data_source.custom_options.configuration.decode("utf8")
        )
        return CustomRequestDataSource(
            path=custom_source_options["path"],
            params=custom_source_options["params"],
            columnTypeMappings=custom_source_options["columnTypeMappings"],
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            field_mapping=dict(data_source.field_mapping),
        )

    def to_proto(self) -> DataSourceProto:
        """
        Creates a DataSource proto representation of this object, by serializing some
        custom options into the custom_options field as a binary encoded json string.
        """

        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="src.datasource.DataSource.CustomFileDataSource",
            field_mapping=self.field_mapping,
            custom_options=self._custom_options.to_proto(),
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def get_table_query_string(self) -> str:
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return pandas_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        It return the mapping of columns and the data types. This is needed
        to configure the data source.

        This method is manadatory to override.

        Args:
            config (RepoConfig): Config of the repository

        Returns:
            Iterable[Tuple[str, str]]: Return iterable for tuple (columns : dtype)
        """

        # if columnTypeMapping is already passed the do not need to fecth anything
        if self._custom_options.columnTypeMappings:
            return self._custom_options.columnTypeMappings

        # Change the params to fetch less data
        requestParams = _changeRequestDates(
            self._custom_options.params, isPullingData=False
        )

        df_tmp = _readData(
            _append_url(config.offline_store.uri, self._custom_options.path),
            requestParams,
            self.timestamp_field,
        )

        return zip(df_tmp.columns, map(str, df_tmp.dtypes))


class CustomFileRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""
        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        df = df.reset_index(drop=True)
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self, timeout: Optional[int] = None):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return pyarrow.Table.from_pandas(df)

    def persist(self, storage: SavedDatasetStorage, allow_overwrite: bool = False):
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return False


class RequestOfflineStore(FileOfflineStore):
    def __init__(self):
        super().__init__()

    @staticmethod
    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:

        raise exceptions(
            "The fetaure to fetch from offline store is not available yet !"
        )

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:

        assert isinstance(config.offline_store, RequestOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        customDataSourceOptions = data_source._custom_options

        requestParams = _changeRequestDates(
            customDataSourceOptions.params,
            start=start_date,
            end=end_date,
            isPullingData=True,
        )

        def evaluate_offline_job():
            source_df = _readData(
                _append_url(config.offline_store.uri, customDataSourceOptions.path),
                requestParams,
                timestamp_field,
            )

            source_columns = set(source_df.columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    customDataSourceOptions.path, set(join_key_columns), source_columns
                )

            ts_columns = (
                [timestamp_field, created_timestamp_column]
                if created_timestamp_column
                else [timestamp_field]
            )

            # try-catch block is added to deal with this issue https://github.com/dask/dask/issues/8939.
            try:
                if created_timestamp_column:
                    source_df = source_df.sort_values(
                        by=created_timestamp_column,
                    )

                source_df = source_df.sort_values(by=timestamp_field)

            except ZeroDivisionError:
                # Use 1 partition to get around case where everything in timestamp column is the same so the partition algorithm doesn't
                # try to divide by zero.
                if created_timestamp_column:
                    source_df = source_df.sort_values(
                        by=created_timestamp_column, npartitions=1
                    )

                source_df = source_df.sort_values(by=timestamp_field, npartitions=1)

            source_df = source_df[
                (source_df[timestamp_field] >= start_date)
                & (source_df[timestamp_field] < end_date)
            ]

            source_df = source_df.persist()

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )

            if join_key_columns:
                source_df = source_df.drop_duplicates(
                    join_key_columns, keep="last", ignore_index=True
                )
            else:
                source_df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)

            source_df = source_df.persist()

            return source_df[list(columns_to_extract)]

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return CustomFileRetrievalJob(
            evaluation_function=evaluate_offline_job,
            full_feature_names=False,
        )
