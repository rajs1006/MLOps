import pendulum
from airflow.decorators import dag, task
from feast import FeatureStore, RepoConfig
from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import RegistryConfig


@dag(
    schedule="@daily",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=["feast"],
)
def materialize_dag():
    @task()
    def materialize(data_interval_start=None, data_interval_end=None):
        repoConfig = RepoConfig(
            registry=RegistryConfig(path="s3://bayaga/ireland/registry.pb"),
            project="IDA",
            provider="local",
            online_store=RedisOnlineStoreConfig(connection_string="localhost:6379"),
            entity_key_serialization_version=2,
        )
        store = FeatureStore(config=repoConfig)

        # Add 1 hr overlap to account for late data
        store.materialize(data_interval_start.subtract(hours=1), data_interval_end)

    materialize()


materialization_dag = materialize_dag()
