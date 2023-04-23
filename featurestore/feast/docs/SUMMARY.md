# Table of contents

* [Introduction](README.md)
* [Community & getting help](community.md)
* [Roadmap](roadmap.md)
* [Changelog](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md)

## Getting started

* [Quickstart](getting-started/quickstart.md)
* [Concepts](getting-started/concepts/README.md)
  * [Overview](getting-started/concepts/overview.md)
  * [Data ingestion](getting-started/concepts/data-ingestion.md)
  * [Entity](getting-started/concepts/entity.md)
  * [Feature view](getting-started/concepts/feature-view.md)
  * [Feature retrieval](getting-started/concepts/feature-retrieval.md)
  * [Point-in-time joins](getting-started/concepts/point-in-time-joins.md)
  * [Registry](getting-started/concepts/registry.md)
  * [\[Alpha\] Saved dataset](getting-started/concepts/dataset.md)
* [Architecture](getting-started/architecture-and-components/README.md)
  * [Overview](getting-started/architecture-and-components/overview.md)
  * [Registry](getting-started/architecture-and-components/registry.md)
  * [Offline store](getting-started/architecture-and-components/offline-store.md)
  * [Online store](getting-started/architecture-and-components/online-store.md)
  * [Batch Materialization Engine](getting-started/architecture-and-components/batch-materialization-engine.md)
  * [Provider](getting-started/architecture-and-components/provider.md)
* [Third party integrations](getting-started/third-party-integrations.md)
* [FAQ](getting-started/faq.md)

## Tutorials

* [Sample use-case tutorials](tutorials/tutorials-overview/README.md)
  * [Driver ranking](tutorials/tutorials-overview/driver-ranking-with-feast.md)
  * [Fraud detection on GCP](tutorials/tutorials-overview/fraud-detection.md)
  * [Real-time credit scoring on AWS](tutorials/tutorials-overview/real-time-credit-scoring-on-aws.md)
  * [Driver stats on Snowflake](tutorials/tutorials-overview/driver-stats-on-snowflake.md)
* [Validating historical features with Great Expectations](tutorials/validating-historical-features.md)
* [Using Scalable Registry](tutorials/using-scalable-registry.md)
* [Building streaming features](tutorials/building-streaming-features.md)

## How-to Guides

* [Running Feast with Snowflake/GCP/AWS](how-to-guides/feast-snowflake-gcp-aws/README.md)
  * [Install Feast](how-to-guides/feast-snowflake-gcp-aws/install-feast.md)
  * [Create a feature repository](how-to-guides/feast-snowflake-gcp-aws/create-a-feature-repository.md)
  * [Deploy a feature store](how-to-guides/feast-snowflake-gcp-aws/deploy-a-feature-store.md)
  * [Build a training dataset](how-to-guides/feast-snowflake-gcp-aws/build-a-training-dataset.md)
  * [Load data into the online store](how-to-guides/feast-snowflake-gcp-aws/load-data-into-the-online-store.md)
  * [Read features from the online store](how-to-guides/feast-snowflake-gcp-aws/read-features-from-the-online-store.md)
  * [Scaling Feast](how-to-guides/scaling-feast.md)
  * [Structuring Feature Repos](how-to-guides/structuring-repos.md)
* [Running Feast in production (e.g. on Kubernetes)](how-to-guides/running-feast-in-production.md)
* [Upgrading for Feast 0.20+](how-to-guides/automated-feast-upgrade.md)
* [Customizing Feast](how-to-guides/customizing-feast/README.md)
  * [Adding a custom batch materialization engine](how-to-guides/customizing-feast/creating-a-custom-materialization-engine.md)
  * [Adding a new offline store](how-to-guides/customizing-feast/adding-a-new-offline-store.md)
  * [Adding a new online store](how-to-guides/customizing-feast/adding-support-for-a-new-online-store.md)
  * [Adding a custom provider](how-to-guides/customizing-feast/creating-a-custom-provider.md)
* [Adding or reusing tests](how-to-guides/adding-or-reusing-tests.md)

## Reference

* [Codebase Structure](reference/codebase-structure.md)
* [Type System](reference/type-system.md)
* [Data sources](reference/data-sources/README.md)
  * [Overview](reference/data-sources/overview.md)
  * [File](reference/data-sources/file.md)
  * [Snowflake](reference/data-sources/snowflake.md)
  * [BigQuery](reference/data-sources/bigquery.md)
  * [Redshift](reference/data-sources/redshift.md)
  * [Push](reference/data-sources/push.md)
  * [Kafka](reference/data-sources/kafka.md)
  * [Kinesis](reference/data-sources/kinesis.md)
  * [Spark (contrib)](reference/data-sources/spark.md)
  * [PostgreSQL (contrib)](reference/data-sources/postgres.md)
  * [Trino (contrib)](reference/data-sources/trino.md)
  * [Azure Synapse + Azure SQL (contrib)](reference/data-sources/mssql.md)
* [Offline stores](reference/offline-stores/README.md)
  * [Overview](reference/offline-stores/overview.md)
  * [File](reference/offline-stores/file.md)
  * [Snowflake](reference/offline-stores/snowflake.md)
  * [BigQuery](reference/offline-stores/bigquery.md)
  * [Redshift](reference/offline-stores/redshift.md)
  * [Spark (contrib)](reference/offline-stores/spark.md)
  * [PostgreSQL (contrib)](reference/offline-stores/postgres.md)
  * [Trino (contrib)](reference/offline-stores/trino.md)
  * [Azure Synapse + Azure SQL (contrib)](reference/offline-stores/mssql.md)
* [Online stores](reference/online-stores/README.md)
  * [Overview](reference/online-stores/overview.md)
  * [SQLite](reference/online-stores/sqlite.md)
  * [Snowflake](reference/online-stores/snowflake.md)
  * [Redis](reference/online-stores/redis.md)
  * [Datastore](reference/online-stores/datastore.md)
  * [DynamoDB](reference/online-stores/dynamodb.md)
  * [Bigtable](reference/online-stores/bigtable.md)
  * [PostgreSQL (contrib)](reference/online-stores/postgres.md)
  * [Cassandra + Astra DB (contrib)](reference/online-stores/cassandra.md) 
  * [MySQL (contrib)](reference/online-stores/mysql.md)
  * [Rockset (contrib)](reference/online-stores/rockset.md)
  * [Hazelcast (contrib)](reference/online-stores/hazelcast.md)
* [Providers](reference/providers/README.md)
  * [Local](reference/providers/local.md)
  * [Google Cloud Platform](reference/providers/google-cloud-platform.md)
  * [Amazon Web Services](reference/providers/amazon-web-services.md)
  * [Azure](reference/providers/azure.md)
* [Batch Materialization Engines](reference/batch-materialization/README.md)
  * [Bytewax](reference/batch-materialization/bytewax.md)
  * [Snowflake](reference/batch-materialization/snowflake.md)
  * [AWS Lambda (alpha)](reference/batch-materialization/lambda.md)
  * [Spark (contrib)](reference/batch-materialization/spark.md)
* [Feature repository](reference/feature-repository/README.md)
  * [feature\_store.yaml](reference/feature-repository/feature-store-yaml.md)
  * [.feastignore](reference/feature-repository/feast-ignore.md)
* [Feature servers](reference/feature-servers/README.md)
  * [Python feature server](reference/feature-servers/python-feature-server.md)
  * [\[Alpha\] Go feature server](reference/feature-servers/go-feature-server.md)
  * [\[Alpha\] AWS Lambda feature server](reference/feature-servers/alpha-aws-lambda-feature-server.md)
* [\[Beta\] Web UI](reference/alpha-web-ui.md)
* [\[Alpha\] On demand feature view](reference/alpha-on-demand-feature-view.md)
* [\[Alpha\] Data quality monitoring](reference/dqm.md)
* [Feast CLI reference](reference/feast-cli-commands.md)
* [Python API reference](http://rtd.feast.dev)
* [Usage](reference/usage.md)

## Project

* [Contribution process](project/contributing.md)
* [Development guide](project/development-guide.md)
* [Backwards Compatibility Policy](project/compatibility.md)
  * [Maintainer Docs](project/maintainers.md)
* [Versioning policy](project/versioning-policy.md)
* [Release process](project/release-process.md)
* [Feast 0.9 vs Feast 0.10+](project/feast-0.9-vs-feast-0.10+.md)