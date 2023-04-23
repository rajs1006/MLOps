# Feast Custom Offline Store


### Overview

This repository demonstrates how developers can create their own custom `offline store`s for Feast.
Custom offline stores allow users to use any underlying data store as their offline feature store. Features can be retrieved from the offline store for model training, and can be materialized into the online feature store for use during model inference.

### Why create a custom offline store?

Feast uses an offline store as the source of truth for features. These features can be retrieved from the offline store for model training. Typically, scalable data warehouses are used for this purpose.

Feast also materializes features from offline stores to an online store for low-latency lookup at model inference time.

Feast comes with some offline stores built in, e.g, Parquet file, Redshift and Bigquery. However, users can develop their own offline stores by creating a class that implements the contract in the [OfflineStore class](https://github.com/feast-dev/feast/blob/5e61a6f17c3b52f20b449214a4bb56bafa5cfcbc/sdk/python/feast/infra/offline_stores/offline_store.py#L41).

### What is included in this repository?

* [feaststore/](feaststore): An example of a custom offline store, `RequestDataSource`, which implements OfflineStore. This example offline store overrides the File offline store that is provided by Feast.


### TO USE custom data source `RequestDataSource`

Build wheel to create pip package : `python -m build --wheel`
    * This will create a wheel file `cnvxfeast-0.0.1-py3-none-any.whl` in `dist` folder

You can use this wheel file to uploda your package to a package manager of use if using pip command to dicstribute it locally.
