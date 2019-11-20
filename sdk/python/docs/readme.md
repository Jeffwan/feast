# Feast Python Library

Feast (Feature Store) is a tool to manage storage and access of machine learning features. It aims to:
* Support ingesting feature data via batch or streaming
* Provide scalable storage of feature data for serving and training
* Provide an API for low latency access of features
* Enable discovery and documentation of features
* Provide an overview of the general health of features in the system

This Python library allows you to **register** features and entities in Feast, **ingest** feature values, and retrieve the values for model **training and serving**.

## Installation

Install `feast` library using `pip`:

```sh
 pip install feast
```

> Make sure you have a running Feast instance. If not, follow this [installation guide](https://github.com/gojek/feast/blob/72cc8bd2cd0040f7bc44df255f95bad00cacd720/docs/install.md)

## Configuration

All interaction with feast cluster happens via an instance of `feast.sdk.client.Client`. The client should be pointed to correct core/serving URL of the feast cluster
```python
from feast.client import Client

# Assuming you are running Feast locally so Feast hostname is localhost
FEAST_CORE_URL="localhost:50051"
FEAST_SERVING_URL="localhost:50052"

feast_client = Client(
    core_url=FEAST_CORE_URL, 
    serving_url=FEAST_SERVING_URL)
```

## FeatureSet Creation

```python
from feast.feature_set import FeatureSet
from feast.entity import Entity
from feast.feature_set import Feature

fs1 = FeatureSet("my-feature-set-1")
fs1.add(Feature(name="fs1-my-feature-1", dtype=ValueType.INT64))
fs1.add(Feature(name="fs1-my-feature-2", dtype=ValueType.STRING))
fs1.add(Entity(name="fs1-my-entity-1", dtype=ValueType.INT64))

# Register Feature Set with Core
client.apply(fs1)

feature_sets = client.list_feature_sets()
```

## Online Features Retrieval

```python
# TODO
``` 
