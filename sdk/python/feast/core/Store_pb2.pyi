# @generated by generate_proto_mypy_stubs.py.  Do not edit!
import sys
from google.protobuf.descriptor import (
    Descriptor as google___protobuf___descriptor___Descriptor,
    EnumDescriptor as google___protobuf___descriptor___EnumDescriptor,
)

from google.protobuf.internal.containers import (
    RepeatedCompositeFieldContainer as google___protobuf___internal___containers___RepeatedCompositeFieldContainer,
)

from google.protobuf.message import (
    Message as google___protobuf___message___Message,
)

from typing import (
    Iterable as typing___Iterable,
    List as typing___List,
    Optional as typing___Optional,
    Text as typing___Text,
    Tuple as typing___Tuple,
    cast as typing___cast,
)

from typing_extensions import (
    Literal as typing_extensions___Literal,
)


class Store(google___protobuf___message___Message):
    DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
    class StoreType(int):
        DESCRIPTOR: google___protobuf___descriptor___EnumDescriptor = ...
        @classmethod
        def Name(cls, number: int) -> str: ...
        @classmethod
        def Value(cls, name: str) -> Store.StoreType: ...
        @classmethod
        def keys(cls) -> typing___List[str]: ...
        @classmethod
        def values(cls) -> typing___List[Store.StoreType]: ...
        @classmethod
        def items(cls) -> typing___List[typing___Tuple[str, Store.StoreType]]: ...
        INVALID = typing___cast(Store.StoreType, 0)
        REDIS = typing___cast(Store.StoreType, 1)
        BIGQUERY = typing___cast(Store.StoreType, 2)
        CASSANDRA = typing___cast(Store.StoreType, 3)
    INVALID = typing___cast(Store.StoreType, 0)
    REDIS = typing___cast(Store.StoreType, 1)
    BIGQUERY = typing___cast(Store.StoreType, 2)
    CASSANDRA = typing___cast(Store.StoreType, 3)

    class RedisConfig(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        host = ... # type: typing___Text
        port = ... # type: int

        def __init__(self,
            *,
            host : typing___Optional[typing___Text] = None,
            port : typing___Optional[int] = None,
            ) -> None: ...
        @classmethod
        def FromString(cls, s: bytes) -> Store.RedisConfig: ...
        def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        if sys.version_info >= (3,):
            def ClearField(self, field_name: typing_extensions___Literal[u"host",u"port"]) -> None: ...
        else:
            def ClearField(self, field_name: typing_extensions___Literal[u"host",b"host",u"port",b"port"]) -> None: ...

    class BigQueryConfig(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        project_id = ... # type: typing___Text
        dataset_id = ... # type: typing___Text

        def __init__(self,
            *,
            project_id : typing___Optional[typing___Text] = None,
            dataset_id : typing___Optional[typing___Text] = None,
            ) -> None: ...
        @classmethod
        def FromString(cls, s: bytes) -> Store.BigQueryConfig: ...
        def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        if sys.version_info >= (3,):
            def ClearField(self, field_name: typing_extensions___Literal[u"dataset_id",u"project_id"]) -> None: ...
        else:
            def ClearField(self, field_name: typing_extensions___Literal[u"dataset_id",b"dataset_id",u"project_id",b"project_id"]) -> None: ...

    class CassandraConfig(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        host = ... # type: typing___Text
        port = ... # type: int

        def __init__(self,
            *,
            host : typing___Optional[typing___Text] = None,
            port : typing___Optional[int] = None,
            ) -> None: ...
        @classmethod
        def FromString(cls, s: bytes) -> Store.CassandraConfig: ...
        def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        if sys.version_info >= (3,):
            def ClearField(self, field_name: typing_extensions___Literal[u"host",u"port"]) -> None: ...
        else:
            def ClearField(self, field_name: typing_extensions___Literal[u"host",b"host",u"port",b"port"]) -> None: ...

    class Subscription(google___protobuf___message___Message):
        DESCRIPTOR: google___protobuf___descriptor___Descriptor = ...
        project = ... # type: typing___Text
        name = ... # type: typing___Text
        version = ... # type: typing___Text

        def __init__(self,
            *,
            project : typing___Optional[typing___Text] = None,
            name : typing___Optional[typing___Text] = None,
            version : typing___Optional[typing___Text] = None,
            ) -> None: ...
        @classmethod
        def FromString(cls, s: bytes) -> Store.Subscription: ...
        def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
        if sys.version_info >= (3,):
            def ClearField(self, field_name: typing_extensions___Literal[u"name",u"project",u"version"]) -> None: ...
        else:
            def ClearField(self, field_name: typing_extensions___Literal[u"name",b"name",u"project",b"project",u"version",b"version"]) -> None: ...

    name = ... # type: typing___Text
    type = ... # type: Store.StoreType

    @property
    def subscriptions(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[Store.Subscription]: ...

    @property
    def redis_config(self) -> Store.RedisConfig: ...

    @property
    def bigquery_config(self) -> Store.BigQueryConfig: ...

    @property
    def cassandra_config(self) -> Store.CassandraConfig: ...

    def __init__(self,
        *,
        name : typing___Optional[typing___Text] = None,
        type : typing___Optional[Store.StoreType] = None,
        subscriptions : typing___Optional[typing___Iterable[Store.Subscription]] = None,
        redis_config : typing___Optional[Store.RedisConfig] = None,
        bigquery_config : typing___Optional[Store.BigQueryConfig] = None,
        cassandra_config : typing___Optional[Store.CassandraConfig] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Store: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def HasField(self, field_name: typing_extensions___Literal[u"bigquery_config",u"cassandra_config",u"config",u"redis_config"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"bigquery_config",u"cassandra_config",u"config",u"name",u"redis_config",u"subscriptions",u"type"]) -> None: ...
    else:
        def HasField(self, field_name: typing_extensions___Literal[u"bigquery_config",b"bigquery_config",u"cassandra_config",b"cassandra_config",u"config",b"config",u"redis_config",b"redis_config"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"bigquery_config",b"bigquery_config",u"cassandra_config",b"cassandra_config",u"config",b"config",u"name",b"name",u"redis_config",b"redis_config",u"subscriptions",b"subscriptions",u"type",b"type"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions___Literal[u"config",b"config"]) -> typing_extensions___Literal["redis_config","bigquery_config","cassandra_config"]: ...
