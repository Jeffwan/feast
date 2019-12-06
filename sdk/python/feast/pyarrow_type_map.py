# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List

import pyarrow as pa
from feast.types.Value_pb2 import (
    Value as ProtoValue,
    ValueType as ProtoValueType,
    Int64List,
    Int32List,
    BoolList,
    BytesList,
    DoubleList,
    StringList,
    FloatList,
)
from feast.value_type import ValueType
from google.protobuf.timestamp_pb2 import Timestamp
from pyarrow.lib import TimestampType

# Mapping of PyArrow type to attribute name in Feast ValueType strings
STR_TYPE_MAP = {
    "timestamp[ms]": "int64_val",
    "int32": "int32_val",
    "int64": "int64_val",
    "double": "double_val",
    "float": "float_val",
    "string": "string_val",
    "binary": "bytes_val",
    "bool": "bool_val",
    "list<item: int32>": "int32_list_val",
    "list<item: int64>": "int64_list_val",
    "list<item: double>": "double_list_val",
    "list<item: float>": "float_list_val",
    "list<item: string>": "string_list_val",
    "list<item: binary>": "bytes_list_val",
    "list<item: bool>": "bool_list_val",
}


def pa_to_feast_value_attr(pa_type: object):
    """
    Returns the equivalent Feast ValueType string for the given pa.lib type.

    :param pa_type: PyArrow type.
    :type pa_type: object
    :return: Feast attribute name in Feast ValueType string-ed representation.
    :rtype: str
    """
    return STR_TYPE_MAP[pa_type.__str__()]


def pa_to_value_type(pa_type: object):
    """
    Returns the equivalent Feast ValueType for the given pa.lib type.
    :param pa_type: PyArrow type.
    :type pa_type: object
    :return: Feast ValueType.
    :rtype: feast.types.Value_pb2.ValueType
    """
    # Mapping of PyArrow to attribute name in Feast ValueType
    type_map = {
        "timestamp[ms]": ProtoValueType.INT64,
        "int32": ProtoValueType.INT32,
        "int64": ProtoValueType.INT64,
        "double": ProtoValueType.DOUBLE,
        "float": ProtoValueType.FLOAT,
        "string": ProtoValueType.STRING,
        "binary": ProtoValueType.BYTES,
        "bool": ProtoValueType.BOOL,
        "list<item: int32>": ProtoValueType.INT32_LIST,
        "list<item: int64>": ProtoValueType.INT64_LIST,
        "list<item: double>": ProtoValueType.DOUBLE_LIST,
        "list<item: float>": ProtoValueType.FLOAT_LIST,
        "list<item: string>": ProtoValueType.STRING_LIST,
        "list<item: binary>": ProtoValueType.BYTES_LIST,
        "list<item: bool>": ProtoValueType.BOOL_LIST,
    }
    return type_map[pa_type.__str__()]


def pa_to_feast_value_type(
        value: object
) -> ValueType:
    type_map = {
        "timestamp[ms]": ValueType.INT64,
        "int32": ValueType.INT32,
        "int64": ValueType.INT64,
        "double": ValueType.DOUBLE,
        "float": ValueType.FLOAT,
        "string": ValueType.STRING,
        "binary": ValueType.BYTES,
        "bool": ValueType.BOOL,
        "list<item: int32>": ValueType.INT32_LIST,
        "list<item: int64>": ValueType.INT64_LIST,
        "list<item: double>": ValueType.DOUBLE_LIST,
        "list<item: float>": ValueType.FLOAT_LIST,
        "list<item: string>": ValueType.STRING_LIST,
        "list<item: binary>": ValueType.BYTES_LIST,
        "list<item: bool>": ValueType.BOOL_LIST,
    }
    return type_map[value.type.__str__()]


def pa_column_to_timestamp_proto_column(
        column: pa.lib.ChunkedArray
) -> Timestamp:
    if not isinstance(column.type, TimestampType):
        raise Exception("Only TimestampType columns are allowed")

    proto_column = []
    for val in column:
        timestamp = Timestamp()
        timestamp.FromMicroseconds(
            micros=int(val.as_py().timestamp() * 1_000_000))
        proto_column.append(timestamp)
    return proto_column


def pa_column_to_proto_column(
        feast_value_type,
        column: pa.lib.ChunkedArray
) -> List[ProtoValue]:
    type_map = {ValueType.INT32: "int32_val",
                ValueType.INT64: "int64_val",
                ValueType.FLOAT: "float_val",
                ValueType.DOUBLE: "double_val",
                ValueType.STRING: "string_val",
                ValueType.BYTES: "bytes_val",
                ValueType.BOOL: "bool_val",
                ValueType.BOOL_LIST: {"bool_list_val": BoolList},
                ValueType.BYTES_LIST: {"bytes_list_val": BytesList},
                ValueType.STRING_LIST: {"string_list_val": StringList},
                ValueType.FLOAT_LIST: {"float_list_val": FloatList},
                ValueType.DOUBLE_LIST: {"double_list_val": DoubleList},
                ValueType.INT32_LIST: {"int32_list_val": Int32List},
                ValueType.INT64_LIST: {"int64_list_val": Int64List}, }

    value = type_map[feast_value_type]
    # Process list types
    if type(value) == dict:
        list_param_name = list(value.keys())[0]
        return [ProtoValue(
            **{list_param_name: value[list_param_name](val=x.as_py())})
            for x in column]
    else:
        return [ProtoValue(**{value: x.as_py()}) for x in column]
