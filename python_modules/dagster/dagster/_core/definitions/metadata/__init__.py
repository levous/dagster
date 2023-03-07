import os
from abc import ABC, abstractmethod
from inspect import Parameter
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    Union,
    overload,
)

from typing_extensions import TypeAlias

import dagster._check as check
import dagster._seven as seven
from dagster._annotations import PublicAttr, experimental, public
from dagster._core.errors import DagsterInvalidMetadata
from dagster._serdes import whitelist_for_serdes
from dagster._serdes.serdes import DefaultNamedTupleSerializer, WhitelistMap, replace_storage_keys
from dagster._utils.backcompat import (
    deprecation_warning,
    experimental_class_warning,
)

from .table import (  # re-exported
    TableColumn as TableColumn,
    TableColumnConstraints as TableColumnConstraints,
    TableConstraints as TableConstraints,
    TableRecord as TableRecord,
    TableSchema as TableSchema,
)

if TYPE_CHECKING:
    from dagster._core.definitions.events import AssetKey

RawMetadataValue = Union[
    "MetadataValue",
    Dict[Any, Any],
    float,
    int,
    List[Any],
    str,
    None,
]

MetadataMapping: TypeAlias = Mapping[str, "MetadataValue"]
MetadataUserInput: TypeAlias = Mapping[str, RawMetadataValue]


# ########################
# ##### NORMALIZATION
# ########################


@overload
def normalize_metadata(
    metadata: Mapping[str, RawMetadataValue],
    metadata_entries: Sequence["MetadataEntry"],
    allow_invalid: bool = False,
) -> Sequence["MetadataEntry"]:
    ...


@overload
def normalize_metadata(
    metadata: Mapping[str, RawMetadataValue],
    metadata_entries: Sequence[Union["MetadataEntry", "PartitionMetadataEntry"]],
    allow_invalid: bool = False,
) -> Sequence[Union["MetadataEntry", "PartitionMetadataEntry"]]:
    ...


def normalize_metadata(
    metadata: Mapping[str, RawMetadataValue],
    metadata_entries: Sequence[Union["MetadataEntry", "PartitionMetadataEntry"]],
    allow_invalid: bool = False,
) -> Sequence[Union["MetadataEntry", "PartitionMetadataEntry"]]:
    if metadata and metadata_entries:
        raise DagsterInvalidMetadata(
            "Attempted to provide both `metadata` and `metadata_entries` arguments to an event. "
            "Must provide only one of the two."
        )

    if metadata_entries:
        deprecation_warning(
            'Argument "metadata_entries"',
            "1.0.0",
            additional_warn_txt=(
                "Use argument `metadata` instead. The `MetadataEntry` `description` attribute is"
                " also deprecated-- argument `metadata` takes a label: value dictionary."
            ),
            stacklevel=4,  # to get the caller of `normalize_metadata`
        )
        return check.sequence_param(
            metadata_entries, "metadata_entries", (MetadataEntry, PartitionMetadataEntry)
        )

    # This is a stopgap measure to deal with unsupported metadata values, which occur when we try
    # to convert arbitrary metadata (on e.g. OutputDefinition) to a MetadataValue, which is required
    # for serialization. This will cause unsupported values to be silently replaced with a
    # string placeholder.
    elif allow_invalid:
        metadata_entries = []
        for k, v in metadata.items():
            try:
                metadata_entries.append(package_metadata_value(k, v))
            except DagsterInvalidMetadata:
                deprecation_warning(
                    "Support for arbitrary metadata values",
                    "1.0.0",
                    additional_warn_txt=(
                        "In the future, all user-supplied metadata values must be one of"
                        f" {RawMetadataValue}"
                    ),
                    stacklevel=4,  # to get the caller of `normalize_metadata`
                )
                metadata_entries.append(
                    MetadataEntry(
                        value=TextMetadataValue(f"[{v.__class__.__name__}] (unserializable)"),
                        label=k,
                    )
                )
        return metadata_entries

    return [
        package_metadata_value(k, v)
        for k, v in check.opt_mapping_param(metadata, "metadata", key_type=str).items()
    ]


def normalize_metadata_value(raw_value: RawMetadataValue) -> "MetadataValue":
    from dagster._core.definitions.events import AssetKey

    if isinstance(raw_value, MetadataValue):
        return raw_value
    elif isinstance(raw_value, str):
        return MetadataValue.text(raw_value)
    elif isinstance(raw_value, float):
        return MetadataValue.float(raw_value)
    elif isinstance(raw_value, bool):
        return MetadataValue.bool(raw_value)
    elif isinstance(raw_value, int):
        return MetadataValue.int(raw_value)
    elif isinstance(raw_value, (list, dict)):
        return MetadataValue.json(raw_value)
    elif isinstance(raw_value, os.PathLike):
        return MetadataValue.path(raw_value)
    elif isinstance(raw_value, AssetKey):
        return MetadataValue.asset(raw_value)
    elif isinstance(raw_value, TableSchema):
        return MetadataValue.table_schema(raw_value)
    elif raw_value is None:
        return MetadataValue.null()

    raise DagsterInvalidMetadata(
        f"Its type was {type(raw_value)}. Consider wrapping the value with the appropriate "
        "MetadataValue type."
    )


def package_metadata_value(label: str, raw_value: RawMetadataValue) -> "MetadataEntry":
    check.str_param(label, "label")

    if isinstance(raw_value, (MetadataEntry, PartitionMetadataEntry)):
        raise DagsterInvalidMetadata(
            f"Expected a metadata value, found an instance of {raw_value.__class__.__name__}."
            " Consider instead using a MetadataValue wrapper for the value."
        )
    try:
        value = normalize_metadata_value(raw_value)
    except DagsterInvalidMetadata as e:
        raise DagsterInvalidMetadata(
            f'Could not resolve the metadata value for "{label}" to a known type. {e}'
        ) from None
    return MetadataEntry(label=label, value=value)


# ########################
# ##### METADATA VALUE
# ########################


class MetadataValue(ABC):
    """Utility class to wrap metadata values passed into Dagster events so that they can be
    displayed in Dagit and other tooling.

    .. code-block:: python

        @op
        def emit_metadata(context, df):
            yield AssetMaterialization(
                asset_key="my_dataset",
                metadata={
                    "my_text_label": "hello",
                    "dashboard_url": MetadataValue.url("http://mycoolsite.com/my_dashboard"),
                    "num_rows": 0,
                },
            )
    """

    @public
    @property
    @abstractmethod
    def value(self) -> object:
        raise NotImplementedError()

    @public
    @staticmethod
    def text(text: str) -> "TextMetadataValue":
        """Static constructor for a metadata value wrapping text as
        :py:class:`TextMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context, df):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "my_text_label": MetadataValue.text("hello")
                    },
                )

        Args:
            text (str): The text string for a metadata entry.
        """
        return TextMetadataValue(text)

    @public
    @staticmethod
    def url(url: str) -> "UrlMetadataValue":
        """Static constructor for a metadata value wrapping a URL as
        :py:class:`UrlMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context):
                yield AssetMaterialization(
                    asset_key="my_dashboard",
                    metadata={
                        "dashboard_url": MetadataValue.url("http://mycoolsite.com/my_dashboard"),
                    }
                )


        Args:
            url (str): The URL for a metadata entry.
        """
        return UrlMetadataValue(url)

    @public
    @staticmethod
    def path(path: Union[str, os.PathLike]) -> "PathMetadataValue":
        """Static constructor for a metadata value wrapping a path as
        :py:class:`PathMetadataValue`. For example:

        .. code-block:: python

            @op
            def emit_metadata(context):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "filepath": MetadataValue.path("path/to/file"),
                    }
                )

        Args:
            path (str): The path for a metadata entry.
        """
        return PathMetadataValue(path)

    @public
    @staticmethod
    def notebook(path: Union[str, os.PathLike]) -> "NotebookMetadataValue":
        """Static constructor for a metadata value wrapping a notebook path as
        :py:class:`NotebookMetadataValue`. For example:

        .. code-block:: python

            @op
            def emit_metadata(context):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "notebook_path": MetadataValue.notebook("path/to/notebook.ipynb"),
                    }
                )

        Args:
            path (str): The path to a notebook for a metadata entry.
        """
        return NotebookMetadataValue(path)

    @public
    @staticmethod
    def json(data: Union[Sequence[Any], Mapping[str, Any]]) -> "JsonMetadataValue":
        """Static constructor for a metadata value wrapping a json-serializable list or dict
        as :py:class:`JsonMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context):
                yield ExpectationResult(
                    success=not missing_things,
                    label="is_present",
                    metadata={
                        "about my dataset": MetadataValue.json({"missing_columns": missing_things})
                    },
                )

        Args:
            data (Union[Sequence[Any], Mapping[str, Any]]): The JSON data for a metadata entry.
        """
        return JsonMetadataValue(data)

    @public
    @staticmethod
    def md(data: str) -> "MarkdownMetadataValue":
        """Static constructor for a metadata value wrapping markdown data as
        :py:class:`MarkdownMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:


        .. code-block:: python

            @op
            def emit_metadata(context, md_str):
                yield AssetMaterialization(
                    asset_key="info",
                    metadata={
                        'Details': MetadataValue.md(md_str)
                    },
                )

        Args:
            md_str (str): The markdown for a metadata entry.
        """
        return MarkdownMetadataValue(data)

    @public
    @staticmethod
    def python_artifact(python_artifact: Callable) -> "PythonArtifactMetadataValue":
        """Static constructor for a metadata value wrapping a python artifact as
        :py:class:`PythonArtifactMetadataValue`. Can be used as the value type for the
        `metadata` parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context, df):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "class": MetadataValue.python_artifact(MyClass),
                        "function": MetadataValue.python_artifact(my_function),
                    }
                )

        Args:
            value (Callable): The python class or function for a metadata entry.
        """
        check.callable_param(python_artifact, "python_artifact")
        return PythonArtifactMetadataValue(python_artifact.__module__, python_artifact.__name__)

    @public
    @staticmethod
    def float(value: float) -> "FloatMetadataValue":
        """Static constructor for a metadata value wrapping a float as
        :py:class:`FloatMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context, df):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "size (bytes)": MetadataValue.float(calculate_bytes(df)),
                    }
                )

        Args:
            value (float): The float value for a metadata entry.
        """
        return FloatMetadataValue(value)

    @public
    @staticmethod
    def int(value: int) -> "IntMetadataValue":
        """Static constructor for a metadata value wrapping an int as
        :py:class:`IntMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context, df):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "number of rows": MetadataValue.int(len(df)),
                    },
                )

        Args:
            value (int): The int value for a metadata entry.
        """
        return IntMetadataValue(value)

    @public
    @staticmethod
    def bool(value: bool) -> "BoolMetadataValue":
        """Static constructor for a metadata value wrapping a bool as
        :py:class:`BoolMetadataValuye`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context, df):
                yield AssetMaterialization(
                    asset_key="my_dataset",
                    metadata={
                        "num rows > 1000": MetadataValue.bool(len(df) > 1000),
                    },
                )

        Args:
            value (bool): The bool value for a metadata entry.
        """
        return BoolMetadataValue(value)

    @public
    @staticmethod
    def dagster_run(run_id: str) -> "DagsterRunMetadataValue":
        """Static constructor for a metadata value wrapping a reference to a Dagster run.

        Args:
            run_id (str): The ID of the run.
        """
        return DagsterRunMetadataValue(run_id)

    @public
    @staticmethod
    def asset(asset_key: "AssetKey") -> "DagsterAssetMetadataValue":
        """Static constructor for a metadata value referencing a Dagster asset, by key.

        For example:

        .. code-block:: python

            @op
            def validate_table(context, df):
                yield AssetMaterialization(
                    asset_key=AssetKey("my_table"),
                    metadata={
                        "Related asset": MetadataValue.asset(AssetKey('my_other_table')),
                    },
                )

        Args:
            asset_key (AssetKey): The asset key referencing the asset.
        """
        from dagster._core.definitions.events import AssetKey

        check.inst_param(asset_key, "asset_key", AssetKey)
        return DagsterAssetMetadataValue(asset_key)

    @public
    @staticmethod
    @experimental
    def table(
        records: Sequence[TableRecord], schema: Optional[TableSchema] = None
    ) -> "TableMetadataValue":
        """Static constructor for a metadata value wrapping arbitrary tabular data as
        :py:class:`TableMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events. For example:

        .. code-block:: python

            @op
            def emit_metadata(context):
                yield ExpectationResult(
                    success=not has_errors,
                    label="is_valid",
                    metadata={
                        "errors": MetadataValue.table(
                            records=[
                                TableRecord(code="invalid-data-type", row=2, col="name"),
                            ],
                            schema=TableSchema(
                                columns=[
                                    TableColumn(name="code", type="string"),
                                    TableColumn(name="row", type="int"),
                                    TableColumn(name="col", type="string"),
                                ]
                            )
                        ),
                    },
                )
        """
        return TableMetadataValue(records, schema)

    @public
    @staticmethod
    def table_schema(
        schema: TableSchema,
    ) -> "TableSchemaMetadataValue":
        """Static constructor for a metadata value wrapping a table schema as
        :py:class:`TableSchemaMetadataValue`. Can be used as the value type
        for the `metadata` parameter for supported events. For example:

        .. code-block:: python

            schema = TableSchema(
                columns = [
                    TableColumn(name="id", type="int"),
                    TableColumn(name="status", type="bool"),
                ]
            )

            DagsterType(
                type_check_fn=some_validation_fn,
                name='MyTable',
                metadata={
                    'my_table_schema': MetadataValue.table_schema(schema),
                }
            )

        Args:
            schema (TableSchema): The table schema for a metadata entry.
        """
        return TableSchemaMetadataValue(schema)

    @public
    @staticmethod
    def null() -> "NullMetadataValue":
        """Static constructor for a metadata value representing null. Can be used as the value type
        for the `metadata` parameter for supported events.
        """
        return NullMetadataValue()


# ########################
# ##### METADATA VALUE TYPES
# ########################

# NOTE: We have `type: ignore` in a few places below because mypy complains about an instance method
# (e.g. `text`) overriding a static method on the superclass of the same name. This is not a concern
# for us because these static methods should never be called on instances.

# NOTE: `XMetadataValue` classes are serialized with a storage name of `XMetadataEntryData` to
# maintain backward compatibility. See docstring of `whitelist_for_serdes` for more info.


@whitelist_for_serdes(storage_name="TextMetadataEntryData")
class TextMetadataValue(
    NamedTuple(
        "_TextMetadataValue",
        [
            ("text", PublicAttr[Optional[str]]),
        ],
    ),
    MetadataValue,
):
    """Container class for text metadata entry data.

    Args:
        text (Optional[str]): The text data.
    """

    def __new__(cls, text: Optional[str]):
        return super(TextMetadataValue, cls).__new__(
            cls, check.opt_str_param(text, "text", default="")
        )

    @public
    @property
    def value(self) -> Optional[str]:
        return self.text


@whitelist_for_serdes(storage_name="UrlMetadataEntryData")
class UrlMetadataValue(
    NamedTuple(
        "_UrlMetadataValue",
        [
            ("url", PublicAttr[Optional[str]]),
        ],
    ),
    MetadataValue,
):
    """Container class for URL metadata entry data.

    Args:
        url (Optional[str]): The URL as a string.
    """

    def __new__(cls, url: Optional[str]):
        return super(UrlMetadataValue, cls).__new__(
            cls, check.opt_str_param(url, "url", default="")
        )

    @public
    @property
    def value(self) -> Optional[str]:
        return self.url


@whitelist_for_serdes(storage_name="PathMetadataEntryData")
class PathMetadataValue(
    NamedTuple("_PathMetadataValue", [("path", PublicAttr[Optional[str]])]), MetadataValue
):
    """Container class for path metadata entry data.

    Args:
        path (Optional[str]): The path as a string or conforming to os.PathLike.
    """

    def __new__(cls, path: Optional[Union[str, os.PathLike]]):
        return super(PathMetadataValue, cls).__new__(
            cls, check.opt_path_param(path, "path", default="")
        )

    @public
    @property
    def value(self) -> Optional[str]:
        return self.path


@whitelist_for_serdes(storage_name="NotebookMetadataEntryData")
class NotebookMetadataValue(
    NamedTuple("_NotebookMetadataValue", [("path", PublicAttr[Optional[str]])]), MetadataValue
):
    """Container class for notebook metadata entry data.

    Args:
        path (Optional[str]): The path to the notebook as a string or conforming to os.PathLike.
    """

    def __new__(cls, path: Optional[Union[str, os.PathLike]]):
        return super(NotebookMetadataValue, cls).__new__(
            cls, check.opt_path_param(path, "path", default="")
        )

    @public
    @property
    def value(self) -> Optional[str]:
        return self.path


@whitelist_for_serdes(storage_name="JsonMetadataEntryData")
class JsonMetadataValue(
    NamedTuple(
        "_JsonMetadataValue",
        [
            ("data", PublicAttr[Optional[Union[Sequence[Any], Mapping[str, Any]]]]),
        ],
    ),
    MetadataValue,
):
    """Container class for JSON metadata entry data.

    Args:
        data (Dict[str, Any]): The JSON data.
    """

    def __new__(cls, data: Optional[Union[Sequence[Any], Mapping[str, Any]]]):
        data = check.opt_inst_param(data, "data", (Sequence, Mapping))
        try:
            # check that the value is JSON serializable
            seven.dumps(data)
        except TypeError:
            raise DagsterInvalidMetadata("Value is a dictionary but is not JSON serializable.")
        return super(JsonMetadataValue, cls).__new__(cls, data)

    @public
    @property
    def value(self) -> Optional[Union[Sequence[Any], Mapping[str, Any]]]:
        return self.data


@whitelist_for_serdes(storage_name="MarkdownMetadataEntryData")
class MarkdownMetadataValue(
    NamedTuple(
        "_MarkdownMetadataValue",
        [
            ("md_str", PublicAttr[Optional[str]]),
        ],
    ),
    MetadataValue,
):
    """Container class for markdown metadata entry data.

    Args:
        md_str (Optional[str]): The markdown as a string.
    """

    def __new__(cls, md_str: Optional[str]):
        return super(MarkdownMetadataValue, cls).__new__(
            cls, check.opt_str_param(md_str, "md_str", default="")
        )

    @property
    def value(self) -> Optional[str]:
        return self.md_str


@whitelist_for_serdes(storage_name="PythonArtifactMetadataEntryData")
class PythonArtifactMetadataValue(
    NamedTuple(
        "_PythonArtifactMetadataValue",
        [
            ("module", PublicAttr[str]),
            ("name", PublicAttr[str]),
        ],
    ),
    MetadataValue,
):
    """Container class for python artifact metadata entry data.

    Args:
        module (str): The module where the python artifact can be found
        name (str): The name of the python artifact
    """

    def __new__(cls, module: str, name: str):
        return super(PythonArtifactMetadataValue, cls).__new__(
            cls, check.str_param(module, "module"), check.str_param(name, "name")
        )

    @public
    @property
    def value(self) -> object:
        return self


@whitelist_for_serdes(storage_name="FloatMetadataEntryData")
class FloatMetadataValue(
    NamedTuple(
        "_FloatMetadataValue",
        [
            ("value", PublicAttr[Optional[float]]),
        ],
    ),
    MetadataValue,
):
    """Container class for float metadata entry data.

    Args:
        value (Optional[float]): The float value.
    """

    def __new__(cls, value: Optional[float]):
        return super(FloatMetadataValue, cls).__new__(cls, check.opt_float_param(value, "value"))


@whitelist_for_serdes(storage_name="IntMetadataEntryData")
class IntMetadataValue(
    NamedTuple(
        "_IntMetadataValue",
        [
            ("value", PublicAttr[Optional[int]]),
        ],
    ),
    MetadataValue,
):
    """Container class for int metadata entry data.

    Args:
        value (Optional[int]): The int value.
    """

    def __new__(cls, value: Optional[int]):
        return super(IntMetadataValue, cls).__new__(cls, check.opt_int_param(value, "value"))


@whitelist_for_serdes(storage_name="BoolMetadataEntryData")
class BoolMetadataValue(
    NamedTuple("_BoolMetadataValue", [("value", PublicAttr[Optional[bool]])]),
    MetadataValue,
):
    """Container class for bool metadata entry data.

    Args:
        value (Optional[bool]): The bool value.
    """

    def __new__(cls, value: Optional[bool]):
        return super(BoolMetadataValue, cls).__new__(cls, check.opt_bool_param(value, "value"))


@whitelist_for_serdes(storage_name="DagsterPipelineRunMetadataEntryData")
class DagsterRunMetadataValue(
    NamedTuple(
        "_DagsterRunMetadataValue",
        [
            ("run_id", PublicAttr[str]),
        ],
    ),
    MetadataValue,
):
    """Representation of a dagster run.

    Args:
        run_id (str): The run id
    """

    def __new__(cls, run_id: str):
        return super(DagsterRunMetadataValue, cls).__new__(cls, check.str_param(run_id, "run_id"))

    @property
    def value(self) -> str:
        return self.run_id


@whitelist_for_serdes(storage_name="DagsterAssetMetadataEntryData")
class DagsterAssetMetadataValue(
    NamedTuple("_DagsterAssetMetadataValue", [("asset_key", PublicAttr["AssetKey"])]), MetadataValue
):
    """Representation of a dagster asset.

    Args:
        asset_key (AssetKey): The dagster asset key
    """

    def __new__(cls, asset_key: "AssetKey"):
        from dagster._core.definitions.events import AssetKey

        return super(DagsterAssetMetadataValue, cls).__new__(
            cls, check.inst_param(asset_key, "asset_key", AssetKey)
        )

    @public
    @property
    def value(self) -> "AssetKey":
        return self.value


@experimental
@whitelist_for_serdes(storage_name="TableMetadataEntryData")
class TableMetadataValue(
    NamedTuple(
        "_TableMetadataValue",
        [
            ("records", PublicAttr[Sequence[TableRecord]]),
            ("schema", PublicAttr[TableSchema]),
        ],
    ),
    MetadataValue,
):
    """Container class for table metadata entry data.

    Args:
        records (TableRecord): The data as a list of records (i.e. rows).
        schema (Optional[TableSchema]): A schema for the table.
    """

    @public
    @staticmethod
    def infer_column_type(value):
        if isinstance(value, bool):
            return "bool"
        elif isinstance(value, int):
            return "int"
        elif isinstance(value, float):
            return "float"
        else:
            return "string"

    def __new__(cls, records: Sequence[TableRecord], schema: Optional[TableSchema]):
        check.sequence_param(records, "records", of_type=TableRecord)
        check.opt_inst_param(schema, "schema", TableSchema)

        if len(records) == 0:
            schema = check.not_none(schema, "schema must be provided if records is empty")
        else:
            columns = set(records[0].data.keys())
            for record in records[1:]:
                check.invariant(
                    set(record.data.keys()) == columns, "All records must have the same fields"
                )
            schema = schema or TableSchema(
                columns=[
                    TableColumn(name=k, type=TableMetadataValue.infer_column_type(v))
                    for k, v in records[0].data.items()
                ]
            )

        return super(TableMetadataValue, cls).__new__(
            cls,
            records,
            schema,
        )

    @public
    @property
    def value(self):
        return self


@whitelist_for_serdes(storage_name="TableSchemaMetadataEntryData")
class TableSchemaMetadataValue(
    NamedTuple("_TableSchemaMetadataValue", [("schema", PublicAttr[TableSchema])]), MetadataValue
):
    """Representation of a schema for arbitrary tabular data.

    Args:
        schema (TableSchema): The dictionary containing the schema representation.
    """

    def __new__(cls, schema: TableSchema):
        return super(TableSchemaMetadataValue, cls).__new__(
            cls, check.inst_param(schema, "schema", TableSchema)
        )

    @property
    def value(self) -> TableSchema:
        return self.schema


@whitelist_for_serdes(storage_name="NullMetadataEntryData")
class NullMetadataValue(NamedTuple("_NullMetadataValue", []), MetadataValue):
    """Representation of null."""

    @property
    def value(self) -> None:
        return None


# ########################
# ##### METADATA ENTRY
# ########################


class MetadataEntrySerializer(DefaultNamedTupleSerializer):
    """Custom serializer for MetadataEntry.
    This is necessary to support backcompat.
    """

    @classmethod
    def value_from_storage_dict(
        cls,
        storage_dict: Dict[str, Any],
        klass: Type["MetadataEntry"],
        args_for_class: Mapping[str, Parameter],
        whitelist_map: WhitelistMap,
        descent_path: str,
    ):
        storage_dict = replace_storage_keys(storage_dict, {"entry_data": "value"})
        if "description" in storage_dict:
            del storage_dict["description"]
        return super().value_from_storage_dict(
            storage_dict, klass, args_for_class, whitelist_map, descent_path
        )

    @classmethod
    def value_to_storage_dict(
        cls,
        value: NamedTuple,
        whitelist_map: WhitelistMap,
        descent_path: str,
    ) -> Dict[str, Any]:
        storage_dict = super().value_to_storage_dict(
            value,
            whitelist_map,
            descent_path,
        )
        # So older version of dagster can read this
        storage_dict["description"] = None
        return replace_storage_keys(
            storage_dict,
            {
                "value": "entry_data",
            },
        )


# NOTE: This would better be implemented as a generic with `MetadataValue` set as a
# typevar, but as of 2022-01-25 mypy does not support generics on NamedTuple.
@whitelist_for_serdes(storage_name="EventMetadataEntry", serializer=MetadataEntrySerializer)
class MetadataEntry(
    NamedTuple(
        "_MetadataEntry",
        [
            ("label", PublicAttr[str]),
            ("value", PublicAttr[MetadataValue]),
        ],
    ),
):
    """The standard structure for describing metadata for Dagster events.

    Lists of objects of this type can be passed as arguments to Dagster events and will be displayed
    in Dagit and other tooling.

    Should be yielded from within an IO manager to append metadata for a given input/output event.
    For other event types, passing a dict with `MetadataValue` values to the `metadata` argument
    is preferred.

    Args:
        label (str): Short display label for this metadata entry.
        value (MetadataValue): Typed metadata entry data. The different types allow
            for customized display in tools like dagit.
    """

    def __new__(
        cls,
        label: str,
        value: "RawMetadataValue" = None,
    ):
        value = normalize_metadata_value(value)

        return super(MetadataEntry, cls).__new__(
            cls,
            check.str_param(label, "label"),
            check.inst_param(value, "value", MetadataValue),
        )


class PartitionMetadataEntry(
    NamedTuple(
        "_PartitionMetadataEntry",
        [
            ("partition", PublicAttr[str]),
            ("entry", PublicAttr["MetadataEntry"]),
        ],
    )
):
    """Event containing an :py:class:`MetadataEntry` and the name of a partition that the entry
    applies to.

    This can be yielded or returned in place of MetadataEntries for cases where you are trying
    to associate metadata more precisely.
    """

    def __new__(cls, partition: str, entry: MetadataEntry):
        experimental_class_warning("PartitionMetadataEntry")
        return super(PartitionMetadataEntry, cls).__new__(
            cls,
            check.str_param(partition, "partition"),
            check.inst_param(entry, "entry", MetadataEntry),
        )
