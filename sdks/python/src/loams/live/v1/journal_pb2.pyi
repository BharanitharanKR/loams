from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WriteKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WRITE_KIND_UNSPECIFIED: _ClassVar[WriteKind]
    WRITE_KIND_INSERT: _ClassVar[WriteKind]
    WRITE_KIND_REPLACE: _ClassVar[WriteKind]
    WRITE_KIND_DELETE: _ClassVar[WriteKind]

class CatalogChangeKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_CHANGE_KIND_UNSPECIFIED: _ClassVar[CatalogChangeKind]
    CATALOG_CHANGE_KIND_TABLE_CREATED: _ClassVar[CatalogChangeKind]
    CATALOG_CHANGE_KIND_INDEXES: _ClassVar[CatalogChangeKind]
    CATALOG_CHANGE_KIND_VALIDATOR: _ClassVar[CatalogChangeKind]
    CATALOG_CHANGE_KIND_DEPLOYMENT_ACTIVATED: _ClassVar[CatalogChangeKind]
    CATALOG_CHANGE_KIND_APP_SETTINGS: _ClassVar[CatalogChangeKind]
WRITE_KIND_UNSPECIFIED: WriteKind
WRITE_KIND_INSERT: WriteKind
WRITE_KIND_REPLACE: WriteKind
WRITE_KIND_DELETE: WriteKind
CATALOG_CHANGE_KIND_UNSPECIFIED: CatalogChangeKind
CATALOG_CHANGE_KIND_TABLE_CREATED: CatalogChangeKind
CATALOG_CHANGE_KIND_INDEXES: CatalogChangeKind
CATALOG_CHANGE_KIND_VALIDATOR: CatalogChangeKind
CATALOG_CHANGE_KIND_DEPLOYMENT_ACTIVATED: CatalogChangeKind
CATALOG_CHANGE_KIND_APP_SETTINGS: CatalogChangeKind

class JournalEntry(_message.Message):
    __slots__ = ("commit_hint_ms", "writes", "function", "request_id", "catalog")
    COMMIT_HINT_MS_FIELD_NUMBER: _ClassVar[int]
    WRITES_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    commit_hint_ms: int
    writes: _containers.RepeatedCompositeFieldContainer[WriteRecord]
    function: str
    request_id: str
    catalog: _containers.RepeatedCompositeFieldContainer[CatalogChange]
    def __init__(self, commit_hint_ms: _Optional[int] = ..., writes: _Optional[_Iterable[_Union[WriteRecord, _Mapping]]] = ..., function: _Optional[str] = ..., request_id: _Optional[str] = ..., catalog: _Optional[_Iterable[_Union[CatalogChange, _Mapping]]] = ...) -> None: ...

class WriteRecord(_message.Message):
    __slots__ = ("table_id", "doc_id", "kind", "index_keys_removed", "index_keys_added")
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    INDEX_KEYS_REMOVED_FIELD_NUMBER: _ClassVar[int]
    INDEX_KEYS_ADDED_FIELD_NUMBER: _ClassVar[int]
    table_id: int
    doc_id: bytes
    kind: WriteKind
    index_keys_removed: _containers.RepeatedScalarFieldContainer[bytes]
    index_keys_added: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, table_id: _Optional[int] = ..., doc_id: _Optional[bytes] = ..., kind: _Optional[_Union[WriteKind, str]] = ..., index_keys_removed: _Optional[_Iterable[bytes]] = ..., index_keys_added: _Optional[_Iterable[bytes]] = ...) -> None: ...

class CatalogChange(_message.Message):
    __slots__ = ("kind", "table_id", "deployment")
    KIND_FIELD_NUMBER: _ClassVar[int]
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    kind: CatalogChangeKind
    table_id: int
    deployment: str
    def __init__(self, kind: _Optional[_Union[CatalogChangeKind, str]] = ..., table_id: _Optional[int] = ..., deployment: _Optional[str] = ...) -> None: ...
