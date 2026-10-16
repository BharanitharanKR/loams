from loams.live.v1 import admin_pb2 as _admin_pb2
from loams.live.v1 import live_pb2 as _live_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TableJobKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TABLE_JOB_KIND_UNSPECIFIED: _ClassVar[TableJobKind]
    TABLE_JOB_KIND_VALIDATE: _ClassVar[TableJobKind]
    TABLE_JOB_KIND_BACKFILL: _ClassVar[TableJobKind]
TABLE_JOB_KIND_UNSPECIFIED: TableJobKind
TABLE_JOB_KIND_VALIDATE: TableJobKind
TABLE_JOB_KIND_BACKFILL: TableJobKind

class TableDef(_message.Message):
    __slots__ = ("format", "id", "name", "indexes", "next_index_id", "validator", "enforcing")
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    NEXT_INDEX_ID_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_FIELD_NUMBER: _ClassVar[int]
    ENFORCING_FIELD_NUMBER: _ClassVar[int]
    format: int
    id: int
    name: str
    indexes: _containers.RepeatedCompositeFieldContainer[IndexDef]
    next_index_id: int
    validator: _live_pb2.Validator
    enforcing: EnforcingValidator
    def __init__(self, format: _Optional[int] = ..., id: _Optional[int] = ..., name: _Optional[str] = ..., indexes: _Optional[_Iterable[_Union[IndexDef, _Mapping]]] = ..., next_index_id: _Optional[int] = ..., validator: _Optional[_Union[_live_pb2.Validator, _Mapping]] = ..., enforcing: _Optional[_Union[EnforcingValidator, _Mapping]] = ...) -> None: ...

class EnforcingValidator(_message.Message):
    __slots__ = ("validator", "deployment")
    VALIDATOR_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    validator: _live_pb2.Validator
    deployment: bytes
    def __init__(self, validator: _Optional[_Union[_live_pb2.Validator, _Mapping]] = ..., deployment: _Optional[bytes] = ...) -> None: ...

class TableJobRecord(_message.Message):
    __slots__ = ("format", "kind", "table_id", "deployment", "owner", "lease_expires_ms", "checkpoint", "scanned", "done", "findings")
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_MS_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    SCANNED_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    format: int
    kind: TableJobKind
    table_id: int
    deployment: bytes
    owner: str
    lease_expires_ms: int
    checkpoint: bytes
    scanned: int
    done: bool
    findings: _containers.RepeatedCompositeFieldContainer[JobFinding]
    def __init__(self, format: _Optional[int] = ..., kind: _Optional[_Union[TableJobKind, str]] = ..., table_id: _Optional[int] = ..., deployment: _Optional[bytes] = ..., owner: _Optional[str] = ..., lease_expires_ms: _Optional[int] = ..., checkpoint: _Optional[bytes] = ..., scanned: _Optional[int] = ..., done: _Optional[bool] = ..., findings: _Optional[_Iterable[_Union[JobFinding, _Mapping]]] = ...) -> None: ...

class JobFinding(_message.Message):
    __slots__ = ("doc_id", "detail")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    doc_id: bytes
    detail: str
    def __init__(self, doc_id: _Optional[bytes] = ..., detail: _Optional[str] = ...) -> None: ...

class IndexDef(_message.Message):
    __slots__ = ("id", "name", "fields", "state", "deployment")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    fields: _containers.RepeatedScalarFieldContainer[str]
    state: _admin_pb2.IndexState
    deployment: bytes
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., fields: _Optional[_Iterable[str]] = ..., state: _Optional[_Union[_admin_pb2.IndexState, str]] = ..., deployment: _Optional[bytes] = ...) -> None: ...

class AppDef(_message.Message):
    __slots__ = ("format", "journal_shards")
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    JOURNAL_SHARDS_FIELD_NUMBER: _ClassVar[int]
    format: int
    journal_shards: int
    def __init__(self, format: _Optional[int] = ..., journal_shards: _Optional[int] = ...) -> None: ...

class DeploymentRecord(_message.Message):
    __slots__ = ("format", "id", "bundle_sha256", "schema", "created_at_ms", "created_by", "message", "parent", "state", "failure_reason", "functions")
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    BUNDLE_SHA256_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    format: int
    id: bytes
    bundle_sha256: bytes
    schema: _live_pb2.Schema
    created_at_ms: int
    created_by: str
    message: str
    parent: bytes
    state: _admin_pb2.DeploymentState
    failure_reason: str
    functions: _containers.RepeatedCompositeFieldContainer[_admin_pb2.FunctionInfo]
    def __init__(self, format: _Optional[int] = ..., id: _Optional[bytes] = ..., bundle_sha256: _Optional[bytes] = ..., schema: _Optional[_Union[_live_pb2.Schema, _Mapping]] = ..., created_at_ms: _Optional[int] = ..., created_by: _Optional[str] = ..., message: _Optional[str] = ..., parent: _Optional[bytes] = ..., state: _Optional[_Union[_admin_pb2.DeploymentState, str]] = ..., failure_reason: _Optional[str] = ..., functions: _Optional[_Iterable[_Union[_admin_pb2.FunctionInfo, _Mapping]]] = ...) -> None: ...

class AppRecord(_message.Message):
    __slots__ = ("format", "app", "cursor_key_ref")
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    APP_FIELD_NUMBER: _ClassVar[int]
    CURSOR_KEY_REF_FIELD_NUMBER: _ClassVar[int]
    format: int
    app: _admin_pb2.App
    cursor_key_ref: str
    def __init__(self, format: _Optional[int] = ..., app: _Optional[_Union[_admin_pb2.App, _Mapping]] = ..., cursor_key_ref: _Optional[str] = ...) -> None: ...

class AppIntent(_message.Message):
    __slots__ = ("format", "id", "name", "keyspace", "started_at_ms")
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    KEYSPACE_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    format: int
    id: int
    name: str
    keyspace: str
    started_at_ms: int
    def __init__(self, format: _Optional[int] = ..., id: _Optional[int] = ..., name: _Optional[str] = ..., keyspace: _Optional[str] = ..., started_at_ms: _Optional[int] = ...) -> None: ...
