from loams.live.v1 import live_pb2 as _live_pb2
from loams.options.v1 import options_pb2 as _options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeploymentState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEPLOYMENT_STATE_UNSPECIFIED: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_PENDING: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_VALIDATING: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_BACKFILLING: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_ACTIVE: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_SUPERSEDED: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_FAILED: _ClassVar[DeploymentState]
    DEPLOYMENT_STATE_DEGRADED: _ClassVar[DeploymentState]

class FunctionKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_KIND_UNSPECIFIED: _ClassVar[FunctionKind]
    FUNCTION_KIND_QUERY: _ClassVar[FunctionKind]
    FUNCTION_KIND_MUTATION: _ClassVar[FunctionKind]

class FunctionVisibility(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_VISIBILITY_UNSPECIFIED: _ClassVar[FunctionVisibility]
    FUNCTION_VISIBILITY_PUBLIC: _ClassVar[FunctionVisibility]
    FUNCTION_VISIBILITY_INTERNAL: _ClassVar[FunctionVisibility]

class IndexState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_STATE_UNSPECIFIED: _ClassVar[IndexState]
    INDEX_STATE_BACKFILLING: _ClassVar[IndexState]
    INDEX_STATE_READY: _ClassVar[IndexState]
    INDEX_STATE_DROPPING: _ClassVar[IndexState]

class LogLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOG_LEVEL_UNSPECIFIED: _ClassVar[LogLevel]
    LOG_LEVEL_DEBUG: _ClassVar[LogLevel]
    LOG_LEVEL_LOG: _ClassVar[LogLevel]
    LOG_LEVEL_INFO: _ClassVar[LogLevel]
    LOG_LEVEL_WARN: _ClassVar[LogLevel]
    LOG_LEVEL_ERROR: _ClassVar[LogLevel]
DEPLOYMENT_STATE_UNSPECIFIED: DeploymentState
DEPLOYMENT_STATE_PENDING: DeploymentState
DEPLOYMENT_STATE_VALIDATING: DeploymentState
DEPLOYMENT_STATE_BACKFILLING: DeploymentState
DEPLOYMENT_STATE_ACTIVE: DeploymentState
DEPLOYMENT_STATE_SUPERSEDED: DeploymentState
DEPLOYMENT_STATE_FAILED: DeploymentState
DEPLOYMENT_STATE_DEGRADED: DeploymentState
FUNCTION_KIND_UNSPECIFIED: FunctionKind
FUNCTION_KIND_QUERY: FunctionKind
FUNCTION_KIND_MUTATION: FunctionKind
FUNCTION_VISIBILITY_UNSPECIFIED: FunctionVisibility
FUNCTION_VISIBILITY_PUBLIC: FunctionVisibility
FUNCTION_VISIBILITY_INTERNAL: FunctionVisibility
INDEX_STATE_UNSPECIFIED: IndexState
INDEX_STATE_BACKFILLING: IndexState
INDEX_STATE_READY: IndexState
INDEX_STATE_DROPPING: IndexState
LOG_LEVEL_UNSPECIFIED: LogLevel
LOG_LEVEL_DEBUG: LogLevel
LOG_LEVEL_LOG: LogLevel
LOG_LEVEL_INFO: LogLevel
LOG_LEVEL_WARN: LogLevel
LOG_LEVEL_ERROR: LogLevel

class Deployment(_message.Message):
    __slots__ = ("id", "bundle_sha256", "created_at_ms", "created_by", "message", "parent", "state", "failure_reason")
    ID_FIELD_NUMBER: _ClassVar[int]
    BUNDLE_SHA256_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    id: str
    bundle_sha256: str
    created_at_ms: int
    created_by: str
    message: str
    parent: str
    state: DeploymentState
    failure_reason: str
    def __init__(self, id: _Optional[str] = ..., bundle_sha256: _Optional[str] = ..., created_at_ms: _Optional[int] = ..., created_by: _Optional[str] = ..., message: _Optional[str] = ..., parent: _Optional[str] = ..., state: _Optional[_Union[DeploymentState, str]] = ..., failure_reason: _Optional[str] = ...) -> None: ...

class FunctionInfo(_message.Message):
    __slots__ = ("path", "kind", "visibility", "args")
    PATH_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    VISIBILITY_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    path: str
    kind: FunctionKind
    visibility: FunctionVisibility
    args: _live_pb2.Validator
    def __init__(self, path: _Optional[str] = ..., kind: _Optional[_Union[FunctionKind, str]] = ..., visibility: _Optional[_Union[FunctionVisibility, str]] = ..., args: _Optional[_Union[_live_pb2.Validator, _Mapping]] = ...) -> None: ...

class DeployRequest(_message.Message):
    __slots__ = ("bundle", "schema", "expected_active", "message", "dry_run")
    BUNDLE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    bundle: bytes
    schema: _live_pb2.Schema
    expected_active: str
    message: str
    dry_run: bool
    def __init__(self, bundle: _Optional[bytes] = ..., schema: _Optional[_Union[_live_pb2.Schema, _Mapping]] = ..., expected_active: _Optional[str] = ..., message: _Optional[str] = ..., dry_run: _Optional[bool] = ...) -> None: ...

class DeployResponse(_message.Message):
    __slots__ = ("deployment", "changes")
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    deployment: Deployment
    changes: SchemaChanges
    def __init__(self, deployment: _Optional[_Union[Deployment, _Mapping]] = ..., changes: _Optional[_Union[SchemaChanges, _Mapping]] = ...) -> None: ...

class SchemaChanges(_message.Message):
    __slots__ = ("tables_created", "validators_changed", "tables_validated", "indexes_added", "indexes_dropped")
    TABLES_CREATED_FIELD_NUMBER: _ClassVar[int]
    VALIDATORS_CHANGED_FIELD_NUMBER: _ClassVar[int]
    TABLES_VALIDATED_FIELD_NUMBER: _ClassVar[int]
    INDEXES_ADDED_FIELD_NUMBER: _ClassVar[int]
    INDEXES_DROPPED_FIELD_NUMBER: _ClassVar[int]
    tables_created: _containers.RepeatedScalarFieldContainer[str]
    validators_changed: _containers.RepeatedScalarFieldContainer[str]
    tables_validated: _containers.RepeatedScalarFieldContainer[str]
    indexes_added: _containers.RepeatedCompositeFieldContainer[IndexRef]
    indexes_dropped: _containers.RepeatedCompositeFieldContainer[IndexRef]
    def __init__(self, tables_created: _Optional[_Iterable[str]] = ..., validators_changed: _Optional[_Iterable[str]] = ..., tables_validated: _Optional[_Iterable[str]] = ..., indexes_added: _Optional[_Iterable[_Union[IndexRef, _Mapping]]] = ..., indexes_dropped: _Optional[_Iterable[_Union[IndexRef, _Mapping]]] = ...) -> None: ...

class IndexRef(_message.Message):
    __slots__ = ("table", "index")
    TABLE_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    table: str
    index: str
    def __init__(self, table: _Optional[str] = ..., index: _Optional[str] = ...) -> None: ...

class ListDeploymentsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListDeploymentsResponse(_message.Message):
    __slots__ = ("deployments", "next_page_token")
    DEPLOYMENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    deployments: _containers.RepeatedCompositeFieldContainer[Deployment]
    next_page_token: str
    def __init__(self, deployments: _Optional[_Iterable[_Union[Deployment, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetDeploymentRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetDeploymentResponse(_message.Message):
    __slots__ = ("deployment", "schema", "functions")
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    deployment: Deployment
    schema: _live_pb2.Schema
    functions: _containers.RepeatedCompositeFieldContainer[FunctionInfo]
    def __init__(self, deployment: _Optional[_Union[Deployment, _Mapping]] = ..., schema: _Optional[_Union[_live_pb2.Schema, _Mapping]] = ..., functions: _Optional[_Iterable[_Union[FunctionInfo, _Mapping]]] = ...) -> None: ...

class ActivateDeploymentRequest(_message.Message):
    __slots__ = ("id", "expected_active")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    id: str
    expected_active: str
    def __init__(self, id: _Optional[str] = ..., expected_active: _Optional[str] = ...) -> None: ...

class ActivateDeploymentResponse(_message.Message):
    __slots__ = ("deployment", "changes")
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    deployment: Deployment
    changes: SchemaChanges
    def __init__(self, deployment: _Optional[_Union[Deployment, _Mapping]] = ..., changes: _Optional[_Union[SchemaChanges, _Mapping]] = ...) -> None: ...

class GetIndexStatusRequest(_message.Message):
    __slots__ = ("table", "index")
    TABLE_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    table: str
    index: str
    def __init__(self, table: _Optional[str] = ..., index: _Optional[str] = ...) -> None: ...

class GetIndexStatusResponse(_message.Message):
    __slots__ = ("state", "done", "total_estimate")
    STATE_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ESTIMATE_FIELD_NUMBER: _ClassVar[int]
    state: IndexState
    done: int
    total_estimate: int
    def __init__(self, state: _Optional[_Union[IndexState, str]] = ..., done: _Optional[int] = ..., total_estimate: _Optional[int] = ...) -> None: ...

class TailLogsRequest(_message.Message):
    __slots__ = ("follow", "since_ms", "function")
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    follow: bool
    since_ms: int
    function: str
    def __init__(self, follow: _Optional[bool] = ..., since_ms: _Optional[int] = ..., function: _Optional[str] = ...) -> None: ...

class TailLogsResponse(_message.Message):
    __slots__ = ("entries", "skipped")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[LogEntry]
    skipped: int
    def __init__(self, entries: _Optional[_Iterable[_Union[LogEntry, _Mapping]]] = ..., skipped: _Optional[int] = ...) -> None: ...

class LogEntry(_message.Message):
    __slots__ = ("ts_ms", "request_id", "function", "deployment", "level", "line", "truncated")
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    ts_ms: int
    request_id: str
    function: str
    deployment: str
    level: LogLevel
    line: str
    truncated: bool
    def __init__(self, ts_ms: _Optional[int] = ..., request_id: _Optional[str] = ..., function: _Optional[str] = ..., deployment: _Optional[str] = ..., level: _Optional[_Union[LogLevel, str]] = ..., line: _Optional[str] = ..., truncated: _Optional[bool] = ...) -> None: ...
