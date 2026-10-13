import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from loams.admin.v1 import common_pb2 as _common_pb2
from loams.options.v1 import options_pb2 as _options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AuditOutcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUDIT_OUTCOME_UNSPECIFIED: _ClassVar[AuditOutcome]
    AUDIT_OUTCOME_SUCCESS: _ClassVar[AuditOutcome]
    AUDIT_OUTCOME_DENIED: _ClassVar[AuditOutcome]
AUDIT_OUTCOME_UNSPECIFIED: AuditOutcome
AUDIT_OUTCOME_SUCCESS: AuditOutcome
AUDIT_OUTCOME_DENIED: AuditOutcome

class AuditEvent(_message.Message):
    __slots__ = ("id", "at", "actor", "acting_for", "action", "target", "project", "environment", "ip", "outcome")
    ID_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    ACTING_FOR_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    id: str
    at: _timestamp_pb2.Timestamp
    actor: _common_pb2.SubjectRef
    acting_for: _common_pb2.SubjectRef
    action: str
    target: _common_pb2.ResourceRef
    project: str
    environment: str
    ip: str
    outcome: AuditOutcome
    def __init__(self, id: _Optional[str] = ..., at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., actor: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., acting_for: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., action: _Optional[str] = ..., target: _Optional[_Union[_common_pb2.ResourceRef, _Mapping]] = ..., project: _Optional[str] = ..., environment: _Optional[str] = ..., ip: _Optional[str] = ..., outcome: _Optional[_Union[AuditOutcome, str]] = ...) -> None: ...

class ListAuditEventsRequest(_message.Message):
    __slots__ = ("project", "actor", "page_size", "page_token")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    project: str
    actor: str
    page_size: int
    page_token: str
    def __init__(self, project: _Optional[str] = ..., actor: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListAuditEventsResponse(_message.Message):
    __slots__ = ("events", "next_page_token")
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[AuditEvent]
    next_page_token: str
    def __init__(self, events: _Optional[_Iterable[_Union[AuditEvent, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class WatchAuditEventsRequest(_message.Message):
    __slots__ = ("project", "actor", "snapshot_size", "resume_cursor")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_SIZE_FIELD_NUMBER: _ClassVar[int]
    RESUME_CURSOR_FIELD_NUMBER: _ClassVar[int]
    project: str
    actor: str
    snapshot_size: int
    resume_cursor: str
    def __init__(self, project: _Optional[str] = ..., actor: _Optional[str] = ..., snapshot_size: _Optional[int] = ..., resume_cursor: _Optional[str] = ...) -> None: ...

class WatchAuditEventsResponse(_message.Message):
    __slots__ = ("snapshot", "append", "heartbeat", "cursor", "snapshot_reset")
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    APPEND_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_RESET_FIELD_NUMBER: _ClassVar[int]
    snapshot: AuditSnapshot
    append: AuditEvent
    heartbeat: Heartbeat
    cursor: str
    snapshot_reset: bool
    def __init__(self, snapshot: _Optional[_Union[AuditSnapshot, _Mapping]] = ..., append: _Optional[_Union[AuditEvent, _Mapping]] = ..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]] = ..., cursor: _Optional[str] = ..., snapshot_reset: _Optional[bool] = ...) -> None: ...

class AuditSnapshot(_message.Message):
    __slots__ = ("events",)
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[AuditEvent]
    def __init__(self, events: _Optional[_Iterable[_Union[AuditEvent, _Mapping]]] = ...) -> None: ...

class Heartbeat(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
