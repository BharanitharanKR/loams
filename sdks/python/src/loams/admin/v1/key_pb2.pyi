import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from loams.admin.v1 import common_pb2 as _common_pb2
from loams.options.v1 import options_pb2 as _options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiKey(_message.Message):
    __slots__ = ("id", "name", "prefix", "environment", "principal", "scopes", "created_at", "expires_at", "last_used_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_USED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    prefix: str
    environment: str
    principal: _common_pb2.SubjectRef
    scopes: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    last_used_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., prefix: _Optional[str] = ..., environment: _Optional[str] = ..., principal: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., scopes: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_used_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListKeysRequest(_message.Message):
    __slots__ = ("project", "environment", "page_size", "page_token")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    project: str
    environment: str
    page_size: int
    page_token: str
    def __init__(self, project: _Optional[str] = ..., environment: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListKeysResponse(_message.Message):
    __slots__ = ("keys", "next_page_token")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[ApiKey]
    next_page_token: str
    def __init__(self, keys: _Optional[_Iterable[_Union[ApiKey, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateKeyRequest(_message.Message):
    __slots__ = ("project", "environment", "name", "service_account", "scopes", "expires_in_days", "idempotency_key")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SERVICE_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_DAYS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    project: str
    environment: str
    name: str
    service_account: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    expires_in_days: int
    idempotency_key: str
    def __init__(self, project: _Optional[str] = ..., environment: _Optional[str] = ..., name: _Optional[str] = ..., service_account: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., expires_in_days: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CreateKeyResponse(_message.Message):
    __slots__ = ("key", "secret")
    KEY_FIELD_NUMBER: _ClassVar[int]
    SECRET_FIELD_NUMBER: _ClassVar[int]
    key: ApiKey
    secret: str
    def __init__(self, key: _Optional[_Union[ApiKey, _Mapping]] = ..., secret: _Optional[str] = ...) -> None: ...

class RevokeKeyRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class RevokeKeyResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RevokeTokenRequest(_message.Message):
    __slots__ = ("jti",)
    JTI_FIELD_NUMBER: _ClassVar[int]
    jti: str
    def __init__(self, jti: _Optional[str] = ...) -> None: ...

class RevokeTokenResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
