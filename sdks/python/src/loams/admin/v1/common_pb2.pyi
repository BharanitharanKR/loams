import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrgRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORG_ROLE_UNSPECIFIED: _ClassVar[OrgRole]
    ORG_ROLE_OWNER: _ClassVar[OrgRole]
    ORG_ROLE_ADMIN: _ClassVar[OrgRole]
    ORG_ROLE_MEMBER: _ClassVar[OrgRole]

class ProjectRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROJECT_ROLE_UNSPECIFIED: _ClassVar[ProjectRole]
    PROJECT_ROLE_ADMIN: _ClassVar[ProjectRole]
    PROJECT_ROLE_DEVELOPER: _ClassVar[ProjectRole]
    PROJECT_ROLE_VIEWER: _ClassVar[ProjectRole]

class SubjectKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUBJECT_KIND_UNSPECIFIED: _ClassVar[SubjectKind]
    SUBJECT_KIND_USER: _ClassVar[SubjectKind]
    SUBJECT_KIND_TEAM: _ClassVar[SubjectKind]
    SUBJECT_KIND_AGENT: _ClassVar[SubjectKind]
    SUBJECT_KIND_SERVICE_ACCOUNT: _ClassVar[SubjectKind]
    SUBJECT_KIND_SYSTEM: _ClassVar[SubjectKind]
ORG_ROLE_UNSPECIFIED: OrgRole
ORG_ROLE_OWNER: OrgRole
ORG_ROLE_ADMIN: OrgRole
ORG_ROLE_MEMBER: OrgRole
PROJECT_ROLE_UNSPECIFIED: ProjectRole
PROJECT_ROLE_ADMIN: ProjectRole
PROJECT_ROLE_DEVELOPER: ProjectRole
PROJECT_ROLE_VIEWER: ProjectRole
SUBJECT_KIND_UNSPECIFIED: SubjectKind
SUBJECT_KIND_USER: SubjectKind
SUBJECT_KIND_TEAM: SubjectKind
SUBJECT_KIND_AGENT: SubjectKind
SUBJECT_KIND_SERVICE_ACCOUNT: SubjectKind
SUBJECT_KIND_SYSTEM: SubjectKind

class User(_message.Message):
    __slots__ = ("id", "name", "email", "avatar_url", "two_factor", "sso", "created_at", "last_seen_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    AVATAR_URL_FIELD_NUMBER: _ClassVar[int]
    TWO_FACTOR_FIELD_NUMBER: _ClassVar[int]
    SSO_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    email: str
    avatar_url: str
    two_factor: bool
    sso: str
    created_at: _timestamp_pb2.Timestamp
    last_seen_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., email: _Optional[str] = ..., avatar_url: _Optional[str] = ..., two_factor: _Optional[bool] = ..., sso: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_seen_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SubjectRef(_message.Message):
    __slots__ = ("kind", "id", "name")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    kind: SubjectKind
    id: str
    name: str
    def __init__(self, kind: _Optional[_Union[SubjectKind, str]] = ..., id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class ResourceRef(_message.Message):
    __slots__ = ("kind", "id", "name")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    kind: str
    id: str
    name: str
    def __init__(self, kind: _Optional[str] = ..., id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...
