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

class Project(_message.Message):
    __slots__ = ("id", "slug", "name", "description", "created_at", "environment_count", "agent_count")
    ID_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    AGENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    id: str
    slug: str
    name: str
    description: str
    created_at: _timestamp_pb2.Timestamp
    environment_count: int
    agent_count: int
    def __init__(self, id: _Optional[str] = ..., slug: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., environment_count: _Optional[int] = ..., agent_count: _Optional[int] = ...) -> None: ...

class Grant(_message.Message):
    __slots__ = ("id", "subject", "role", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    subject: _common_pb2.SubjectRef
    role: _common_pb2.ProjectRole
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., subject: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., role: _Optional[_Union[_common_pb2.ProjectRole, str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Environment(_message.Message):
    __slots__ = ("id", "slug", "name", "project", "namespace", "protected", "region", "bucket", "created_at", "stats")
    ID_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PROTECTED_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    id: str
    slug: str
    name: str
    project: str
    namespace: str
    protected: bool
    region: str
    bucket: str
    created_at: _timestamp_pb2.Timestamp
    stats: EnvironmentStats
    def __init__(self, id: _Optional[str] = ..., slug: _Optional[str] = ..., name: _Optional[str] = ..., project: _Optional[str] = ..., namespace: _Optional[str] = ..., protected: _Optional[bool] = ..., region: _Optional[str] = ..., bucket: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stats: _Optional[_Union[EnvironmentStats, _Mapping]] = ...) -> None: ...

class EnvironmentStats(_message.Message):
    __slots__ = ("collections", "documents", "storage_bytes", "queries_24h", "writes_24h")
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    STORAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    QUERIES_24H_FIELD_NUMBER: _ClassVar[int]
    WRITES_24H_FIELD_NUMBER: _ClassVar[int]
    collections: int
    documents: int
    storage_bytes: int
    queries_24h: int
    writes_24h: int
    def __init__(self, collections: _Optional[int] = ..., documents: _Optional[int] = ..., storage_bytes: _Optional[int] = ..., queries_24h: _Optional[int] = ..., writes_24h: _Optional[int] = ...) -> None: ...

class ActivityDay(_message.Message):
    __slots__ = ("date", "queries", "writes", "storage_bytes")
    DATE_FIELD_NUMBER: _ClassVar[int]
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    WRITES_FIELD_NUMBER: _ClassVar[int]
    STORAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    date: str
    queries: int
    writes: int
    storage_bytes: int
    def __init__(self, date: _Optional[str] = ..., queries: _Optional[int] = ..., writes: _Optional[int] = ..., storage_bytes: _Optional[int] = ...) -> None: ...

class ListProjectsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListProjectsResponse(_message.Message):
    __slots__ = ("projects", "next_page_token")
    PROJECTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    projects: _containers.RepeatedCompositeFieldContainer[Project]
    next_page_token: str
    def __init__(self, projects: _Optional[_Iterable[_Union[Project, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateProjectRequest(_message.Message):
    __slots__ = ("name", "slug", "description")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    slug: str
    description: str
    def __init__(self, name: _Optional[str] = ..., slug: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class CreateProjectResponse(_message.Message):
    __slots__ = ("project",)
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    project: Project
    def __init__(self, project: _Optional[_Union[Project, _Mapping]] = ...) -> None: ...

class GetProjectRequest(_message.Message):
    __slots__ = ("project",)
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    project: str
    def __init__(self, project: _Optional[str] = ...) -> None: ...

class GetProjectResponse(_message.Message):
    __slots__ = ("project",)
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    project: Project
    def __init__(self, project: _Optional[_Union[Project, _Mapping]] = ...) -> None: ...

class ListAccessRequest(_message.Message):
    __slots__ = ("project", "page_size", "page_token")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    project: str
    page_size: int
    page_token: str
    def __init__(self, project: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListAccessResponse(_message.Message):
    __slots__ = ("grants", "next_page_token")
    GRANTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    grants: _containers.RepeatedCompositeFieldContainer[Grant]
    next_page_token: str
    def __init__(self, grants: _Optional[_Iterable[_Union[Grant, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GrantAccessRequest(_message.Message):
    __slots__ = ("project", "subject", "role")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    project: str
    subject: _common_pb2.SubjectRef
    role: _common_pb2.ProjectRole
    def __init__(self, project: _Optional[str] = ..., subject: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., role: _Optional[_Union[_common_pb2.ProjectRole, str]] = ...) -> None: ...

class GrantAccessResponse(_message.Message):
    __slots__ = ("grant",)
    GRANT_FIELD_NUMBER: _ClassVar[int]
    grant: Grant
    def __init__(self, grant: _Optional[_Union[Grant, _Mapping]] = ...) -> None: ...

class ListEnvironmentsRequest(_message.Message):
    __slots__ = ("project", "page_size", "page_token")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    project: str
    page_size: int
    page_token: str
    def __init__(self, project: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListEnvironmentsResponse(_message.Message):
    __slots__ = ("environments", "next_page_token")
    ENVIRONMENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    environments: _containers.RepeatedCompositeFieldContainer[Environment]
    next_page_token: str
    def __init__(self, environments: _Optional[_Iterable[_Union[Environment, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateEnvironmentRequest(_message.Message):
    __slots__ = ("project", "name", "slug", "protected")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    PROTECTED_FIELD_NUMBER: _ClassVar[int]
    project: str
    name: str
    slug: str
    protected: bool
    def __init__(self, project: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ..., protected: _Optional[bool] = ...) -> None: ...

class CreateEnvironmentResponse(_message.Message):
    __slots__ = ("environment",)
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    environment: Environment
    def __init__(self, environment: _Optional[_Union[Environment, _Mapping]] = ...) -> None: ...

class GetEnvironmentRequest(_message.Message):
    __slots__ = ("project", "environment")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    project: str
    environment: str
    def __init__(self, project: _Optional[str] = ..., environment: _Optional[str] = ...) -> None: ...

class GetEnvironmentResponse(_message.Message):
    __slots__ = ("environment",)
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    environment: Environment
    def __init__(self, environment: _Optional[_Union[Environment, _Mapping]] = ...) -> None: ...

class GetEnvironmentActivityRequest(_message.Message):
    __slots__ = ("project", "environment")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    project: str
    environment: str
    def __init__(self, project: _Optional[str] = ..., environment: _Optional[str] = ...) -> None: ...

class GetEnvironmentActivityResponse(_message.Message):
    __slots__ = ("environment", "period_start", "period_end", "series")
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    PERIOD_START_FIELD_NUMBER: _ClassVar[int]
    PERIOD_END_FIELD_NUMBER: _ClassVar[int]
    SERIES_FIELD_NUMBER: _ClassVar[int]
    environment: str
    period_start: _timestamp_pb2.Timestamp
    period_end: _timestamp_pb2.Timestamp
    series: _containers.RepeatedCompositeFieldContainer[ActivityDay]
    def __init__(self, environment: _Optional[str] = ..., period_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., period_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., series: _Optional[_Iterable[_Union[ActivityDay, _Mapping]]] = ...) -> None: ...
