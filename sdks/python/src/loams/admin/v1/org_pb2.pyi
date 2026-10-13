import datetime

from google.protobuf import field_mask_pb2 as _field_mask_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from loams.admin.v1 import common_pb2 as _common_pb2
from loams.options.v1 import options_pb2 as _options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Org(_message.Message):
    __slots__ = ("id", "name", "slug", "created_at", "require_two_factor", "allowed_domains")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_TWO_FACTOR_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DOMAINS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    slug: str
    created_at: _timestamp_pb2.Timestamp
    require_two_factor: bool
    allowed_domains: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., require_two_factor: _Optional[bool] = ..., allowed_domains: _Optional[_Iterable[str]] = ...) -> None: ...

class Member(_message.Message):
    __slots__ = ("user", "role", "teams", "joined_at")
    USER_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TEAMS_FIELD_NUMBER: _ClassVar[int]
    JOINED_AT_FIELD_NUMBER: _ClassVar[int]
    user: _common_pb2.User
    role: _common_pb2.OrgRole
    teams: _containers.RepeatedScalarFieldContainer[str]
    joined_at: _timestamp_pb2.Timestamp
    def __init__(self, user: _Optional[_Union[_common_pb2.User, _Mapping]] = ..., role: _Optional[_Union[_common_pb2.OrgRole, str]] = ..., teams: _Optional[_Iterable[str]] = ..., joined_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Invitation(_message.Message):
    __slots__ = ("id", "email", "role", "teams", "invited_by", "created_at", "expires_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TEAMS_FIELD_NUMBER: _ClassVar[int]
    INVITED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    email: str
    role: _common_pb2.OrgRole
    teams: _containers.RepeatedScalarFieldContainer[str]
    invited_by: _common_pb2.SubjectRef
    created_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., email: _Optional[str] = ..., role: _Optional[_Union[_common_pb2.OrgRole, str]] = ..., teams: _Optional[_Iterable[str]] = ..., invited_by: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Team(_message.Message):
    __slots__ = ("id", "name", "slug", "description", "member_count", "idp_group", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    IDP_GROUP_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    slug: str
    description: str
    member_count: int
    idp_group: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., slug: _Optional[str] = ..., description: _Optional[str] = ..., member_count: _Optional[int] = ..., idp_group: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ProjectGrantRef(_message.Message):
    __slots__ = ("project", "role")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    project: str
    role: _common_pb2.ProjectRole
    def __init__(self, project: _Optional[str] = ..., role: _Optional[_Union[_common_pb2.ProjectRole, str]] = ...) -> None: ...

class GetOrgRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetOrgResponse(_message.Message):
    __slots__ = ("org",)
    ORG_FIELD_NUMBER: _ClassVar[int]
    org: Org
    def __init__(self, org: _Optional[_Union[Org, _Mapping]] = ...) -> None: ...

class UpdateOrgRequest(_message.Message):
    __slots__ = ("org", "update_mask")
    ORG_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    org: Org
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, org: _Optional[_Union[Org, _Mapping]] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateOrgResponse(_message.Message):
    __slots__ = ("org",)
    ORG_FIELD_NUMBER: _ClassVar[int]
    org: Org
    def __init__(self, org: _Optional[_Union[Org, _Mapping]] = ...) -> None: ...

class ListMembersRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListMembersResponse(_message.Message):
    __slots__ = ("members", "next_page_token")
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    members: _containers.RepeatedCompositeFieldContainer[Member]
    next_page_token: str
    def __init__(self, members: _Optional[_Iterable[_Union[Member, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class RemoveMemberRequest(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: str
    def __init__(self, user: _Optional[str] = ...) -> None: ...

class RemoveMemberResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListInvitationsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListInvitationsResponse(_message.Message):
    __slots__ = ("invitations", "next_page_token")
    INVITATIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    invitations: _containers.RepeatedCompositeFieldContainer[Invitation]
    next_page_token: str
    def __init__(self, invitations: _Optional[_Iterable[_Union[Invitation, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateInvitationRequest(_message.Message):
    __slots__ = ("email", "role", "teams")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    TEAMS_FIELD_NUMBER: _ClassVar[int]
    email: str
    role: _common_pb2.OrgRole
    teams: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, email: _Optional[str] = ..., role: _Optional[_Union[_common_pb2.OrgRole, str]] = ..., teams: _Optional[_Iterable[str]] = ...) -> None: ...

class CreateInvitationResponse(_message.Message):
    __slots__ = ("invitation",)
    INVITATION_FIELD_NUMBER: _ClassVar[int]
    invitation: Invitation
    def __init__(self, invitation: _Optional[_Union[Invitation, _Mapping]] = ...) -> None: ...

class ListTeamsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListTeamsResponse(_message.Message):
    __slots__ = ("teams", "next_page_token")
    TEAMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    teams: _containers.RepeatedCompositeFieldContainer[Team]
    next_page_token: str
    def __init__(self, teams: _Optional[_Iterable[_Union[Team, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateTeamRequest(_message.Message):
    __slots__ = ("name", "description", "idp_group")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    IDP_GROUP_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    idp_group: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., idp_group: _Optional[str] = ...) -> None: ...

class CreateTeamResponse(_message.Message):
    __slots__ = ("team",)
    TEAM_FIELD_NUMBER: _ClassVar[int]
    team: Team
    def __init__(self, team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class GetTeamRequest(_message.Message):
    __slots__ = ("team",)
    TEAM_FIELD_NUMBER: _ClassVar[int]
    team: str
    def __init__(self, team: _Optional[str] = ...) -> None: ...

class GetTeamResponse(_message.Message):
    __slots__ = ("team", "members", "projects")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    PROJECTS_FIELD_NUMBER: _ClassVar[int]
    team: Team
    members: _containers.RepeatedCompositeFieldContainer[_common_pb2.User]
    projects: _containers.RepeatedCompositeFieldContainer[ProjectGrantRef]
    def __init__(self, team: _Optional[_Union[Team, _Mapping]] = ..., members: _Optional[_Iterable[_Union[_common_pb2.User, _Mapping]]] = ..., projects: _Optional[_Iterable[_Union[ProjectGrantRef, _Mapping]]] = ...) -> None: ...

class AddTeamMemberRequest(_message.Message):
    __slots__ = ("team", "user")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    team: str
    user: str
    def __init__(self, team: _Optional[str] = ..., user: _Optional[str] = ...) -> None: ...

class AddTeamMemberResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
