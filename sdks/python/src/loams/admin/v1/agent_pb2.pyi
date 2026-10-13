import datetime

from google.protobuf import field_mask_pb2 as _field_mask_pb2
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

class AgentStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AGENT_STATUS_UNSPECIFIED: _ClassVar[AgentStatus]
    AGENT_STATUS_ACTIVE: _ClassVar[AgentStatus]
    AGENT_STATUS_SUSPENDED: _ClassVar[AgentStatus]

class TokenFlow(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TOKEN_FLOW_UNSPECIFIED: _ClassVar[TokenFlow]
    TOKEN_FLOW_FEDERATION: _ClassVar[TokenFlow]
    TOKEN_FLOW_DELEGATION: _ClassVar[TokenFlow]
    TOKEN_FLOW_VENDING: _ClassVar[TokenFlow]
AGENT_STATUS_UNSPECIFIED: AgentStatus
AGENT_STATUS_ACTIVE: AgentStatus
AGENT_STATUS_SUSPENDED: AgentStatus
TOKEN_FLOW_UNSPECIFIED: TokenFlow
TOKEN_FLOW_FEDERATION: TokenFlow
TOKEN_FLOW_DELEGATION: TokenFlow
TOKEN_FLOW_VENDING: TokenFlow

class PolicyRule(_message.Message):
    __slots__ = ("environment", "actions", "collections")
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    environment: str
    actions: _containers.RepeatedScalarFieldContainer[str]
    collections: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, environment: _Optional[str] = ..., actions: _Optional[_Iterable[str]] = ..., collections: _Optional[_Iterable[str]] = ...) -> None: ...

class AgentLimits(_message.Message):
    __slots__ = ("requests_per_s", "concurrent_queries")
    REQUESTS_PER_S_FIELD_NUMBER: _ClassVar[int]
    CONCURRENT_QUERIES_FIELD_NUMBER: _ClassVar[int]
    requests_per_s: int
    concurrent_queries: int
    def __init__(self, requests_per_s: _Optional[int] = ..., concurrent_queries: _Optional[int] = ...) -> None: ...

class Agent(_message.Message):
    __slots__ = ("id", "name", "description", "project", "owner", "status", "policy", "max_token_ttl_s", "limits", "created_at", "last_active_at", "active_tokens")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    MAX_TOKEN_TTL_S_FIELD_NUMBER: _ClassVar[int]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_ACTIVE_AT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    project: str
    owner: _common_pb2.SubjectRef
    status: AgentStatus
    policy: _containers.RepeatedCompositeFieldContainer[PolicyRule]
    max_token_ttl_s: int
    limits: AgentLimits
    created_at: _timestamp_pb2.Timestamp
    last_active_at: _timestamp_pb2.Timestamp
    active_tokens: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., project: _Optional[str] = ..., owner: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., status: _Optional[_Union[AgentStatus, str]] = ..., policy: _Optional[_Iterable[_Union[PolicyRule, _Mapping]]] = ..., max_token_ttl_s: _Optional[int] = ..., limits: _Optional[_Union[AgentLimits, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_active_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., active_tokens: _Optional[int] = ...) -> None: ...

class AgentToken(_message.Message):
    __slots__ = ("jti", "agent", "environment", "scopes", "flow", "subject", "acting_for", "issued_at", "expires_at")
    JTI_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    FLOW_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ACTING_FOR_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    jti: str
    agent: str
    environment: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    flow: TokenFlow
    subject: str
    acting_for: _common_pb2.SubjectRef
    issued_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, jti: _Optional[str] = ..., agent: _Optional[str] = ..., environment: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., flow: _Optional[_Union[TokenFlow, str]] = ..., subject: _Optional[str] = ..., acting_for: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., issued_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class TrustPolicy(_message.Message):
    __slots__ = ("id", "agent", "name", "issuer", "audience", "subject_pattern", "environments", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_PATTERN_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENTS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    agent: str
    name: str
    issuer: str
    audience: str
    subject_pattern: str
    environments: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., agent: _Optional[str] = ..., name: _Optional[str] = ..., issuer: _Optional[str] = ..., audience: _Optional[str] = ..., subject_pattern: _Optional[str] = ..., environments: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ServiceAccount(_message.Message):
    __slots__ = ("id", "name", "description", "project", "created_at", "key_count", "last_active_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    KEY_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_ACTIVE_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    project: str
    created_at: _timestamp_pb2.Timestamp
    key_count: int
    last_active_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., project: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., key_count: _Optional[int] = ..., last_active_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListAgentsRequest(_message.Message):
    __slots__ = ("project", "page_size", "page_token")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    project: str
    page_size: int
    page_token: str
    def __init__(self, project: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListAgentsResponse(_message.Message):
    __slots__ = ("agents", "next_page_token")
    AGENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    agents: _containers.RepeatedCompositeFieldContainer[Agent]
    next_page_token: str
    def __init__(self, agents: _Optional[_Iterable[_Union[Agent, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateAgentRequest(_message.Message):
    __slots__ = ("project", "name", "description", "owner", "policy", "max_token_ttl_s", "limits")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    MAX_TOKEN_TTL_S_FIELD_NUMBER: _ClassVar[int]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    project: str
    name: str
    description: str
    owner: _common_pb2.SubjectRef
    policy: _containers.RepeatedCompositeFieldContainer[PolicyRule]
    max_token_ttl_s: int
    limits: AgentLimits
    def __init__(self, project: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., owner: _Optional[_Union[_common_pb2.SubjectRef, _Mapping]] = ..., policy: _Optional[_Iterable[_Union[PolicyRule, _Mapping]]] = ..., max_token_ttl_s: _Optional[int] = ..., limits: _Optional[_Union[AgentLimits, _Mapping]] = ...) -> None: ...

class CreateAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class GetAgentRequest(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: str
    def __init__(self, agent: _Optional[str] = ...) -> None: ...

class GetAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class UpdateAgentRequest(_message.Message):
    __slots__ = ("agent", "update_mask")
    AGENT_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class UpdateAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class SuspendAgentRequest(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: str
    def __init__(self, agent: _Optional[str] = ...) -> None: ...

class SuspendAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class ResumeAgentRequest(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: str
    def __init__(self, agent: _Optional[str] = ...) -> None: ...

class ResumeAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class ListAgentTokensRequest(_message.Message):
    __slots__ = ("agent", "page_size", "page_token")
    AGENT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    agent: str
    page_size: int
    page_token: str
    def __init__(self, agent: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListAgentTokensResponse(_message.Message):
    __slots__ = ("tokens", "next_page_token")
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tokens: _containers.RepeatedCompositeFieldContainer[AgentToken]
    next_page_token: str
    def __init__(self, tokens: _Optional[_Iterable[_Union[AgentToken, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class ListTrustPoliciesRequest(_message.Message):
    __slots__ = ("agent", "page_size", "page_token")
    AGENT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    agent: str
    page_size: int
    page_token: str
    def __init__(self, agent: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListTrustPoliciesResponse(_message.Message):
    __slots__ = ("trust_policies", "next_page_token")
    TRUST_POLICIES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    trust_policies: _containers.RepeatedCompositeFieldContainer[TrustPolicy]
    next_page_token: str
    def __init__(self, trust_policies: _Optional[_Iterable[_Union[TrustPolicy, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateTrustPolicyRequest(_message.Message):
    __slots__ = ("agent", "name", "issuer", "audience", "subject_pattern", "environments")
    AGENT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ISSUER_FIELD_NUMBER: _ClassVar[int]
    AUDIENCE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_PATTERN_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENTS_FIELD_NUMBER: _ClassVar[int]
    agent: str
    name: str
    issuer: str
    audience: str
    subject_pattern: str
    environments: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, agent: _Optional[str] = ..., name: _Optional[str] = ..., issuer: _Optional[str] = ..., audience: _Optional[str] = ..., subject_pattern: _Optional[str] = ..., environments: _Optional[_Iterable[str]] = ...) -> None: ...

class CreateTrustPolicyResponse(_message.Message):
    __slots__ = ("trust_policy",)
    TRUST_POLICY_FIELD_NUMBER: _ClassVar[int]
    trust_policy: TrustPolicy
    def __init__(self, trust_policy: _Optional[_Union[TrustPolicy, _Mapping]] = ...) -> None: ...

class ListServiceAccountsRequest(_message.Message):
    __slots__ = ("project", "page_size", "page_token")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    project: str
    page_size: int
    page_token: str
    def __init__(self, project: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListServiceAccountsResponse(_message.Message):
    __slots__ = ("service_accounts", "next_page_token")
    SERVICE_ACCOUNTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    service_accounts: _containers.RepeatedCompositeFieldContainer[ServiceAccount]
    next_page_token: str
    def __init__(self, service_accounts: _Optional[_Iterable[_Union[ServiceAccount, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateServiceAccountRequest(_message.Message):
    __slots__ = ("project", "name", "description")
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    project: str
    name: str
    description: str
    def __init__(self, project: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class CreateServiceAccountResponse(_message.Message):
    __slots__ = ("service_account",)
    SERVICE_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    service_account: ServiceAccount
    def __init__(self, service_account: _Optional[_Union[ServiceAccount, _Mapping]] = ...) -> None: ...
