// @loams/admin-ui/testing: an in-memory loams.auth.v1 and loams.admin.v1 for the
// console pages' tests, a stub router and session, and a render wrapper with the
// console's server state and toasts. The seed is a small Acme org: one project with a
// development and a protected production environment, two agents, keys and events.

import { create } from '@bufbuild/protobuf';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { createRouterTransport, type ServiceImpl, type Transport } from '@connectrpc/connect';
import type {
  ConsoleAccount,
  Location,
  RouterService,
  SessionGate,
  SessionService,
} from '@loams/console-host';
import { createQueryClient, ServerStateProvider } from '@loams/console-host/testing';
import { admin, auth, collection } from '@loams/proto';
import { Toaster, ToastProvider } from '@loams/ui';
import type { ReactNode } from 'react';

// Base UI's popups measure their anchors; jsdom has no ResizeObserver.
globalThis.ResizeObserver ??= class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

const at = (minutesAgo: number) => timestampFromDate(new Date(Date.now() - minutesAgo * 60_000));

export const SEED = {
  org: create(admin.OrgSchema, {
    id: 'org_acme',
    name: 'Acme Robotics',
    slug: 'acme',
    createdAt: at(60 * 24 * 90),
    allowedDomains: ['acme.dev'],
  }),
  user: create(admin.UserSchema, { id: 'usr_01', name: 'Priya Raman', email: 'priya@acme.dev' }),
  projects: [
    create(admin.ProjectSchema, {
      id: 'prj_docs',
      slug: 'docs-search',
      name: 'Docs search',
      description: 'Hybrid search over the product docs.',
      environmentCount: 2,
      agentCount: 2,
    }),
    create(admin.ProjectSchema, {
      id: 'prj_code',
      slug: 'code-index',
      name: 'Code index',
      description: 'Code, searchable by coding agents.',
      environmentCount: 0,
      agentCount: 0,
    }),
  ],
  environments: [
    create(admin.EnvironmentSchema, {
      id: 'env_dev',
      slug: 'development',
      name: 'Development',
      project: 'docs-search',
      namespace: 'docs-search-development',
      region: 'us-east-1',
      bucket: 's3://acme-loams/docs-search',
      stats: {
        collections: 2n,
        documents: 394n,
        storageBytes: 2_200_000n,
        queries24h: 2223n,
        writes24h: 44n,
      },
    }),
    create(admin.EnvironmentSchema, {
      id: 'env_prod',
      slug: 'production',
      name: 'Production',
      project: 'docs-search',
      namespace: 'docs-search-production',
      protected: true,
      region: 'us-east-1',
      bucket: 's3://acme-loams/docs-search',
      stats: {
        collections: 2n,
        documents: 20_000n,
        storageBytes: 108_000_000n,
        queries24h: 655_000n,
        writes24h: 900n,
      },
    }),
  ],
  agents: [
    create(admin.AgentSchema, {
      id: 'agt_docs_qa',
      name: 'docs-qa',
      description: 'Answers questions in the docs chat.',
      project: 'docs-search',
      owner: { kind: admin.SubjectKind.USER, id: 'usr_02', name: 'Aiko Tanaka' },
      status: admin.AgentStatus.ACTIVE,
      policy: [{ environment: 'production', actions: ['query'], collections: ['docs'] }],
      maxTokenTtlS: 600,
      limits: { requestsPerS: 200, concurrentQueries: 32 },
      lastActiveAt: at(2),
      activeTokens: 2,
    }),
    create(admin.AgentSchema, {
      id: 'agt_notes',
      name: 'release-notes-writer',
      project: 'docs-search',
      status: admin.AgentStatus.SUSPENDED,
      policy: [{ environment: 'development', actions: ['query', 'collections:write'] }],
      maxTokenTtlS: 900,
    }),
  ],
  keys: [
    create(admin.ApiKeySchema, {
      id: 'key_ci',
      name: 'ci-deploy',
      prefix: 'lk_01J9',
      environment: 'development',
      principal: { kind: admin.SubjectKind.SERVICE_ACCOUNT, id: 'sa_ci', name: 'ci' },
      scopes: ['query'],
      createdAt: at(60),
    }),
  ],
  events: [
    create(admin.AuditEventSchema, {
      id: 'evt_1',
      at: at(1),
      actor: { kind: admin.SubjectKind.AGENT, id: 'agt_docs_qa', name: 'docs-qa' },
      action: 'query.search',
      target: { kind: 'collection', id: 'docs', name: 'docs' },
      project: 'docs-search',
      environment: 'production',
      outcome: admin.AuditOutcome.SUCCESS,
    }),
    create(admin.AuditEventSchema, {
      id: 'evt_2',
      at: at(20),
      actor: { kind: admin.SubjectKind.AGENT, id: 'agt_claude', name: 'claude-code' },
      actingFor: { kind: admin.SubjectKind.USER, id: 'usr_02', name: 'Aiko Tanaka' },
      action: 'documents.delete',
      target: { kind: 'collection', id: 'code', name: 'code' },
      project: 'code-index',
      environment: 'production',
      outcome: admin.AuditOutcome.DENIED,
    }),
  ],
  teams: [
    create(admin.TeamSchema, {
      id: 'team_platform',
      name: 'Platform',
      slug: 'platform',
      description: 'Runs Loams.',
      memberCount: 2,
      idpGroup: 'acme-platform',
    }),
  ],
};

/** A console session for the seed's person, as `AuthService/GetSession` answers it. */
export function seedAccount(): ConsoleAccount {
  return create(auth.SessionSchema, {
    user: SEED.user,
    org: SEED.org,
    role: admin.OrgRole.OWNER,
    csrfToken: 'csrf_test',
  });
}

export interface AdminImpl {
  auth?: Partial<ServiceImpl<typeof auth.AuthService>>;
  org?: Partial<ServiceImpl<typeof admin.OrgService>>;
  projects?: Partial<ServiceImpl<typeof admin.ProjectService>>;
  agents?: Partial<ServiceImpl<typeof admin.AgentService>>;
  keys?: Partial<ServiceImpl<typeof admin.KeyService>>;
  audit?: Partial<ServiceImpl<typeof admin.AuditService>>;
  /** The engine's loams.collection.v1, for the environment page's collections. */
  collections?: Partial<ServiceImpl<typeof collection.CollectionService>>;
  /**
   * The engine's NamespaceService, for the org home's Namespaces tile. Absent, the
   * service is not served (an engine before ListNamespaces answers `unimplemented`).
   */
  namespaces?: Partial<ServiceImpl<typeof collection.NamespaceService>>;
}

function withStatus(id: string, status: admin.AgentStatus): admin.Agent {
  const agent = SEED.agents.find((a) => a.id === id) ?? create(admin.AgentSchema, { id });
  return create(admin.AgentSchema, { ...agent, status });
}

/** The seed over loams.auth.v1 and loams.admin.v1, with any method replaced. */
export function createAdminTransport(impl: AdminImpl = {}): Transport {
  const envsOf = (project: string) => SEED.environments.filter((e) => e.project === project);
  return createRouterTransport(({ service }) => {
    service(auth.AuthService, {
      listProviders: () => ({ password: true, totp: true, oidc: [] }),
      getSession: () => ({ session: seedAccount() }),
      createSession: () => ({ session: seedAccount() }),
      deleteSession: () => ({}),
      completeSetup: () => ({ session: seedAccount() }),
      decideConsent: () => ({ redirectTo: 'http://127.0.0.1:9999/cb?code=c&state=s' }),
      ...impl.auth,
    });
    service(admin.OrgService, {
      getOrg: () => ({ org: SEED.org }),
      updateOrg: (req) => ({ org: req.org }),
      listMembers: () => ({
        members: [{ user: SEED.user, role: admin.OrgRole.OWNER, teams: ['team_platform'] }],
      }),
      removeMember: () => ({}),
      listInvitations: () => ({
        invitations: [
          {
            id: 'inv_1',
            email: 'lena@acme.dev',
            role: admin.OrgRole.MEMBER,
            invitedBy: { name: 'Priya Raman' },
            expiresAt: at(-60 * 24),
          },
        ],
      }),
      createInvitation: (req) => ({
        invitation: { id: 'inv_2', email: req.email, role: req.role },
      }),
      listTeams: () => ({ teams: SEED.teams }),
      createTeam: (req) => ({ team: { id: 'team_new', name: req.name } }),
      getTeam: () => ({
        team: SEED.teams[0],
        members: [SEED.user],
        projects: [{ project: 'docs-search', role: admin.ProjectRole.ADMIN }],
      }),
      addTeamMember: () => ({}),
      ...impl.org,
    });
    service(admin.ProjectService, {
      listProjects: () => ({ projects: SEED.projects }),
      createProject: (req) => ({ project: { slug: req.slug, name: req.name } }),
      getProject: (req) => ({ project: SEED.projects.find((p) => p.slug === req.project) }),
      listAccess: () => ({
        grants: [
          {
            id: 'grant_1',
            subject: { kind: admin.SubjectKind.TEAM, id: 'team_platform', name: 'Platform' },
            role: admin.ProjectRole.ADMIN,
            createdAt: at(60 * 24 * 30),
          },
        ],
      }),
      grantAccess: (req) => ({ grant: { id: 'grant_2', subject: req.subject, role: req.role } }),
      listEnvironments: (req) => ({ environments: envsOf(req.project) }),
      createEnvironment: (req) => ({
        environment: { slug: req.slug, name: req.name, namespace: `${req.project}-${req.slug}` },
      }),
      getEnvironment: (req) => ({
        environment: envsOf(req.project).find((e) => e.slug === req.environment),
      }),
      getEnvironmentActivity: () => ({
        series: Array.from({ length: 30 }, (_, i) => ({
          date: new Date(Date.UTC(2026, 8, 14 + i)).toISOString().slice(0, 10),
          queries: BigInt(1000 + i * 10),
          writes: BigInt(40 + i),
        })),
      }),
      ...impl.projects,
    });
    service(admin.AgentService, {
      listAgents: (req) => ({ agents: SEED.agents.filter((a) => a.project === req.project) }),
      createAgent: (req) => ({ agent: { id: 'agt_new', name: req.name, project: req.project } }),
      getAgent: (req) => ({ agent: SEED.agents.find((a) => a.id === req.agent) }),
      updateAgent: (req) => ({ agent: req.agent }),
      suspendAgent: (req) => ({
        agent: withStatus(req.agent, admin.AgentStatus.SUSPENDED),
      }),
      resumeAgent: (req) => ({
        agent: withStatus(req.agent, admin.AgentStatus.ACTIVE),
      }),
      listAgentTokens: (req) => ({
        tokens: [
          {
            jti: 'tok_a1b0',
            agent: req.agent,
            environment: 'production',
            scopes: ['query'],
            flow: admin.TokenFlow.FEDERATION,
            subject: 'system:serviceaccount:docs:docs-qa',
            issuedAt: at(5),
            expiresAt: at(-5),
          },
        ],
      }),
      listTrustPolicies: () => ({
        trustPolicies: [
          {
            id: 'tp_1',
            name: 'Docs site on EKS',
            issuer: 'https://oidc.eks.us-east-1.amazonaws.com/id/7F3A2C',
            subjectPattern: 'system:serviceaccount:docs:docs-qa',
            environments: ['production'],
          },
        ],
      }),
      createTrustPolicy: (req) => ({ trustPolicy: { id: 'tp_2', name: req.name } }),
      listServiceAccounts: () => ({
        serviceAccounts: [{ id: 'sa_ci', name: 'ci', description: 'CI deploys', keyCount: 1 }],
      }),
      createServiceAccount: (req) => ({ serviceAccount: { id: 'sa_new', name: req.name } }),
      ...impl.agents,
    });
    service(admin.KeyService, {
      listKeys: () => ({ keys: SEED.keys }),
      createKey: (req) => ({
        key: { id: 'key_new', name: req.name, prefix: 'lk_NEW' },
        secret: 'lk_NEW_secret',
      }),
      revokeKey: () => ({}),
      revokeToken: () => ({}),
      ...impl.keys,
    });
    service(collection.CollectionService, {
      listCollections: (req) => ({
        collections: [
          {
            id: 1n,
            name: 'docs',
            namespace: req.namespace,
            schema: {
              fields: [{ name: 'title' }, { name: 'body' }],
              vectors: [{ name: 'embedding', dim: 1024 }],
            },
            liveDocCount: 368n,
            sizeBytes: 2_000_000n,
            manifestVersion: 46n,
          },
        ],
      }),
      ...impl.collections,
    });
    if (impl.namespaces) service(collection.NamespaceService, impl.namespaces);
    service(admin.AuditService, {
      listAuditEvents: () => ({ events: SEED.events }),
      async *watchAuditEvents() {
        yield { cursor: 'a1', event: { case: 'snapshot', value: { events: SEED.events } } };
      },
      ...impl.audit,
    });
  });
}

export type TestRouter = RouterService & {
  go(path: string, query?: Record<string, string>): void;
  trail: () => { label: string; href?: string }[] | undefined;
  navigated: string[];
};

/** A router that only records: the page under test is rendered directly. */
export function stubRouter(path = '/', query?: Record<string, string>): TestRouter {
  const listeners = new Set<() => void>();
  let location: Location = { path, params: {}, ...(query ? { query } : {}) };
  let trail: { label: string; href?: string }[] | undefined;
  const navigated: string[] = [];
  return {
    page: () => () => {},
    navigate(to) {
      navigated.push(to);
    },
    current: () => location,
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    setTrail(next) {
      trail = next;
    },
    trail: () => trail,
    navigated,
    go(next, q) {
      location = { path: next, params: {}, ...(q ? { query: q } : {}) };
      for (const l of listeners) l();
    },
  };
}

/** A session with the seed's person signed in (or the given gate). */
export function stubSession(
  options: { gate?: SessionGate; account?: ConsoleAccount | null } = {},
): SessionService & { refreshed: number; signedOut: number } {
  const account = options.account === null ? undefined : (options.account ?? seedAccount());
  const state = { refreshed: 0, signedOut: 0 };
  return {
    principal: () => undefined,
    environment: () => undefined,
    environments: () => [],
    select() {},
    authenticatedAt: () => undefined,
    subscribe: () => () => {},
    account: () => account,
    gate: () => options.gate ?? 'ready',
    async signOut() {
      state.signedOut++;
    },
    async refresh() {
      state.refreshed++;
    },
    get refreshed() {
      return state.refreshed;
    },
    get signedOut() {
      return state.signedOut;
    },
  };
}

/** The page under test with the console's query client, transport and toasts. */
export function withConsole(ui: ReactNode, transport: Transport = createAdminTransport()) {
  return (
    <ServerStateProvider client={createQueryClient()} transport={transport}>
      <ToastProvider>
        {ui}
        <Toaster />
      </ToastProvider>
    </ServerStateProvider>
  );
}
