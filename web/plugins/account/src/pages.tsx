import { useMutation, useQuery } from '@connectrpc/connect-query';
import { errorText, useAccount, useTrail } from '@loams/admin-ui';
import type { FlagsService, RouterService, SessionService } from '@loams/console-host';
import { auth } from '@loams/proto';
import { Badge, Button, buttonClass, Field, Input, Logo, Notice } from '@loams/ui';
import { KeyRound, ShieldCheck } from 'lucide-react';
import {
  type ChangeEvent,
  type FormEvent,
  type ReactNode,
  useState,
  useSyncExternalStore,
} from 'react';

function AuthFrame({
  title,
  children,
  foot,
}: {
  title: string;
  children: ReactNode;
  foot?: ReactNode;
}) {
  return (
    <div className="la">
      <div className="auth">
        <div className="auth-card">
          <Logo height={22} />
          <h1>{title}</h1>
          {children}
        </div>
        {foot && <div className="auth-foot">{foot}</div>}
      </div>
    </div>
  );
}

/** A same-console route to return to after sign-in; anything else is the home page. */
export function safeNext(next: string | undefined): string {
  return next?.startsWith('/') && !next.startsWith('//') ? next : '/';
}

function useQueryParams(router: RouterService): Record<string, string> {
  return useSyncExternalStore(router.subscribe, () => router.current()).query ?? {};
}

export function SignInPage({
  router,
  session,
  flags,
  server,
}: {
  router: RouterService;
  session: SessionService;
  flags: FlagsService;
  /** The engine's origin, for the OIDC start links (HTTP, D602). */
  server: string;
}) {
  useTrail(router, [{ label: 'Sign in' }]);
  const providers = useQuery(auth.AuthService.method.listProviders, {});
  const create = useMutation(auth.AuthService.method.createSession);
  const next = safeNext(useQueryParams(router).next);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [code, setCode] = useState('');
  const [error, setError] = useState<string>();
  const methods = providers.data;

  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    setError(undefined);
    try {
      await create.mutateAsync({ email, password, totpCode: code });
      // The session gate starts from the new cookie; only console routes are followed.
      await session.refresh?.();
      router.navigate(next);
    } catch (e) {
      setError(errorText(e));
    }
  };

  return (
    <AuthFrame
      title={`Sign in to ${flags.instanceName || 'Loams'}`}
      foot={
        <>
          New here? Ask an admin for an invitation. <Badge>{flags.serverVersion}</Badge>
        </>
      }
    >
      {(methods?.oidc ?? []).map((o) => (
        <a
          key={o.id}
          href={`${server}/api/v1/auth/oidc/${encodeURIComponent(o.id)}/start?next=${encodeURIComponent(next)}`}
          className={buttonClass({ variant: 'secondary', className: 'wide' })}
        >
          <ShieldCheck size={16} aria-hidden="true" /> Continue with {o.name}
        </a>
      ))}
      {methods?.password && (
        <>
          {methods.oidc.length > 0 && <div className="or">or with your email</div>}
          <form className="form" onSubmit={submit}>
            {error && (
              <Notice tone="danger" title="Couldn't sign you in">
                {error}
              </Notice>
            )}
            <Field label="Email">
              {(p) => (
                <Input
                  {...p}
                  type="email"
                  autoComplete="username"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                />
              )}
            </Field>
            <Field label="Password">
              {(p) => (
                <Input
                  {...p}
                  type="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
              )}
            </Field>
            {methods.totp && (
              <Field label="Two-factor code" hint="If your account uses one.">
                {(p) => (
                  <Input
                    {...p}
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    value={code}
                    onChange={(e) => setCode(e.target.value)}
                    maxLength={6}
                  />
                )}
              </Field>
            )}
            <Button variant="primary" type="submit" disabled={create.isPending} className="wide">
              Sign in
            </Button>
          </form>
        </>
      )}
      {providers.error && (
        <Notice tone="danger" title="The console can't reach its API">
          {errorText(providers.error)}
        </Notice>
      )}
    </AuthFrame>
  );
}

type SetupForm = {
  setupToken: string;
  orgName: string;
  name: string;
  email: string;
  password: string;
};

export function SetupPage({ router, session }: { router: RouterService; session: SessionService }) {
  useTrail(router, [{ label: 'Set up Loams' }]);
  const complete = useMutation(auth.AuthService.method.completeSetup);
  const [form, setForm] = useState<SetupForm>({
    setupToken: '',
    orgName: '',
    name: '',
    email: '',
    password: '',
  });
  const [error, setError] = useState<string>();
  const set = (k: keyof SetupForm) => (e: ChangeEvent<HTMLInputElement>) =>
    setForm({ ...form, [k]: e.target.value });
  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    if (form.password.length < 12) return setError('Use a password of at least 12 characters.');
    setError(undefined);
    try {
      await complete.mutateAsync(form);
      await session.refresh?.();
      router.navigate('/');
    } catch (e) {
      setError(errorText(e));
    }
  };
  return (
    <AuthFrame title="Set up this install">
      <p className="muted">
        When it started, the server wrote a one-time setup token to the file it named in its log,
        readable only by the server's user. The token lasts an hour, creates the organization and
        its owner, then stops working.
      </p>
      <form className="form" onSubmit={submit}>
        {error && (
          <Notice tone="danger" title="Setup failed">
            {error}
          </Notice>
        )}
        <Field label="Setup token">
          {(p) => (
            <Input
              {...p}
              value={form.setupToken}
              onChange={set('setupToken')}
              className={/* mono-ok: id */ 'loams-mono'}
              required
            />
          )}
        </Field>
        <Field label="Organization name">
          {(p) => (
            <Input
              {...p}
              value={form.orgName}
              onChange={set('orgName')}
              placeholder="Acme"
              required
            />
          )}
        </Field>
        <Field label="Your name">
          {(p) => <Input {...p} value={form.name} onChange={set('name')} required />}
        </Field>
        <Field label="Email">
          {(p) => <Input {...p} type="email" value={form.email} onChange={set('email')} required />}
        </Field>
        <Field
          label="Password"
          hint="12 characters or more. You can add single sign-on after setup."
        >
          {(p) => (
            <Input
              {...p}
              type="password"
              autoComplete="new-password"
              value={form.password}
              onChange={set('password')}
              required
            />
          )}
        </Field>
        <Button variant="primary" type="submit" className="wide" disabled={complete.isPending}>
          Create organization
        </Button>
      </form>
    </AuthFrame>
  );
}

/** The consent screen of user delegation (design §19 §5.2, flow 2). */
export function ConsentPage({
  router,
  session,
  navigate = (url) => globalThis.location.assign(url),
}: {
  router: RouterService;
  session: SessionService;
  /** Where the browser goes with the decision (the client's redirect). */
  navigate?: (url: string) => void;
}) {
  useTrail(router, [{ label: 'Allow access' }]);
  const account = useAccount(session);
  const params = useQueryParams(router);
  const decide = useMutation(auth.AuthService.method.decideConsent);
  const request = {
    clientId: params.client_id ?? '',
    redirectUri: params.redirect_uri ?? '',
    scope: params.scope ?? '',
    audience: params.audience ?? '',
    state: params.state ?? '',
    codeChallenge: params.code_challenge ?? '',
    codeChallengeMethod: 'S256',
  };
  const scopes = request.scope.split(' ').filter(Boolean);
  const complete =
    Boolean(request.clientId && request.redirectUri && request.audience && request.codeChallenge) &&
    params.code_challenge_method === 'S256';
  const [error, setError] = useState<string>();

  const choose = async (decision: auth.ConsentDecision) => {
    setError(undefined);
    try {
      const r = await decide.mutateAsync({ ...request, decision });
      navigate(r.redirectTo);
    } catch (e) {
      setError(errorText(e));
    }
  };

  if (!complete)
    return (
      <AuthFrame title="This link is incomplete">
        <Notice tone="danger" title="Missing authorization parameters">
          Start again from the app that sent you here. A consent request needs a client, a redirect,
          an environment and a PKCE S256 challenge.
        </Notice>
      </AuthFrame>
    );
  return (
    <AuthFrame title={`${request.clientId} wants to act as you`}>
      <div className="consent">
        <div>
          <span className="muted small">As</span>
          <span>
            {account?.user?.name} <span className="muted small">{account?.user?.email}</span>
          </span>
        </div>
        <div>
          <span className="muted small">In</span>
          <Badge>{request.audience}</Badge>
        </div>
        <div>
          <span className="muted small">It may</span>
          <span className="chips">
            {scopes.map((s) => (
              <Badge key={s}>{s}</Badge>
            ))}
          </span>
        </div>
        <p className="muted small">
          <KeyRound size={13} aria-hidden="true" /> Its token lasts at most the agent's lifetime
          cap, never exceeds your rights, and is logged under both of you. Revoke it from the
          agent's page.
        </p>
      </div>
      {error && (
        <Notice tone="danger" title="The decision didn't go through">
          {error}
        </Notice>
      )}
      <div className="consent-actions">
        <Button onClick={() => choose(auth.ConsentDecision.DENY)} disabled={decide.isPending}>
          Deny
        </Button>
        <Button
          variant="primary"
          onClick={() => choose(auth.ConsentDecision.ALLOW)}
          disabled={decide.isPending}
        >
          Allow
        </Button>
      </div>
    </AuthFrame>
  );
}
