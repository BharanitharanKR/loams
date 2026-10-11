// The hosted console's screens before the console itself: sign-in, and the
// first-run "Connect your engine" screen.

import { Button, Field, Input, Logo, Notice } from '@loams/ui';
import { type FormEvent, type ReactNode, useEffect, useRef, useState } from 'react';
import { checkHealth, type Health, isTailnetHost, parseEngineUrl } from './engine.js';

export function PreviewBadge() {
  return (
    <span
      className="lc-preview"
      title="Loams is in preview: interfaces and data formats may change between releases."
    >
      Preview
    </span>
  );
}

function Frame({ title, children }: { title: string; children: ReactNode }) {
  return (
    <main className="lc-gate">
      <div className="lc-gate-card">
        <div className="lc-gate-brand">
          <Logo />
          <PreviewBadge />
        </div>
        <h1>{title}</h1>
        {children}
      </div>
    </main>
  );
}

export function SignInScreen({
  issuer,
  error,
  onSignIn,
}: {
  issuer: string;
  error?: string;
  onSignIn: () => Promise<void>;
}) {
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState<string>();
  const go = () => {
    setBusy(true);
    setFailed(undefined);
    onSignIn().catch((e: unknown) => {
      setBusy(false);
      setFailed(e instanceof Error ? e.message : String(e));
    });
  };
  const problem = failed ?? error;
  return (
    <Frame title="Sign in to Loams">
      <p className="lc-gate-lead">
        The console runs in your browser and talks to your own Loams engine. Sign in with your Loams
        account; the engine checks the token it is given.
      </p>
      {problem && (
        <Notice tone="danger" title="Sign-in did not complete">
          {problem}
        </Notice>
      )}
      <div className="lc-gate-actions">
        <Button variant="primary" onClick={go} disabled={busy}>
          {busy ? 'Opening sign-in…' : 'Sign in'}
        </Button>
        <span className="lc-gate-faint">via {new URL(issuer).host}</span>
      </div>
    </Frame>
  );
}

export interface ConnectResult {
  engine: string;
  /** An API key for an engine without OIDC (LOAMS_AUTH_MODE=apikey); held in memory only. */
  apiKey?: string;
}

export function ConnectScreen({
  initial,
  fixed = false,
  askApiKey,
  check = checkHealth,
  onConnect,
}: {
  /** A remembered (or fixed) engine: prefilled and tested at once. */
  initial?: string;
  /** The deploy fixes the engine (LOAMS_ENGINE_URL): the URL cannot be changed. */
  fixed?: boolean;
  /** No OIDC issuer is configured: offer an API key field. */
  askApiKey: boolean;
  check?: (origin: string) => Promise<Health>;
  onConnect: (result: ConnectResult) => void;
}) {
  const [url, setUrl] = useState(initial ?? '');
  const [apiKey, setApiKey] = useState('');
  const [problem, setProblem] = useState<string>();
  const [testing, setTesting] = useState(false);
  const [health, setHealth] = useState<{ origin: string; result: Health }>();

  const parsed = parseEngineUrl(url);
  const tested = health && parsed.ok && health.origin === parsed.origin ? health.result : undefined;

  const test = async (e?: FormEvent) => {
    e?.preventDefault();
    if (!parsed.ok) {
      setProblem(parsed.problem);
      return;
    }
    setProblem(undefined);
    setTesting(true);
    const result = await check(parsed.origin);
    setTesting(false);
    setHealth({ origin: parsed.origin, result });
  };

  // Test a remembered engine straight away, once.
  const autoTested = useRef(false);
  useEffect(() => {
    if (!initial || autoTested.current) return;
    autoTested.current = true;
    void test();
  });

  const connect = () => {
    if (!parsed.ok) return;
    onConnect({ engine: parsed.origin, apiKey: apiKey.trim() || undefined });
  };

  return (
    <Frame title="Connect your engine">
      <p className="lc-gate-lead">
        Your Loams engine runs on your machine or your own VMs, never on the public internet. This
        browser reaches it over your tailnet, so this device needs Tailscale running and logged in
        to the same tailnet as the engine.
      </p>
      <form className="lc-gate-form" onSubmit={test}>
        <Field
          label="Engine URL"
          hint="The engine's tailnet address, for example https://loams.your-tailnet.ts.net"
          error={problem}
        >
          {(props) => (
            <Input
              {...props}
              // Text, not url: a bare tailnet name (no scheme) is fine here.
              type="text"
              inputMode="url"
              autoComplete="url"
              spellCheck={false}
              placeholder="https://loams.your-tailnet.ts.net"
              value={url}
              readOnly={fixed}
              onChange={(e) => setUrl(e.currentTarget.value)}
            />
          )}
        </Field>
        {askApiKey && (
          <Field
            label="API key (optional)"
            hint="For an engine that uses API keys. Kept in this tab's memory only."
          >
            {(props) => (
              <Input
                {...props}
                type="password"
                autoComplete="off"
                value={apiKey}
                onChange={(e) => setApiKey(e.currentTarget.value)}
              />
            )}
          </Field>
        )}
        <div className="lc-gate-actions">
          <Button type="submit" disabled={testing || url.trim() === ''}>
            {testing ? 'Testing…' : 'Test connection'}
          </Button>
          <Button variant="primary" onClick={connect} disabled={!tested?.ok}>
            Connect
          </Button>
          {tested && !tested.ok && (
            <Button variant="quiet" onClick={connect}>
              Connect anyway
            </Button>
          )}
        </div>
      </form>
      {tested?.ok && (
        <Notice tone="success" title="The engine answered">
          /health returned {tested.status} in {tested.ms} ms.
        </Notice>
      )}
      {tested && !tested.ok && parsed.ok && (
        <Notice tone="danger" title="This browser cannot reach the engine">
          {tested.message} <Unreachable origin={parsed.origin} result={tested} />
        </Notice>
      )}
      <details className="lc-gate-help">
        <summary>How the engine is reached</summary>
        <ul>
          <li>
            The release Compose file (<code>deploy/release/compose.yaml</code>) runs the engine with
            a <code>tailscale</code> sidecar that joins your tailnet as <code>loams</code> and
            serves the engine over HTTPS with <code>tailscale serve</code>. Nothing listens on the
            host's public interfaces.
          </li>
          <li>
            Tailscale: install the client on this device, log in to the same tailnet, and use the
            MagicDNS name, <code>https://loams.&lt;tailnet&gt;.ts.net</code>.
          </li>
          <li>
            Headscale: start Compose with the <code>headscale</code> profile (or point{' '}
            <code>TS_LOGIN_SERVER</code> at yours), then run{' '}
            <code>tailscale up --login-server https://your-headscale</code> on this device.
            Headscale issues no HTTPS certificates, so put a certificate in front of the engine, or
            deploy this console yourself on the tailnet, and add the engine's address to the
            console's <code>LOAMS_ENGINE_URL</code>.
          </li>
          <li>
            The engine must allow this console's origin (CORS) and accept your sign-in: see{' '}
            <code>LOAMS_AUTH_MODE</code> in the Compose <code>.env</code>.
          </li>
        </ul>
      </details>
    </Frame>
  );
}

function Unreachable({ origin, result }: { origin: string; result: Health }) {
  if (result.ok) return null;
  if (result.kind === 'http') return <>The engine is up but not healthy yet; try again shortly.</>;
  if (isTailnetHost(origin))
    return (
      <>
        Check that Tailscale is running on this device and logged in to the engine's tailnet, that
        the engine is up (<code>docker compose ps</code>), and that it allows this console's origin.
      </>
    );
  return (
    <>
      Check that this device can reach that address (VPN or tailnet up), that the engine is running,
      and that it allows this console's origin.
    </>
  );
}

export function BootError({ message }: { message: string }) {
  return (
    <Frame title="The console could not start">
      <Notice tone="danger" title="Error">
        {message}
      </Notice>
    </Frame>
  );
}
