// The blocking page for an engine this console does not support (design §54
// §13.1, D938): older than N−1, or without GetInstance. It names the engine's
// version, the versions the console supports and the command that upgrades
// the stack.

import { type Capabilities, UPGRADE_COMMAND } from '@loams/console-host';

export function EngineTooOld({ caps }: { caps: Capabilities }) {
  const found =
    caps.reason === 'no-instance'
      ? 'This engine does not answer GetInstance, so it is older than this console supports.'
      : `This engine runs Loams ${caps.version}.`;
  return (
    <main className="lc-bare" id="lc-main">
      <section className="lc-engine-too-old" role="alert" aria-labelledby="lc-engine-too-old-title">
        <h1 id="lc-engine-too-old-title">Upgrade your engine</h1>
        <p>{found}</p>
        <p>
          This console works with Loams {caps.minEngine} and later. Upgrade the stack from its
          directory, with a snapshot taken first:
        </p>
        <pre>
          <code>{UPGRADE_COMMAND}</code>
        </pre>
        <p>
          <code>loams upgrade check</code> shows what the upgrade will do first.
        </p>
      </section>
    </main>
  );
}
