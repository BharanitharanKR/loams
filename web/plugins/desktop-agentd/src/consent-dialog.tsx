import { Button, Dialog } from '@loams/ui';

/**
 * The first-run question of §50 §5.5. Neither choice is pre-selected or
 * focused first (Q705): the dialog's own Close button takes initial focus,
 * and closing it leaves the setting at `ask`.
 */
export function ConsentDialog({
  open,
  busy,
  onAnswer,
  onClose,
}: {
  open: boolean;
  busy: boolean;
  onAnswer: (v: 'on' | 'off') => void;
  onClose: () => void;
}) {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Keep agents running in the background?"
      footer={
        <div className="lc-actions" role="group" aria-label="Background mode">
          <Button disabled={busy} onClick={() => onAnswer('on')}>
            Keep running in the background
          </Button>
          <Button disabled={busy} onClick={() => onAnswer('off')}>
            Only while the app is open
          </Button>
        </div>
      }
    >
      <p>
        Agents and the local engine keep working after you close Loams Desktop, start when you log
        in, and restart if they crash.
      </p>
      <p className="lc-muted">You can change this later in Settings › Background agents.</p>
    </Dialog>
  );
}

/** The explicit stop (D785): running turns are drained before the daemon stops. */
export function StopDialog({
  open,
  busy,
  runningTurns,
  service,
  onConfirm,
  onClose,
}: {
  open: boolean;
  busy: boolean;
  runningTurns: number;
  service: boolean;
  onConfirm: () => void;
  onClose: () => void;
}) {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Stop background agents?"
      footer={
        <div className="lc-actions">
          <Button variant="quiet" disabled={busy} onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" disabled={busy} onClick={onConfirm}>
            {busy ? 'Stopping…' : 'Stop agents'}
          </Button>
        </div>
      }
    >
      <p>
        {runningTurns > 0
          ? `${runningTurns} running turn${runningTurns === 1 ? '' : 's'} will be checkpointed and resume when the agents start again.`
          : 'No turns are running.'}
      </p>
      {service && (
        <p className="lc-muted">
          The background service stays stopped until you log in again or start it here.
        </p>
      )}
    </Dialog>
  );
}
