// Destructive confirmations (design 59, D1082): the dialog names the object and what will
// happen to it. Typed confirmation is required for production data: the person types a
// phrase (the branch or environment name) before the action unlocks. Part 3 applies the
// rule in each flow; this is the primitive.

import { type ReactNode, useEffect, useId, useState } from 'react';
import {
  AlertDialogClose,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogPopup,
  AlertDialogRoot,
  AlertDialogTitle,
} from './alert-dialog';
import { Button } from './button';
import { InputControl } from './input';

export interface ConfirmDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  /** What is acted on, as it reads after a verb: "the table orders". */
  object: string;
  /** What happens to it, in a sentence. */
  impact: ReactNode;
  /** The verb on the button ("Drop table"); never "OK". Its first word starts the title. */
  confirm: string;
  /** While true, the dialog cannot be dismissed and the button reports busy. */
  pending?: boolean;
}

export interface TypedConfirmDialogProps extends ConfirmDialogProps {
  /** The exact text to type, case-sensitive: the branch or environment name. */
  phrase: string;
}

function verbOf(confirm: string): string {
  return confirm.trim().split(/\s+/)[0] ?? confirm;
}

function Frame({
  open,
  onClose,
  object,
  impact,
  confirm,
  pending = false,
  children,
  disabled = false,
  onConfirm,
}: ConfirmDialogProps & { children?: ReactNode; disabled?: boolean }) {
  return (
    <AlertDialogRoot
      open={open}
      onOpenChange={(next) => {
        if (!next && !pending) onClose();
      }}
    >
      <AlertDialogPopup>
        <AlertDialogHeader>
          <AlertDialogTitle>{`${verbOf(confirm)} ${object}?`}</AlertDialogTitle>
          <AlertDialogDescription>{impact}</AlertDialogDescription>
          {children}
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogClose render={<Button variant="quiet" />} disabled={pending}>
            Cancel
          </AlertDialogClose>
          <Button
            variant="danger-solid"
            onClick={onConfirm}
            disabled={pending || disabled}
            aria-busy={pending || undefined}
          >
            {confirm}
          </Button>
        </AlertDialogFooter>
      </AlertDialogPopup>
    </AlertDialogRoot>
  );
}

/** "Drop the table orders?", what happens to it, Cancel and the danger verb. */
export function ConfirmDialog(props: ConfirmDialogProps) {
  return <Frame {...props} />;
}

/** A `ConfirmDialog` whose button unlocks only once `phrase` is typed exactly. */
export function TypedConfirmDialog({ phrase, ...props }: TypedConfirmDialogProps) {
  const [typed, setTyped] = useState('');
  const id = useId();
  const matches = typed === phrase;
  useEffect(() => {
    if (!props.open) setTyped('');
  }, [props.open]);
  return (
    <Frame {...props} disabled={!matches}>
      <label htmlFor={id} className="mt-2 text-muted text-small">
        Type <code className="font-mono text-ink">{phrase}</code> to confirm
      </label>
      <InputControl
        id={id}
        value={typed}
        onChange={(e) => setTyped(e.currentTarget.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && matches && !props.pending) props.onConfirm();
        }}
        autoComplete="off"
        autoCapitalize="off"
        spellCheck={false}
        font="mono"
      />
    </Frame>
  );
}
