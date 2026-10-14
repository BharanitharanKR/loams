// Adding a project on the web (§53 §5.2): the dialog that asks for a git URL for
// `CloneRepo`. The sidebar that lists the projects with their threads is
// sidebar/Sidebar.tsx (SD1 Task 17); on the desktop Add opens the OS folder picker.

import { Button, Dialog, Field, Input } from '@loams/ui';
import { type FormEvent, useState } from 'react';
import { isCloneUrl } from '../agentd.js';

const URL_HINT = 'An https, ssh or user@host:path git URL. The daemon clones it on its host.';

export function CloneDialog({
  open,
  onClose,
  onClone,
  pending,
  failure,
}: {
  open: boolean;
  onClose: () => void;
  onClone: (url: string) => void;
  pending: boolean;
  failure?: string;
}) {
  const [url, setUrl] = useState('');
  const [invalid, setInvalid] = useState(false);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const value = url.trim();
    if (!isCloneUrl(value)) {
      setInvalid(true);
      return;
    }
    setInvalid(false);
    onClone(value);
  };
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Add a project"
      dismissible={!pending}
      footer={
        <>
          <Button variant="quiet" onClick={onClose} disabled={pending}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="studio-clone" disabled={pending}>
            {pending ? 'Cloning…' : 'Clone'}
          </Button>
        </>
      }
    >
      <form id="studio-clone" onSubmit={submit} noValidate>
        <Field
          label="Repository URL"
          hint={URL_HINT}
          error={invalid ? `Use ${URL_HINT.charAt(0).toLowerCase()}${URL_HINT.slice(1)}` : failure}
        >
          {(props) => (
            <Input
              {...props}
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://git.example.com/team/repo.git"
              autoComplete="off"
              spellCheck={false}
            />
          )}
        </Field>
      </form>
    </Dialog>
  );
}
