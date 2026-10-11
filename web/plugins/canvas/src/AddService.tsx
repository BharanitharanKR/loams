// The "add service" palette: pick a kind (filterable), name it, optionally connect it from
// an existing service, and create it through the client.

import { Button, Dialog, Field, Input, Notice, Select } from '@loams/ui';
import { type FormEvent, useId, useMemo, useState } from 'react';
import { errorText, type Service, type ServiceKind, validServiceName } from './client.js';
import { KIND_ORDER, kindInfo } from './kinds.js';

export interface AddServiceRequest {
  kind: ServiceKind;
  name: string;
  connectFrom?: string;
}

export function AddServiceDialog({
  open,
  services,
  onClose,
  onCreate,
}: {
  open: boolean;
  services: readonly Service[];
  onClose: () => void;
  /** Resolves when the service exists; a rejection's message is shown in the dialog. */
  onCreate: (req: AddServiceRequest) => Promise<void>;
}) {
  const formId = useId();
  const [filter, setFilter] = useState('');
  const [kind, setKind] = useState<ServiceKind>('postgres-branch');
  const [name, setName] = useState('');
  const [from, setFrom] = useState('');
  const [touched, setTouched] = useState(false);
  const [busy, setBusy] = useState(false);
  const [failure, setFailure] = useState<string>();

  const kinds = useMemo(() => {
    const q = filter.trim().toLowerCase();
    return KIND_ORDER.filter((k) => {
      if (!q) return true;
      const i = kindInfo(k);
      return `${i.label} ${i.product} ${i.blurb}`.toLowerCase().includes(q);
    });
  }, [filter]);
  const nameError = touched ? validServiceName(name) : undefined;

  const reset = () => {
    setFilter('');
    setName('');
    setFrom('');
    setTouched(false);
    setFailure(undefined);
  };
  const close = () => {
    if (busy) return;
    reset();
    onClose();
  };
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setTouched(true);
    if (validServiceName(name) || !kinds.includes(kind)) return;
    setBusy(true);
    setFailure(undefined);
    try {
      await onCreate({ kind, name: name.trim(), connectFrom: from || undefined });
      reset();
    } catch (err) {
      setFailure(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={close}
      title="Add a service"
      footer={
        <>
          <Button variant="quiet" onClick={close} disabled={busy}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form={formId} disabled={busy}>
            {busy ? 'Adding…' : 'Add service'}
          </Button>
        </>
      }
    >
      <form id={formId} onSubmit={submit} className="flex flex-col gap-4" noValidate>
        <Field label="Find a kind">
          {(p) => (
            <Input
              {...p}
              type="search"
              value={filter}
              placeholder="Postgres, connector, git…"
              onChange={(e) => setFilter(e.target.value)}
            />
          )}
        </Field>
        <fieldset className="m-0 border-0 p-0">
          <legend className="mb-2 text-sm">Kind</legend>
          {kinds.length === 0 ? (
            <p className="m-0 text-sm text-muted" role="status">
              No kind matches “{filter}”.
            </p>
          ) : (
            <div className="grid gap-2 sm:grid-cols-2">
              {kinds.map((k) => {
                const i = kindInfo(k);
                return (
                  <label
                    key={k}
                    className={[
                      'flex cursor-pointer items-start gap-2 border p-2 text-sm',
                      kind === k ? 'border-accent bg-accent-soft' : 'border-rule',
                    ].join(' ')}
                  >
                    <input
                      type="radio"
                      name={`${formId}-kind`}
                      value={k}
                      checked={kind === k}
                      onChange={() => setKind(k)}
                      className="mt-1"
                    />
                    <span>
                      <span className="block font-medium">{i.label}</span>
                      <span className="block text-xs text-muted">{i.blurb}</span>
                    </span>
                  </label>
                );
              })}
            </div>
          )}
        </fieldset>
        <Field label="Name" error={nameError} hint="Letters, digits, - and _.">
          {(p) => (
            <Input
              {...p}
              value={name}
              autoComplete="off"
              spellCheck={false}
              onChange={(e) => setName(e.target.value)}
              onBlur={() => setTouched(true)}
            />
          )}
        </Field>
        <Field label="Connect from (optional)">
          {(p) => (
            <Select {...p} value={from} onChange={(e) => setFrom(e.target.value)}>
              <option value="">Nothing</option>
              {services.map((s) => (
                <option key={s.id} value={s.id}>
                  {kindInfo(s.kind).label}: {s.name}
                </option>
              ))}
            </Select>
          )}
        </Field>
        {failure && (
          <Notice tone="danger" title="Could not add the service">
            {failure}
          </Notice>
        )}
      </form>
    </Dialog>
  );
}
