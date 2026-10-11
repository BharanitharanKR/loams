// Add a panel, or change one: pick a metric, a chart type and a title. The page mounts it
// per opening (keyed), so each opening starts from the panel or the defaults.

import { Button, Dialog, Field, Input, Select } from '@loams/ui';
import { useId, useState } from 'react';
import { GROUP_LABELS, METRICS, type MetricGroup, metricDef } from './catalog.js';
import { PANEL_TYPES, type Panel, type PanelType } from './spec.js';

export interface PanelChoice {
  metric: string;
  type: PanelType;
  title: string;
}

const GROUPS = Object.keys(GROUP_LABELS) as MetricGroup[];

export function PanelDialog({
  open,
  editing,
  defaultGroup,
  onClose,
  onSubmit,
}: {
  open: boolean;
  /** The panel being changed; none adds a new one. */
  editing?: Panel;
  /** The dashboard's own group, offered first when adding. */
  defaultGroup?: MetricGroup;
  onClose: () => void;
  onSubmit: (choice: PanelChoice) => void;
}) {
  const firstOfGroup = METRICS.find((m) => m.group === defaultGroup) ?? METRICS[0];
  const [metric, setMetric] = useState(editing?.metric ?? firstOfGroup?.id ?? '');
  const [type, setType] = useState<PanelType>(editing?.type ?? 'line');
  const [title, setTitle] = useState(editing?.title ?? '');
  const typeName = useId();

  const def = metricDef(metric);
  const submit = () => {
    if (!def) return;
    // A title that was only the old metric's label follows the metric.
    const stale =
      editing &&
      metric !== editing.metric &&
      title.trim() === metricDef(editing.metric)?.label;
    onSubmit({ metric, type, title: (!stale && title.trim()) || def.label });
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={editing ? `Edit ${editing.title}` : 'Add panel'}
      footer={
        <>
          <Button variant="quiet" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" onClick={submit} disabled={!def}>
            {editing ? 'Apply' : 'Add panel'}
          </Button>
        </>
      }
    >
      <form
        className="flex flex-col gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
      >
        <Field label="Metric" hint={def?.description}>
          {(p) => (
            <Select {...p} value={metric} onChange={(e) => setMetric(e.target.value)}>
              {GROUPS.map((g) => (
                <optgroup key={g} label={GROUP_LABELS[g]}>
                  {METRICS.filter((m) => m.group === g).map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.label} ({m.id})
                    </option>
                  ))}
                </optgroup>
              ))}
            </Select>
          )}
        </Field>
        <fieldset className="m-0 flex flex-col gap-1 border-0 p-0">
          <legend className="mb-1 text-sm">Chart type</legend>
          <div className="flex flex-wrap gap-3">
            {PANEL_TYPES.map((t) => (
              <label key={t.id} className="loams-check">
                <input
                  type="radio"
                  name={typeName}
                  value={t.id}
                  checked={type === t.id}
                  onChange={() => setType(t.id)}
                />
                {t.label}
              </label>
            ))}
          </div>
        </fieldset>
        <Field label="Title" hint={def ? `Leave empty for “${def.label}”.` : undefined}>
          {(p) => (
            <Input
              {...p}
              value={title}
              maxLength={120}
              placeholder={def?.label}
              onChange={(e) => setTitle(e.target.value)}
            />
          )}
        </Field>
        {/* Enter in the title submits. */}
        <button type="submit" hidden aria-hidden tabIndex={-1} />
      </form>
    </Dialog>
  );
}
