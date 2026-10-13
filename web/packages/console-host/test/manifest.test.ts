// The manifest's `modes` field (SD1 Task 3, §53 §4.1, D901), and the JSON
// Schema kept in step with the validator.

import { describe, expect, it } from 'vitest';
import schema from '../plugin-manifest.schema.json';
import { MANIFEST_KEYS, ManifestError, PERMISSIONS, validateManifest } from '../src/index.js';

function pkg(plugin: Record<string, unknown>, name = '@loams/plugin-jobs') {
  return {
    name,
    version: '0.1.0',
    loams: { plugin: { kind: 'console', entry: '.', tier: 'first-party', ...plugin } },
  };
}

describe('manifest modes', () => {
  it('manifest_modes_default_console', () => {
    expect(validateManifest(pkg({})).modes).toEqual(['console']);
    expect(validateManifest(pkg({ modes: ['studio'] })).modes).toEqual(['studio']);
    expect(validateManifest(pkg({ modes: ['console', 'studio'] })).modes).toEqual([
      'console',
      'studio',
    ]);
  });

  it('manifest_modes_unknown_refused', () => {
    expect(() => validateManifest(pkg({ modes: ['admin'] }))).toThrow(ManifestError);
    expect(() => validateManifest(pkg({ modes: ['admin'] }))).toThrow(/unknown mode "admin"/);
    expect(() => validateManifest(pkg({ modes: [] }))).toThrow(/at least one mode/);
    expect(() => validateManifest(pkg({ modes: 'studio' }))).toThrow(/list of strings/);
    expect(() => validateManifest(pkg({ modes: ['studio', 'studio'] }))).toThrow(/twice/);
  });

  it('schema_and_validator_agree', () => {
    const props = schema.properties as Record<string, unknown>;
    // Every key the validator accepts is in the schema, and the other way round.
    expect([...MANIFEST_KEYS].sort()).toEqual(Object.keys(props).sort());
    expect(() => validateManifest(pkg({ notInSchema: 1 }))).toThrow(/unknown manifest key/);
    // modes: the same enum, unique, at least one.
    expect(props.modes).toEqual({
      type: 'array',
      items: { enum: ['console', 'studio'] },
      uniqueItems: true,
      minItems: 1,
      description: expect.any(String),
    });
    // permissions: the schema's enum is the validator's list.
    const permissions = props.permissions as { items: { enum: string[] } };
    expect(permissions.items.enum).toEqual([...PERMISSIONS]);
  });
});
