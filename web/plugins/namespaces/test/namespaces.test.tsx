import { create } from '@bufbuild/protobuf';
import type { SessionService } from '@loams/console-host';
import { instance } from '@loams/proto';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { NamespacesPage } from '../src/index.js';

afterEach(cleanup);

describe('NamespacesPage', () => {
  it('lists environments with their namespaces and switches the selection', () => {
    // A fake session (plugins never import each other's values).
    const listeners = new Set<() => void>();
    const environments = [
      { id: 'env_dev', name: 'development', namespace: 'search-dev', protected: false },
      { id: 'env_prod', name: 'production', namespace: 'search-prod', protected: true },
    ];
    let selected = 'env_dev';
    const session: SessionService = {
      principal: () => create(instance.PrincipalSchema, { id: 'usr_omar', displayName: 'Omar' }),
      environments: () => environments,
      environment: () => environments.find((e) => e.id === selected),
      select: (id) => {
        selected = id;
        for (const l of listeners) l();
      },
      authenticatedAt: () => undefined,
      subscribe: (l) => {
        listeners.add(l);
        return () => listeners.delete(l);
      },
    };
    render(<NamespacesPage session={session} />);
    expect(screen.getByText('Signed in as Omar')).toBeTruthy();
    expect(screen.getByText('search-prod')).toBeTruthy();
    expect(screen.getByText('protected')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Select' }));
    expect(session.environment()?.id).toBe('env_prod');
  });

  it('with sign-in off and no environments, offers the namespaces to open (B7)', () => {
    localStorage.setItem('loams.data.namespaces', JSON.stringify(['e2e']));
    const session: SessionService = {
      principal: () =>
        create(instance.PrincipalSchema, { id: 'dev-loopback', displayName: 'Local developer' }),
      environments: () => [],
      environment: () => undefined,
      select: () => {},
      authenticatedAt: () => undefined,
      subscribe: () => () => {},
    };
    const went: string[] = [];
    render(<NamespacesPage session={session} navigate={(to) => went.push(to)} />);
    expect(screen.getByText(/runs with sign-in off/)).toBeTruthy();
    expect(screen.queryByText(/cannot reach any environment/)).toBeNull();
    expect(screen.getByText('e2e')).toBeTruthy();
    expect(screen.getAllByRole('row')).toHaveLength(3); // the header, default and e2e
    fireEvent.click(screen.getAllByRole('button', { name: 'Data explorer' })[1] as HTMLElement);
    fireEvent.click(screen.getAllByRole('button', { name: 'Streams & Links' })[0] as HTMLElement);
    expect(went).toEqual(['/data/e2e', '/streams/default']);
    localStorage.clear();
  });
});
