import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { LocalOnly } from '../src/index.js';

afterEach(cleanup);
// The empty state's grain canvas observes its size; jsdom has no ResizeObserver.
globalThis.ResizeObserver ??= class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

const desktopOn = (kind: string) =>
  ({
    servers: { list: async () => ({ servers: [{ id: 's', kind }], activeId: 's' }) },
  }) as unknown as Parameters<typeof LocalOnly>[0]['desktop'];

describe('LocalOnly', () => {
  it('shows the page on this computer', async () => {
    render(
      <LocalOnly desktop={desktopOn('local')}>
        <p>promises</p>
      </LocalOnly>,
    );
    expect(await screen.findByText('promises')).toBeTruthy();
  });

  it('explains, without loading, on another server', async () => {
    render(
      <LocalOnly desktop={desktopOn('demo')}>
        <p>promises</p>
      </LocalOnly>,
    );
    expect(await screen.findByText('Durable execution runs on this computer')).toBeTruthy();
    expect(screen.queryByText('promises')).toBeNull();
  });
});
