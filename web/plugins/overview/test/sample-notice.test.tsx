import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import { LiveDashboard } from '../src/live/live-dashboard.js';
import { mockOverviewMetrics } from '../src/live/mock.js';

afterEach(cleanup);

it('the live dashboard says its fixture data is a sample, with the shared notice and badge', async () => {
  const { container } = render(<LiveDashboard client={mockOverviewMetrics()} />);
  await screen.findAllByText('Sample data');
  expect(container.querySelector('.loams-sample-data')).not.toBeNull();
  expect(container.querySelector('.loams-sample-data-badge')).not.toBeNull();
});
