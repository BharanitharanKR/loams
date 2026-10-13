// @loams/admin-ui: what the console's org, project, agent, audit and account
// pages share (API2 Task 10, AP1a Task 5). Plugins never import each other, so
// the parts the original console kept in src/page.tsx and src/parts live here,
// with the pages' styles (admin.css, tokens only).

import './admin.css';

export { ActivityChart } from './activity-chart.js';
export { AuditTable } from './audit.js';
export {
  ACTIONS,
  avatarKind,
  errorText,
  FLOW_LABEL,
  iso,
  num,
  ORG_ROLE,
  PROJECT_ROLE,
  slugify,
} from './model.js';
export {
  Form,
  LoadError,
  Loading,
  Page,
  PageHead,
  RouteLink,
  Scoped,
  useAccount,
  useNotify,
  useTrail,
} from './page.js';
