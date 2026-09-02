import type { Page } from '@playwright/test'

import { mockAuthenticatedAppShell } from '../../support/auth'

const evaluationUser = {
  id: 'user-eval-1',
  email: 'eval@example.com',
  name: 'Eval User',
  role: 'read',
  permissions: [
    'config.read',
    'evaluation.read',
    'evaluation.run',
    'evaluation.write',
    'logs.read',
    'topology.read',
  ],
}

export async function mockEvaluationUserSession(page: Page): Promise<void> {
  await mockAuthenticatedAppShell(page, {
    user: evaluationUser,
    settings: { readonlyMode: false, serverReadonly: false },
  })
}
