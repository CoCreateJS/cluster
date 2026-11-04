import fs from 'node:fs/promises';
import path from 'node:path';

const ORGANIZATION_SEGMENT = /^[A-Za-z0-9_-]+$/;

export function createWorkspaceResolver(baseDirectory, workspaceDirectory = 'sites') {
  const root = path.resolve(baseDirectory);

  if (!ORGANIZATION_SEGMENT.test(workspaceDirectory) && workspaceDirectory !== '.ai') {
    throw new Error(`Invalid organization workspace directory: ${workspaceDirectory}`);
  }

  const assertContained = (workspace, target, filePath) => {
    if (target !== workspace && !target.startsWith(`${workspace}${path.sep}`)) {
      throw new Error(`Local file path escapes the organization workspace: ${filePath}`);
    }
  };

  const organizationRoot = (organization_id) => {
    if (!organization_id || !ORGANIZATION_SEGMENT.test(organization_id)) {
      throw new Error('A valid authenticated organization_id is required for local workspace access.');
    }
    return path.join(root, organization_id, workspaceDirectory);
  };

  const ensure = async (organization_id) => {
    const organizationDirectory = path.join(root, organization_id);
    const workspace = organizationRoot(organization_id);
    await fs.mkdir(root, { recursive: true, mode: 0o700 });
    await fs.mkdir(organizationDirectory, { recursive: true, mode: 0o700 });
    await fs.mkdir(workspace, { recursive: true, mode: 0o700 });
    const realRoot = await fs.realpath(root);
    const realOrganizationDirectory = await fs.realpath(organizationDirectory);
    const realWorkspace = await fs.realpath(workspace);
    assertContained(realRoot, realOrganizationDirectory, organization_id);
    assertContained(realOrganizationDirectory, realWorkspace, workspaceDirectory);
    return workspace;
  };

  return { organizationRoot, ensure };
}
