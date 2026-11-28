// API Client for kubeadp

const API_BASE = '/api/v1';

export interface AgentReference {
  name: string;
}

export interface Condition {
  type: string;
  status: string;
  reason?: string;
  message?: string;
}

export interface TokenUsage {
  input?: number;
  output?: number;
  reasoning?: number;
  cache?: number;
}

export interface SessionSummary {
  messageCount?: number;
  tokenUsage?: TokenUsage;
  cost?: string;
  filesChanged?: number;
  additions?: number;
  deletions?: number;
}

export interface SessionInfo {
  id?: string;
  title?: string;
  summary?: SessionSummary;
}

export interface DiffStat {
  files?: number;
  additions?: number;
  deletions?: number;
}

export interface TodoItem {
  id?: string;
  content: string;
  status: string;
  activeForm?: string;
}

export interface TaskAction {
  type: string;
  by?: string;
  time?: string;
  result?: string;
}

export interface TaskJobStatus {
  id?: string;
  status?: string;
  engine?: string;
  model?: string;
  elapsedSeconds?: number;
  conversationId?: string;
  branch?: string;
  worktree?: string;
  commit?: string;
  diff?: DiffStat;
  todo?: TodoItem[];
  pendingPermissions?: number;
  lastAction?: TaskAction;
}

export interface Task {
  name: string;
  namespace: string;
  phase: string;
  description?: string;
  agentRef?: AgentReference;
  templateRef?: AgentReference;
  timeout?: string;
  podName?: string;
  session?: SessionInfo;
  job?: TaskJobStatus;
  startTime?: string;
  completionTime?: string;
  duration?: string;
  createdAt: string;
  conditions?: Condition[];
  labels?: Record<string, string>;
}

export interface Pagination {
  limit: number;
  offset: number;
  totalCount: number;
  hasMore: boolean;
}

export interface TaskListResponse {
  tasks: Task[];
  total: number;
  pagination?: Pagination;
}

export interface FilterParams {
  name?: string;
  labelSelector?: string;
  limit?: number;
  offset?: number;
  sortOrder?: 'asc' | 'desc';
}

export interface ListTasksParams extends FilterParams {
  phase?: string;
}

export interface CreateTaskRequest {
  name?: string;
  description?: string;
  agentRef?: AgentReference;
  templateRef?: { name: string };
  timeout?: string;
  project?: string;
  gitRef?: string;
  model?: string;
  effort?: string;
  sandbox?: boolean;
  addDirs?: string[];
  plan?: boolean;
  printTimeout?: string;
  mode?: string;
}

export interface CreateVolumePersistence {
  storageClassName?: string;
  size?: string;
}

export interface CreatePersistenceConfig {
  sessions?: CreateVolumePersistence;
  workspace?: CreateVolumePersistence;
}

export interface AssemblySkillInput {
  name: string;
  git?: {
    repository: string;
    ref?: string;
    path?: string;
    names?: string[];
  };
}

export interface AssemblyPluginInput {
  name: string;
  target?: string;
}

export interface CreateAgentTemplateRequest {
  name: string;
  engine?: 'opencode' | 'agy' | 'claude';
  worker?: WorkerConfigInput;
  workspaceDir?: string;
  serviceAccountName?: string;
  agentImage?: string;
  kubeadpImage?: string;
  executorImage?: string;
  skills?: AssemblySkillInput[];
  plugins?: AssemblyPluginInput[];
}

export interface WorkerCapacityStatus {
  maxParallel?: number;
  running?: number;
}

export interface WorkerDiskStatus {
  totalBytes?: number;
  freeBytes?: number;
}

export interface WorkerEngineAuthState {
  engine: string;
  state: string;
}

export interface WorkerAuthStatus {
  state?: string;
  engines?: WorkerEngineAuthState[];
}

export interface WorkerCapability {
  engine: string;
  transport?: string;
  endpoints?: Record<string, string>;
}

export interface WorkerQuotaStatus {
  group: string;
  window?: string;
  remainingPercent?: number;
  resetsAt?: string;
}

export interface WorkerProjectStatus {
  name: string;
  branch?: string;
  head?: string;
  lastSync?: string;
  jobs?: number;
}

export interface WorkerRemoteControlStatus {
  enabled?: boolean;
  state?: string;
}

export interface WorkerStatus {
  version?: string;
  ready?: boolean;
  lastSyncTime?: string;
  capacity?: WorkerCapacityStatus;
  disk?: WorkerDiskStatus;
  auth?: WorkerAuthStatus;
  capabilities?: WorkerCapability[];
  models?: string[];
  quota?: WorkerQuotaStatus[];
  projects?: WorkerProjectStatus[];
  remoteControl?: WorkerRemoteControlStatus;
}

export interface WorkerConfigInput {
  maxParallel?: number;
  gcMaxAgeDays?: number;
  stallMinutes?: number;
  launchTimeoutSecs?: number;
  shutdownGraceSecs?: number;
  defaultModel?: string;
  defaultEffort?: string;
  defaultBaseBranch?: string;
  defaultEngine?: 'opencode' | 'agy' | 'claude';
  maxRuntime?: string;
  jobsDirName?: string;
  reposRoot?: string;
  sandbox?: {
    roPaths?: string[];
    envAllow?: string[];
  };
}

export interface CreateAgentRequest {
  name: string;
  profile?: string;
  engine?: 'opencode' | 'agy' | 'claude';
  worker?: WorkerConfigInput;
  templateRef?: AgentReference;
  workspaceDir?: string;
  serviceAccountName?: string;
  // P0: Images
  agentImage?: string;
  kubeadpImage?: string;
  executorImage?: string;
  // P1: Common configuration
  maxConcurrentTasks?: number;
  standby?: StandbyInfo;
  persistence?: CreatePersistenceConfig;
  // P2: Advanced
  port?: number;
  proxy?: ProxyConfigInfo;
  // Assembly: assets selected from a Registry catalog
  skills?: AssemblySkillInput[];
  plugins?: AssemblyPluginInput[];
}

export interface StandbyInfo {
  idleTimeout: string;
}

export interface ContextItem {
  name?: string;
  description?: string;
  type: string;
  mountPath?: string;
  sync?: GitSyncConfig;
}

export interface GitSyncConfig {
  enabled: boolean;
  interval?: string;
  policy?: string;
}

export interface CredentialInfo {
  name: string;
  secretRef: string;
  mountPath?: string;
  env?: string;
}

export interface QuotaInfo {
  maxTaskStarts?: number;
  windowSeconds?: number;
}

export interface GitSyncStatusInfo {
  name: string;
  commitHash?: string;
  lastSynced?: string;
}

export interface ServerStatusInfo {
  deploymentName?: string;
  serviceName?: string;
  url?: string;
  ready: boolean;
  port?: number;
  suspended: boolean;
  idleSince?: string;
  gitSyncStatuses?: GitSyncStatusInfo[];
}

export interface SkillGitInfo {
  repository: string;
  ref?: string;
  path?: string;
  names?: string[];
}

export interface SkillInfo {
  name: string;
  git?: SkillGitInfo;
}

export interface PluginInfo {
  name: string;
  target?: string;
  options?: Record<string, unknown>;
}

export interface ShareStatusInfo {
  enabled: boolean;
  active: boolean;
  secretName?: string;
  url?: string;
  expiresAt?: string;
  allowedIPs?: string[];
}

export interface ShareTokenResponse {
  enabled: boolean;
  active: boolean;
  token?: string;
  path?: string;
  expiresAt?: string;
  allowedIPs?: string[];
}

export interface UpdateShareRequest {
  enabled: boolean;
  expiresIn?: string;
  allowedIPs?: string[];
}

export interface CapabilitiesResponse {
  engine: string;
  endpoints: Record<string, 'native' | 'emulated' | 'unsupported'>;
}

export interface ModelEntry {
  id: string;
  name?: string;
  provider?: string;
}

export interface QuotaEntry {
  limit?: number;
  remaining?: number;
  reset?: string;
}

export interface QuotaResponse {
  groups?: Record<string, QuotaEntry>;
}

export interface ProjectEntry {
  id: string;
  name: string;
  path?: string;
}

export interface WorkerJob {
  id: string;
  project?: string;
  engine?: string;
  status: string;
  elapsed_seconds?: number;
  task_name?: string;
}

export interface AuthStatusResponse {
  authenticated: boolean;
  user?: string;
  engines?: Record<string, string>;
}

export interface RemoteControlStatusResponse {
  enabled: boolean;
}

export interface GCRequest {
  dry_run?: boolean;
}

export interface GCResult {
  removed_jobs?: number;
  freed_bytes?: number;
}

export interface Agent {
  name: string;
  namespace: string;
  profile?: string;
  engine?: 'opencode' | 'agy' | 'claude';
  worker?: WorkerConfigInput;
  templateRef?: AgentReference;
  executorImage?: string;
  kubeadpImage?: string;
  agentImage?: string;
  workspaceDir?: string;
  serviceAccountName?: string;
  contextsCount: number;
  credentialsCount: number;
  skillsCount: number;
  pluginsCount: number;
  maxConcurrentTasks?: number;
  quota?: QuotaInfo;
  credentials?: CredentialInfo[];
  contexts?: ContextItem[];
  skills?: SkillInfo[];
  plugins?: PluginInfo[];
  config?: Record<string, unknown>;
  createdAt: string;
  labels?: Record<string, string>;
  standby?: StandbyInfo;
  share?: ShareStatusInfo;
  conditions?: Condition[];
  serverStatus?: ServerStatusInfo;
  workerStatus?: WorkerStatus;
}

export interface AgentTemplate {
  name: string;
  namespace: string;
  engine?: 'opencode' | 'agy' | 'claude';
  worker?: WorkerConfigInput;
  agentImage?: string;
  kubeadpImage?: string;
  executorImage?: string;
  workspaceDir?: string;
  serviceAccountName?: string;
  contextsCount: number;
  credentialsCount: number;
  skillsCount: number;
  pluginsCount: number;
  credentials?: CredentialInfo[];
  contexts?: ContextItem[];
  skills?: SkillInfo[];
  plugins?: PluginInfo[];
  config?: Record<string, unknown>;
  createdAt: string;
  labels?: Record<string, string>;
  conditions?: Condition[];
  agentCount: number;
}

export interface AgentTemplateListResponse {
  templates: AgentTemplate[];
  total: number;
  pagination?: Pagination;
}

export interface AgentListResponse {
  agents: Agent[];
  total: number;
  pagination?: Pagination;
}

export interface SystemImageConfig {
  image?: string;
  imagePullPolicy?: string;
}

export interface CleanupConfig {
  ttlSecondsAfterFinished?: number;
  maxRetainedTasks?: number;
}

export interface ProxyConfigInfo {
  httpProxy?: string;
  httpsProxy?: string;
  noProxy?: string;
}

export interface ConfigResponse {
  name: string;
  createdAt: string;
  systemImage?: SystemImageConfig;
  cleanup?: CleanupConfig;
  proxy?: ProxyConfigInfo;
  labels?: Record<string, string>;
}

// CronTask types
export interface CronTaskTemplateInfo {
  description?: string;
  agentRef?: AgentReference;
  templateRef?: AgentReference;
}

export interface CronTask {
  name: string;
  namespace: string;
  schedule: string;
  timeZone?: string;
  concurrencyPolicy: string;
  suspend: boolean;
  maxRetainedTasks?: number;
  startingDeadlineSeconds?: number;
  active: number;
  lastScheduleTime?: string;
  lastSuccessfulTime?: string;
  nextScheduleTime?: string;
  totalExecutions: number;
  taskTemplate: CronTaskTemplateInfo;
  createdAt: string;
  labels?: Record<string, string>;
  conditions?: Condition[];
}

export interface CronTaskListResponse {
  cronTasks: CronTask[];
  total: number;
  pagination?: Pagination;
}

export interface CreateCronTaskRequest {
  name?: string;
  schedule: string;
  timeZone?: string;
  concurrencyPolicy?: string;
  suspend?: boolean;
  startingDeadlineSeconds?: number;
  maxRetainedTasks?: number;
  description?: string;
  agentRef?: AgentReference;
  templateRef?: { name: string };
  contexts?: ContextItem[];
}

export interface ServerInfo {
  version: string;
}

export interface NamespaceList {
  namespaces: string[];
}

// Log streaming event types
export interface LogEvent {
  type: 'status' | 'log' | 'error' | 'info' | 'complete';
  phase?: string;
  podPhase?: string;
  content?: string;
  message?: string;
}

// Registry types
export interface RegistryImageMetadata {
  description?: string;
  category?: string;
  tags?: string[];
  tools?: string[];
  baseImage?: string;
  maintainer?: string;
}

export interface RegistryImageInfo {
  name: string;
  image: string;
  phase: string;
  digest?: string;
  lastChecked?: string;
  message?: string;
  metadata?: RegistryImageMetadata;
}

export interface RegistrySkillInfo {
  name: string;
  repository?: string;
  ref?: string;
  path?: string;
  names?: string[];
  phase: string;
  latestCommit?: string;
  lastChecked?: string;
  message?: string;
  description?: string;
  tags?: string[];
}

export interface RegistryPluginInfo {
  name: string;
  package: string;
  target?: string;
  phase: string;
  resolvedVersion?: string;
  lastChecked?: string;
  message?: string;
  description?: string;
  tags?: string[];
}

export interface RegistrySummary {
  images: number;
  skills: number;
  plugins: number;
  readyCount: number;
  totalCount: number;
}

export interface Registry {
  name: string;
  namespace: string;
  images?: RegistryImageInfo[];
  skills?: RegistrySkillInfo[];
  plugins?: RegistryPluginInfo[];
  summary: RegistrySummary;
  createdAt: string;
  labels?: Record<string, string>;
  conditions?: Condition[];
}

export interface RegistryListResponse {
  registries: Registry[];
  total: number;
  pagination?: Pagination;
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Unknown error' }));
    throw new Error(error.message || error.error || `HTTP ${response.status}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json();
}

export const api = {
  // Info
  getInfo: () => request<ServerInfo>('/info'),
  getNamespaces: () => request<NamespaceList>('/namespaces'),

  // Tasks
  listAllTasks: (params?: ListTasksParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.phase) searchParams.set('phase', params.phase);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<TaskListResponse>(`/tasks${queryString ? `?${queryString}` : ''}`);
  },

  listTasks: (namespace: string, params?: ListTasksParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.phase) searchParams.set('phase', params.phase);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<TaskListResponse>(`/namespaces/${namespace}/tasks${queryString ? `?${queryString}` : ''}`);
  },

  getTask: (namespace: string, name: string) =>
    request<Task>(`/namespaces/${namespace}/tasks/${name}`),

  getTaskYaml: async (namespace: string, name: string): Promise<string> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/tasks/${name}?output=yaml`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.text();
  },

  createTask: (namespace: string, task: CreateTaskRequest) =>
    request<Task>(`/namespaces/${namespace}/tasks`, {
      method: 'POST',
      body: JSON.stringify(task),
    }),

  deleteTask: (namespace: string, name: string) =>
    request<void>(`/namespaces/${namespace}/tasks/${name}`, {
      method: 'DELETE',
    }),

  stopTask: (namespace: string, name: string) =>
    request<Task>(`/namespaces/${namespace}/tasks/${name}/stop`, {
      method: 'POST',
    }),

  // Task Actions
  retryTask: (namespace: string, name: string, prompt?: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/retry`, {
      method: 'POST',
      body: JSON.stringify({ prompt }),
    }),

  killTask: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/kill`, {
      method: 'POST',
    }),

  commitTask: (namespace: string, name: string, message: string, only?: string[], exclude?: string[]) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/commit`, {
      method: 'POST',
      body: JSON.stringify({ message, only, exclude }),
    }),

  discardTask: (namespace: string, name: string, files: string[]) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/discard`, {
      method: 'POST',
      body: JSON.stringify({ files }),
    }),

  cleanTask: (namespace: string, name: string, force?: boolean, purgeLog?: boolean) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/clean`, {
      method: 'POST',
      body: JSON.stringify({ force, purge_log: purgeLog }),
    }),

  reviewTask: (namespace: string, name: string, reviewerModel?: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/review`, {
      method: 'POST',
      body: JSON.stringify({ reviewer_model: reviewerModel }),
    }),

  messageTask: (namespace: string, name: string, text: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/message`, {
      method: 'POST',
      body: JSON.stringify({ text }),
    }),

  interruptTask: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/interrupt`, {
      method: 'POST',
    }),

  pauseTask: (namespace: string, name: string, paused: boolean) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/pause`, {
      method: 'POST',
      body: JSON.stringify({ paused }),
    }),

  compactTask: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/compact`, {
      method: 'POST',
    }),

  forkTask: (namespace: string, name: string) =>
    request<{ status: string; job_id: string }>(`/namespaces/${namespace}/tasks/${name}/fork`, {
      method: 'POST',
    }),

  revertTask: (namespace: string, name: string, commit: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/tasks/${name}/revert`, {
      method: 'POST',
      body: JSON.stringify({ commit }),
    }),

  getTaskDiff: async (namespace: string, name: string, stat?: boolean): Promise<string> => {
    const res = await fetch(`${API_BASE}/namespaces/${namespace}/tasks/${name}/diff${stat ? '?stat=true' : ''}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.text();
  },

  getTaskFiles: (namespace: string, name: string, path?: string, depth?: number) => {
    const p = new URLSearchParams();
    if (path) p.set('path', path);
    if (depth) p.set('depth', depth.toString());
    const q = p.toString();
    return request<string[]>(`/namespaces/${namespace}/tasks/${name}/files${q ? `?${q}` : ''}`);
  },

  getTaskCat: async (namespace: string, name: string, path: string, lines?: string, maxBytes?: number): Promise<string> => {
    const p = new URLSearchParams({ path });
    if (lines) p.set('lines', lines);
    if (maxBytes) p.set('max_bytes', maxBytes.toString());
    const res = await fetch(`${API_BASE}/namespaces/${namespace}/tasks/${name}/cat?${p.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.text();
  },

  getTaskJobLog: (namespace: string, name: string, offset?: number) => {
    const p = new URLSearchParams();
    if (offset !== undefined) p.set('offset', offset.toString());
    const q = p.toString();
    return request<{ log: string; next_offset: number }>(`/namespaces/${namespace}/tasks/${name}/job-log${q ? `?${q}` : ''}`);
  },

  getTaskTodo: (namespace: string, name: string) =>
    request<TodoItem[]>(`/namespaces/${namespace}/tasks/${name}/todo`),

  getTaskPermissions: (namespace: string, name: string) =>
    request<string[]>(`/namespaces/${namespace}/tasks/${name}/permissions`),

  getTaskExportUrl: (namespace: string, name: string) =>
    `${API_BASE}/namespaces/${namespace}/tasks/${name}/export`,

  getTaskEventsUrl: (namespace: string, name: string) =>
    `${API_BASE}/namespaces/${namespace}/tasks/${name}/events`,

  // Log streaming - returns an EventSource for SSE
  getTaskLogsUrl: (namespace: string, name: string, container?: string) => {
    const params = new URLSearchParams();
    if (container) params.set('container', container);
    const queryString = params.toString();
    return `${API_BASE}/namespaces/${namespace}/tasks/${name}/logs${queryString ? `?${queryString}` : ''}`;
  },

  // Agents
  listAllAgents: (params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<AgentListResponse>(`/agents${queryString ? `?${queryString}` : ''}`);
  },

  listAgents: (namespace: string, params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<AgentListResponse>(`/namespaces/${namespace}/agents${queryString ? `?${queryString}` : ''}`);
  },

  getAgent: (namespace: string, name: string) =>
    request<Agent>(`/namespaces/${namespace}/agents/${name}`),

  getAgentYaml: async (namespace: string, name: string): Promise<string> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/agents/${name}?output=yaml`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.text();
  },

  createAgent: (namespace: string, agent: CreateAgentRequest) =>
    request<Agent>(`/namespaces/${namespace}/agents`, {
      method: 'POST',
      body: JSON.stringify(agent),
    }),

  suspendAgent: (namespace: string, name: string) =>
    request<Agent>(`/namespaces/${namespace}/agents/${name}/suspend`, { method: 'POST' }),

  resumeAgent: (namespace: string, name: string) =>
    request<Agent>(`/namespaces/${namespace}/agents/${name}/resume`, { method: 'POST' }),

  updateAgentYaml: async (namespace: string, name: string, yaml: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/agents/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new Error(error.message || error.error || `HTTP ${response.status}`);
    }
  },

  deleteAgent: (namespace: string, name: string) =>
    request<void>(`/namespaces/${namespace}/agents/${name}`, { method: 'DELETE' }),

  // Agent Share
  getAgentShare: (namespace: string, name: string) =>
    request<ShareTokenResponse>(`/namespaces/${namespace}/agents/${name}/share`),

  updateAgentShare: (namespace: string, name: string, data: UpdateShareRequest) =>
    request<Agent>(`/namespaces/${namespace}/agents/${name}/share`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  deleteAgentShare: (namespace: string, name: string) =>
    request<Agent>(`/namespaces/${namespace}/agents/${name}/share`, {
      method: 'DELETE',
    }),

  // Agent Templates
  listAllAgentTemplates: (params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<AgentTemplateListResponse>(`/agenttemplates${queryString ? `?${queryString}` : ''}`);
  },

  listAgentTemplates: (namespace: string, params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<AgentTemplateListResponse>(`/namespaces/${namespace}/agenttemplates${queryString ? `?${queryString}` : ''}`);
  },

  getAgentTemplate: (namespace: string, name: string) =>
    request<AgentTemplate>(`/namespaces/${namespace}/agenttemplates/${name}`),

  createAgentTemplate: (namespace: string, data: CreateAgentTemplateRequest) =>
    request<AgentTemplate>(`/namespaces/${namespace}/agenttemplates`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  deleteAgentTemplate: (namespace: string, name: string) =>
    request<void>(`/namespaces/${namespace}/agenttemplates/${name}`, { method: 'DELETE' }),

  getAgentTemplateYaml: async (namespace: string, name: string): Promise<string> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/agenttemplates/${name}?output=yaml`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.text();
  },

  updateAgentTemplateYaml: async (namespace: string, name: string, yaml: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/agenttemplates/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new Error(error.message || error.error || `HTTP ${response.status}`);
    }
  },

  // CronTasks
  listAllCronTasks: (params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<CronTaskListResponse>(`/crontasks${queryString ? `?${queryString}` : ''}`);
  },

  listCronTasks: (namespace: string, params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<CronTaskListResponse>(`/namespaces/${namespace}/crontasks${queryString ? `?${queryString}` : ''}`);
  },

  getCronTask: (namespace: string, name: string) =>
    request<CronTask>(`/namespaces/${namespace}/crontasks/${name}`),

  getCronTaskYaml: async (namespace: string, name: string): Promise<string> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/crontasks/${name}?output=yaml`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.text();
  },

  createCronTask: (namespace: string, cronTask: CreateCronTaskRequest) =>
    request<CronTask>(`/namespaces/${namespace}/crontasks`, {
      method: 'POST',
      body: JSON.stringify(cronTask),
    }),

  updateCronTask: (namespace: string, name: string, data: Partial<CreateCronTaskRequest>) =>
    request<CronTask>(`/namespaces/${namespace}/crontasks/${name}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    }),

  deleteCronTask: (namespace: string, name: string) =>
    request<void>(`/namespaces/${namespace}/crontasks/${name}`, {
      method: 'DELETE',
    }),

  suspendCronTask: (namespace: string, name: string) =>
    request<CronTask>(`/namespaces/${namespace}/crontasks/${name}/suspend`, { method: 'POST' }),

  resumeCronTask: (namespace: string, name: string) =>
    request<CronTask>(`/namespaces/${namespace}/crontasks/${name}/resume`, { method: 'POST' }),

  triggerCronTask: (namespace: string, name: string) =>
    request<{ message: string }>(`/namespaces/${namespace}/crontasks/${name}/trigger`, { method: 'POST' }),

  getCronTaskHistory: (namespace: string, name: string, params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<TaskListResponse>(`/namespaces/${namespace}/crontasks/${name}/history${queryString ? `?${queryString}` : ''}`);
  },

  // Config (cluster-scoped singleton)
  getConfig: () => request<ConfigResponse>('/config'),

  getConfigYaml: async (): Promise<string> => {
    const response = await fetch(`${API_BASE}/config?output=yaml`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.text();
  },

  updateConfigYaml: async (yaml: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/config`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new Error(error.message || error.error || `HTTP ${response.status}`);
    }
  },

  updateCronTaskYaml: async (namespace: string, name: string, yaml: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/crontasks/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new Error(error.message || error.error || `HTTP ${response.status}`);
    }
  },

  // Registries
  listAllRegistries: (params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<RegistryListResponse>(`/registries${queryString ? `?${queryString}` : ''}`);
  },

  listRegistries: (namespace: string, params?: FilterParams) => {
    const searchParams = new URLSearchParams();
    if (params?.name) searchParams.set('name', params.name);
    if (params?.labelSelector) searchParams.set('labelSelector', params.labelSelector);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset !== undefined) searchParams.set('offset', params.offset.toString());
    if (params?.sortOrder) searchParams.set('sortOrder', params.sortOrder);
    const queryString = searchParams.toString();
    return request<RegistryListResponse>(`/namespaces/${namespace}/registries${queryString ? `?${queryString}` : ''}`);
  },

  getRegistry: (namespace: string, name: string) =>
    request<Registry>(`/namespaces/${namespace}/registries/${name}`),

  getRegistryYaml: async (namespace: string, name: string): Promise<string> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/registries/${name}?output=yaml`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.text();
  },

  deleteRegistry: (namespace: string, name: string) =>
    request<void>(`/namespaces/${namespace}/registries/${name}`, { method: 'DELETE' }),

  updateRegistryYaml: async (namespace: string, name: string, yaml: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/namespaces/${namespace}/registries/${name}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new Error(error.message || error.error || `HTTP ${response.status}`);
    }
  },

  // Agent Worker API (M6)
  getAgentWorkerInfo: (namespace: string, name: string) =>
    request<any>(`/namespaces/${namespace}/agents/${name}/worker/info`),

  getAgentWorkerCapabilities: (namespace: string, name: string) =>
    request<CapabilitiesResponse>(`/namespaces/${namespace}/agents/${name}/worker/capabilities`),

  getAgentWorkerModels: (namespace: string, name: string) =>
    request<ModelEntry[]>(`/namespaces/${namespace}/agents/${name}/worker/models`),

  getAgentWorkerQuota: (namespace: string, name: string, fresh?: boolean) =>
    request<QuotaResponse>(`/namespaces/${namespace}/agents/${name}/worker/quota${fresh ? '?fresh=true' : ''}`),

  getAgentWorkerProjects: (namespace: string, name: string) =>
    request<ProjectEntry[]>(`/namespaces/${namespace}/agents/${name}/worker/projects`),

  getAgentWorkerJobs: (namespace: string, name: string, filter?: { project?: string; engine?: string; status?: string }) => {
    const p = new URLSearchParams();
    if (filter?.project) p.set('project', filter.project);
    if (filter?.engine) p.set('engine', filter.engine);
    if (filter?.status) p.set('status', filter.status);
    const q = p.toString();
    return request<WorkerJob[]>(`/namespaces/${namespace}/agents/${name}/worker/jobs${q ? `?${q}` : ''}`);
  },

  getAgentWorkerAuthStatus: (namespace: string, name: string) =>
    request<AuthStatusResponse>(`/namespaces/${namespace}/agents/${name}/worker/auth/status`),

  startAgentWorkerAuth: (namespace: string, name: string) =>
    request<{ url?: string; code?: string }>(`/namespaces/${namespace}/agents/${name}/worker/auth/start`, { method: 'POST' }),

  submitAgentWorkerAuthCode: (namespace: string, name: string, code: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/agents/${name}/worker/auth/code`, {
      method: 'POST',
      body: JSON.stringify({ code }),
    }),

  cancelAgentWorkerAuth: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/agents/${name}/worker/auth/cancel`, { method: 'POST' }),

  getAgentWorkerRemoteControlStatus: (namespace: string, name: string) =>
    request<RemoteControlStatusResponse>(`/namespaces/${namespace}/agents/${name}/worker/remote-control/status`),

  enableAgentWorkerRemoteControl: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/agents/${name}/worker/remote-control/enable`, { method: 'POST' }),

  disableAgentWorkerRemoteControl: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/agents/${name}/worker/remote-control/disable`, { method: 'POST' }),

  runAgentWorkerGC: (namespace: string, name: string, req?: GCRequest) =>
    request<GCResult>(`/namespaces/${namespace}/agents/${name}/worker/gc`, {
      method: 'POST',
      body: JSON.stringify(req || {}),
    }),

  initAgentWorker: (namespace: string, name: string, req?: any) =>
    request<{ status: string }>(`/namespaces/${namespace}/agents/${name}/worker/init`, {
      method: 'POST',
      body: JSON.stringify(req || {}),
    }),

  repairAgentWorkerJobs: (namespace: string, name: string) =>
    request<{ status: string }>(`/namespaces/${namespace}/agents/${name}/worker/jobs/repair`, { method: 'POST' }),

  endAgentWorkerJobset: (namespace: string, name: string, jobsetId: string) =>
    request<any>(`/namespaces/${namespace}/agents/${name}/worker/jobsets/${jobsetId}/end`, { method: 'POST' }),

};

export default api;
