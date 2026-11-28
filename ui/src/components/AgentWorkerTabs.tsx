import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api, { Agent, WorkerJob, ProjectEntry, QuotaResponse, AuthStatusResponse, CapabilitiesResponse, RemoteControlStatusResponse } from '../api/client';
import { useToast } from '../contexts/ToastContext';

interface Props {
  agent: Agent;
  tab: string;
}

export const AgentWorkerTabs: React.FC<Props> = ({ agent, tab }) => {
  const { addToast } = useToast();
  const queryClient = useQueryClient();

  // Models Query
  const { data: models, isLoading: loadingModels } = useQuery({
    queryKey: ['agent-models', agent.namespace, agent.name],
    queryFn: () => api.getAgentWorkerModels(agent.namespace, agent.name),
    enabled: tab === 'models',
  });

  // Quota Query
  const { data: quota, isLoading: loadingQuota, refetch: refetchQuota } = useQuery({
    queryKey: ['agent-quota', agent.namespace, agent.name],
    queryFn: () => api.getAgentWorkerQuota(agent.namespace, agent.name, true),
    enabled: tab === 'quota',
  });

  // Projects Query
  const { data: projects, isLoading: loadingProjects } = useQuery({
    queryKey: ['agent-projects', agent.namespace, agent.name],
    queryFn: () => api.getAgentWorkerProjects(agent.namespace, agent.name),
    enabled: tab === 'projects',
  });

  // Jobs Query
  const [jobEngineFilter, setJobEngineFilter] = useState('');
  const [jobStatusFilter, setJobStatusFilter] = useState('');
  const { data: jobs, isLoading: loadingJobs, refetch: refetchJobs } = useQuery({
    queryKey: ['agent-jobs', agent.namespace, agent.name, jobEngineFilter, jobStatusFilter],
    queryFn: () => api.getAgentWorkerJobs(agent.namespace, agent.name, {
      engine: jobEngineFilter || undefined,
      status: jobStatusFilter || undefined,
    }),
    enabled: tab === 'jobs',
  });

  // Auth Query & Mutations
  const { data: authStatus, isLoading: loadingAuth, refetch: refetchAuth } = useQuery({
    queryKey: ['agent-auth', agent.namespace, agent.name],
    queryFn: () => api.getAgentWorkerAuthStatus(agent.namespace, agent.name),
    enabled: tab === 'auth',
  });
  const [authCode, setAuthCode] = useState('');
  const [authStartResult, setAuthStartResult] = useState<{ url?: string; code?: string } | null>(null);

  const startAuthMutation = useMutation({
    mutationFn: () => api.startAgentWorkerAuth(agent.namespace, agent.name),
    onSuccess: (res) => {
      setAuthStartResult(res);
      addToast('Auth session initiated', 'info');
    },
    onError: (err: Error) => addToast(`Failed to start auth: ${err.message}`, 'error'),
  });

  const submitCodeMutation = useMutation({
    mutationFn: (code: string) => api.submitAgentWorkerAuthCode(agent.namespace, agent.name, code),
    onSuccess: () => {
      addToast('Auth code submitted successfully', 'success');
      setAuthCode('');
      setAuthStartResult(null);
      refetchAuth();
    },
    onError: (err: Error) => addToast(`Failed to submit code: ${err.message}`, 'error'),
  });

  const cancelAuthMutation = useMutation({
    mutationFn: () => api.cancelAgentWorkerAuth(agent.namespace, agent.name),
    onSuccess: () => {
      addToast('Auth flow cancelled', 'info');
      setAuthStartResult(null);
      refetchAuth();
    },
    onError: (err: Error) => addToast(`Failed to cancel auth: ${err.message}`, 'error'),
  });

  // Capabilities Query
  const { data: caps, isLoading: loadingCaps } = useQuery({
    queryKey: ['agent-caps', agent.namespace, agent.name],
    queryFn: () => api.getAgentWorkerCapabilities(agent.namespace, agent.name),
    enabled: tab === 'capabilities',
  });

  // Remote Control Query & Mutations
  const { data: rcStatus, isLoading: loadingRC, refetch: refetchRC } = useQuery({
    queryKey: ['agent-rc', agent.namespace, agent.name],
    queryFn: () => api.getAgentWorkerRemoteControlStatus(agent.namespace, agent.name),
    enabled: tab === 'maintenance',
  });

  const enableRCMutation = useMutation({
    mutationFn: () => api.enableAgentWorkerRemoteControl(agent.namespace, agent.name),
    onSuccess: () => { addToast('Remote control enabled', 'success'); refetchRC(); },
    onError: (err: Error) => addToast(`Failed to enable remote control: ${err.message}`, 'error'),
  });

  const disableRCMutation = useMutation({
    mutationFn: () => api.disableAgentWorkerRemoteControl(agent.namespace, agent.name),
    onSuccess: () => { addToast('Remote control disabled', 'info'); refetchRC(); },
    onError: (err: Error) => addToast(`Failed to disable remote control: ${err.message}`, 'error'),
  });

  // Maintenance Actions
  const gcMutation = useMutation({
    mutationFn: (dryRun: boolean) => api.runAgentWorkerGC(agent.namespace, agent.name, { dry_run: dryRun }),
    onSuccess: (res, dryRun) => {
      addToast(
        `${dryRun ? '[Dry-run] ' : ''}GC completed: ${res.removed_jobs ?? 0} jobs, ${((res.freed_bytes ?? 0) / 1024 / 1024).toFixed(1)} MB freed`,
        'success'
      );
    },
    onError: (err: Error) => addToast(`GC failed: ${err.message}`, 'error'),
  });

  const repairMutation = useMutation({
    mutationFn: () => api.repairAgentWorkerJobs(agent.namespace, agent.name),
    onSuccess: () => addToast('Worker jobs repair completed', 'success'),
    onError: (err: Error) => addToast(`Repair failed: ${err.message}`, 'error'),
  });

  const initMutation = useMutation({
    mutationFn: () => api.initAgentWorker(agent.namespace, agent.name),
    onSuccess: () => addToast('Worker workspace re-initialized', 'success'),
    onError: (err: Error) => addToast(`Init failed: ${err.message}`, 'error'),
  });

  if (tab === 'models') {
    return (
      <div className="px-6 py-5 space-y-4">
        <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
          Available Models {models ? `(${models.length})` : ''}
        </h3>
        {loadingModels ? (
          <p className="text-xs text-stone-400">Loading models...</p>
        ) : !models || models.length === 0 ? (
          <p className="text-xs text-stone-400 font-mono">No models reported by worker.</p>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2">
            {models.map((m, idx) => (
              <div key={idx} className="bg-stone-50 border border-stone-200/80 rounded-lg px-3 py-2 text-xs font-mono text-stone-700 flex items-center justify-between">
                <span className="truncate">{m.id}</span>
                {m.provider && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-stone-200/60 text-stone-500 ml-2">
                    {m.provider}
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (tab === 'quota') {
    return (
      <div className="px-6 py-5 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
            Worker Quota & Rate Limits
          </h3>
          <button
            onClick={() => refetchQuota()}
            className="px-2.5 py-1 text-xs text-stone-600 bg-stone-100 hover:bg-stone-200 rounded font-medium transition-colors"
          >
            Refresh
          </button>
        </div>
        {loadingQuota ? (
          <p className="text-xs text-stone-400">Loading quota...</p>
        ) : !quota?.groups || Object.keys(quota.groups).length === 0 ? (
          <p className="text-xs text-stone-400 font-mono">No active quota limits or groups reported.</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {Object.entries(quota.groups).map(([grp, entry]) => (
              <div key={grp} className="bg-stone-50 border border-stone-200 rounded-lg p-3">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-medium text-stone-800 font-mono">{grp}</span>
                  {entry.remaining !== undefined && entry.limit !== undefined && (
                    <span className="text-xs font-mono text-stone-600 font-semibold">
                      {entry.remaining} / {entry.limit}
                    </span>
                  )}
                </div>
                {entry.reset && (
                  <p className="text-[11px] text-stone-400">Resets: {new Date(entry.reset).toLocaleString()}</p>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (tab === 'projects') {
    return (
      <div className="px-6 py-5 space-y-4">
        <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
          Workspace Projects
        </h3>
        {loadingProjects ? (
          <p className="text-xs text-stone-400">Loading projects...</p>
        ) : !projects || projects.length === 0 ? (
          <p className="text-xs text-stone-400 font-mono">No tracked projects found on worker.</p>
        ) : (
          <div className="space-y-2">
            {projects.map((proj) => (
              <div key={proj.id} className="bg-stone-50 border border-stone-200 rounded-lg p-3 flex items-center justify-between">
                <div>
                  <h4 className="text-xs font-semibold text-stone-800 font-mono">{proj.name || proj.id}</h4>
                  {proj.path && <p className="text-[11px] text-stone-400 font-mono mt-0.5">{proj.path}</p>}
                </div>
                <span className="text-xs text-stone-500 font-mono bg-stone-100 px-2 py-1 rounded">
                  {proj.id}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (tab === 'jobs') {
    return (
      <div className="px-6 py-5 space-y-4">
        <div className="flex items-center justify-between gap-4">
          <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
            Worker Jobs ({jobs?.length ?? 0})
          </h3>
          <div className="flex items-center gap-2">
            <select
              value={jobEngineFilter}
              onChange={(e) => setJobEngineFilter(e.target.value)}
              className="text-xs border border-stone-200 rounded px-2 py-1 bg-white"
            >
              <option value="">All Engines</option>
              <option value="opencode">opencode</option>
              <option value="agy">agy</option>
              <option value="claude">claude</option>
            </select>
            <select
              value={jobStatusFilter}
              onChange={(e) => setJobStatusFilter(e.target.value)}
              className="text-xs border border-stone-200 rounded px-2 py-1 bg-white"
            >
              <option value="">All Statuses</option>
              <option value="running">running</option>
              <option value="done">done</option>
              <option value="failed">failed</option>
            </select>
            <button
              onClick={() => refetchJobs()}
              className="px-2.5 py-1 text-xs text-stone-600 bg-stone-100 hover:bg-stone-200 rounded font-medium transition-colors"
            >
              Refresh
            </button>
          </div>
        </div>

        {loadingJobs ? (
          <p className="text-xs text-stone-400">Loading jobs...</p>
        ) : !jobs || jobs.length === 0 ? (
          <p className="text-xs text-stone-400 font-mono">No jobs found on worker storage.</p>
        ) : (
          <div className="divide-y divide-stone-100 border border-stone-200 rounded-lg overflow-hidden bg-white">
            {jobs.map((job) => (
              <div key={job.id} className="p-3 hover:bg-stone-50/60 flex items-center justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-mono font-medium text-stone-900">{job.id}</span>
                    <span className={`text-[10px] uppercase font-mono px-1.5 py-0.5 rounded font-semibold ${
                      job.status === 'running' ? 'bg-blue-100 text-blue-700' :
                      job.status === 'done' ? 'bg-emerald-100 text-emerald-700' :
                      'bg-rose-100 text-rose-700'
                    }`}>
                      {job.status}
                    </span>
                    {job.engine && (
                      <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-stone-100 text-stone-600">
                        {job.engine}
                      </span>
                    )}
                  </div>
                  {job.project && (
                    <p className="text-[11px] text-stone-500 mt-1">Project: <span className="font-mono">{job.project}</span></p>
                  )}
                </div>
                <div className="text-right">
                  {job.elapsed_seconds !== undefined && (
                    <span className="text-xs text-stone-400 font-mono">{job.elapsed_seconds}s</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (tab === 'auth') {
    return (
      <div className="px-6 py-5 space-y-5">
        <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
          Worker Authentication & OAuth
        </h3>

        {loadingAuth ? (
          <p className="text-xs text-stone-400">Loading auth state...</p>
        ) : (
          <div className="space-y-4">
            <div className="bg-stone-50 border border-stone-200 rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-medium text-stone-700">Overall Status:</span>
                <span className={`text-xs font-semibold px-2 py-0.5 rounded ${
                  authStatus?.authenticated ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'
                }`}>
                  {authStatus?.authenticated ? 'Authenticated' : 'Unauthenticated'}
                </span>
              </div>
              {authStatus?.user && (
                <p className="text-xs text-stone-600">User: <span className="font-mono">{authStatus.user}</span></p>
              )}
              {authStatus?.engines && (
                <div className="mt-2 space-y-1">
                  <span className="text-[11px] text-stone-400 uppercase font-semibold">Engine States:</span>
                  {Object.entries(authStatus.engines).map(([eng, st]) => (
                    <div key={eng} className="flex justify-between text-xs text-stone-600 font-mono">
                      <span>{eng}:</span>
                      <span>{st}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="border border-stone-200 rounded-lg p-4 bg-white space-y-3">
              <h4 className="text-xs font-display font-semibold text-stone-700">Interactive Flow</h4>
              {!authStartResult ? (
                <button
                  onClick={() => startAuthMutation.mutate()}
                  disabled={startAuthMutation.isPending}
                  className="px-3 py-1.5 text-xs font-medium text-white bg-primary-600 rounded-lg hover:bg-primary-700 disabled:opacity-50 transition-colors"
                >
                  {startAuthMutation.isPending ? 'Starting...' : 'Start OAuth Flow'}
                </button>
              ) : (
                <div className="space-y-3">
                  {authStartResult.url && (
                    <div className="p-3 bg-blue-50 border border-blue-200 rounded text-xs text-blue-800">
                      <p className="font-semibold mb-1">Open this URL in your browser to authorize:</p>
                      <a href={authStartResult.url} target="_blank" rel="noreferrer" className="underline break-all font-mono">
                        {authStartResult.url}
                      </a>
                    </div>
                  )}
                  {authStartResult.code && (
                    <p className="text-xs font-mono text-stone-600">Verification Code: <span className="font-bold text-stone-900">{authStartResult.code}</span></p>
                  )}
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="Paste callback / auth code here..."
                      value={authCode}
                      onChange={(e) => setAuthCode(e.target.value)}
                      className="text-xs border border-stone-200 rounded px-3 py-1.5 flex-1 font-mono"
                    />
                    <button
                      onClick={() => submitCodeMutation.mutate(authCode)}
                      disabled={!authCode || submitCodeMutation.isPending}
                      className="px-3 py-1.5 text-xs font-medium text-white bg-emerald-600 rounded hover:bg-emerald-700 disabled:opacity-50"
                    >
                      Submit Code
                    </button>
                    <button
                      onClick={() => cancelAuthMutation.mutate()}
                      disabled={cancelAuthMutation.isPending}
                      className="px-3 py-1.5 text-xs font-medium text-stone-600 bg-stone-100 rounded hover:bg-stone-200"
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    );
  }

  if (tab === 'capabilities') {
    return (
      <div className="px-6 py-5 space-y-4">
        <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
          Engine Capabilities Matrix
        </h3>
        {loadingCaps ? (
          <p className="text-xs text-stone-400">Loading capabilities...</p>
        ) : !caps || !caps.endpoints ? (
          <p className="text-xs text-stone-400 font-mono">No capabilities reported.</p>
        ) : (
          <div className="border border-stone-200 rounded-lg overflow-hidden bg-white">
            <div className="px-4 py-2 bg-stone-50 border-b border-stone-200 flex justify-between items-center text-xs font-mono">
              <span className="font-semibold text-stone-800">Engine: {caps.engine}</span>
            </div>
            <div className="divide-y divide-stone-100">
              {Object.entries(caps.endpoints).map(([endpoint, support]) => (
                <div key={endpoint} className="px-4 py-2 flex items-center justify-between text-xs font-mono">
                  <span className="text-stone-700">{endpoint}</span>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-semibold uppercase ${
                    support === 'native' ? 'bg-emerald-100 text-emerald-800' :
                    support === 'emulated' ? 'bg-blue-100 text-blue-800' :
                    'bg-stone-100 text-stone-500'
                  }`}>
                    {support}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  }

  if (tab === 'maintenance') {
    return (
      <div className="px-6 py-5 space-y-6">
        <div>
          <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider mb-3">
            Remote Control Daemon
          </h3>
          <div className="bg-stone-50 border border-stone-200 rounded-lg p-4 flex items-center justify-between">
            <div>
              <p className="text-xs font-medium text-stone-800">
                Daemon State: <span className="font-mono font-semibold">{rcStatus?.enabled ? 'Active / Enabled' : 'Disabled'}</span>
              </p>
              <p className="text-[11px] text-stone-400 mt-0.5">Control autonomous task dispatch and telemetry</p>
            </div>
            <div>
              {rcStatus?.enabled ? (
                <button
                  onClick={() => disableRCMutation.mutate()}
                  disabled={disableRCMutation.isPending}
                  className="px-3 py-1.5 text-xs font-medium text-amber-700 bg-amber-50 border border-amber-200 rounded-lg hover:bg-amber-100 disabled:opacity-50"
                >
                  Disable Daemon
                </button>
              ) : (
                <button
                  onClick={() => enableRCMutation.mutate()}
                  disabled={enableRCMutation.isPending}
                  className="px-3 py-1.5 text-xs font-medium text-white bg-primary-600 rounded-lg hover:bg-primary-700 disabled:opacity-50"
                >
                  Enable Daemon
                </button>
              )}
            </div>
          </div>
        </div>

        <div>
          <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider mb-3">
            Worker Maintenance & Cleanup
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div className="bg-stone-50 border border-stone-200 rounded-lg p-3 space-y-2">
              <h4 className="text-xs font-semibold text-stone-800">Garbage Collection</h4>
              <p className="text-[11px] text-stone-500 leading-tight">Remove stale completed jobs and prune workspace worktrees.</p>
              <div className="flex gap-2 pt-1">
                <button
                  onClick={() => gcMutation.mutate(true)}
                  disabled={gcMutation.isPending}
                  className="px-2.5 py-1 text-[11px] font-medium text-stone-600 bg-white border border-stone-200 rounded hover:bg-stone-100"
                >
                  Dry-Run
                </button>
                <button
                  onClick={() => gcMutation.mutate(false)}
                  disabled={gcMutation.isPending}
                  className="px-2.5 py-1 text-[11px] font-medium text-white bg-rose-600 rounded hover:bg-rose-700"
                >
                  Prune Now
                </button>
              </div>
            </div>

            <div className="bg-stone-50 border border-stone-200 rounded-lg p-3 space-y-2">
              <h4 className="text-xs font-semibold text-stone-800">Repair Jobs</h4>
              <p className="text-[11px] text-stone-500 leading-tight">Fix stuck lock files, recover aborted job trees, and clean corrupt worktrees.</p>
              <div className="pt-1">
                <button
                  onClick={() => repairMutation.mutate()}
                  disabled={repairMutation.isPending}
                  className="px-2.5 py-1 text-[11px] font-medium text-stone-700 bg-white border border-stone-200 rounded hover:bg-stone-100"
                >
                  Repair Jobs
                </button>
              </div>
            </div>

            <div className="bg-stone-50 border border-stone-200 rounded-lg p-3 space-y-2">
              <h4 className="text-xs font-semibold text-stone-800">Workspace Init</h4>
              <p className="text-[11px] text-stone-500 leading-tight">Verify directory structure and re-initialize worker runtime storage.</p>
              <div className="pt-1">
                <button
                  onClick={() => initMutation.mutate()}
                  disabled={initMutation.isPending}
                  className="px-2.5 py-1 text-[11px] font-medium text-stone-700 bg-white border border-stone-200 rounded hover:bg-stone-100"
                >
                  Run Init
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return null;
};
