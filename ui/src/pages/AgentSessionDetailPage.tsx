import React, { useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { useQuery, useMutation } from '@tanstack/react-query';
import api from '../api/client';
import { useToast } from '../contexts/ToastContext';
import Breadcrumbs from '../components/Breadcrumbs';
import TimeAgo from '../components/TimeAgo';

function AgentSessionDetailPage() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>();
  const navigate = useNavigate();
  const { addToast } = useToast();
  const [inputText, setInputText] = useState('');

  const { data: session, isLoading, error, refetch } = useQuery({
    queryKey: ['agentsession', namespace, name],
    queryFn: () => api.getAgentSession(namespace!, name!),
    refetchInterval: 3000,
    enabled: !!namespace && !!name,
  });

  const { data: logText, refetch: refetchLog } = useQuery({
    queryKey: ['agentsession-log', namespace, name],
    queryFn: () => api.getAgentSessionLog(namespace!, name!, 200),
    refetchInterval: 3000,
    enabled: !!namespace && !!name,
  });

  const startMutation = useMutation({
    mutationFn: () => api.startAgentSession(namespace!, name!),
    onSuccess: () => { addToast('Session started', 'success'); refetch(); },
    onError: (err: Error) => addToast(`Failed: ${err.message}`, 'error'),
  });

  const stopMutation = useMutation({
    mutationFn: () => api.stopAgentSession(namespace!, name!),
    onSuccess: () => { addToast('Session stopped', 'info'); refetch(); },
    onError: (err: Error) => addToast(`Failed: ${err.message}`, 'error'),
  });

  const restartMutation = useMutation({
    mutationFn: () => api.restartAgentSession(namespace!, name!),
    onSuccess: () => { addToast('Session restarted', 'success'); refetch(); },
    onError: (err: Error) => addToast(`Failed: ${err.message}`, 'error'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.deleteAgentSession(namespace!, name!),
    onSuccess: () => {
      addToast('Session deleted', 'success');
      navigate('/sessions');
    },
    onError: (err: Error) => addToast(`Failed: ${err.message}`, 'error'),
  });

  const messageMutation = useMutation({
    mutationFn: (text: string) => api.messageAgentSession(namespace!, name!, text),
    onSuccess: () => {
      addToast('Message delivered to session', 'success');
      setInputText('');
      refetchLog();
    },
    onError: (err: Error) => addToast(`Message rejected: ${err.message}`, 'error'),
  });

  if (isLoading) {
    return <div className="p-8 text-center text-stone-400">Loading session...</div>;
  }

  if (error || !session) {
    return <div className="p-8 text-center text-red-500">Error loading session</div>;
  }

  return (
    <div className="space-y-6 animate-fade-in">
      <Breadcrumbs items={[
        { label: 'Sessions', to: '/sessions' },
        { label: namespace!, isNamespace: true },
        { label: name! },
      ]} />

      <div className="bg-white rounded-xl border-0 overflow-hidden shadow-card">
        <div className="px-6 py-5 border-b border-stone-100 flex items-center justify-between">
          <div>
            <div className="flex items-center gap-3">
              <h2 className="font-display text-xl font-bold text-stone-900">{session.name}</h2>
              <span className={`px-2 py-0.5 rounded text-xs font-semibold uppercase font-mono ${
                session.phase === 'Running' ? 'bg-emerald-100 text-emerald-700' :
                session.phase === 'Stopped' ? 'bg-stone-100 text-stone-600' :
                session.phase === 'Failed' ? 'bg-rose-100 text-rose-700' :
                'bg-blue-100 text-blue-700'
              }`}>
                {session.phase}
              </span>
            </div>
            <p className="text-xs text-stone-400 mt-0.5 font-mono">
              Agent: <Link to={`/agents/${session.namespace}/${session.agentName}`} className="text-primary-600 hover:underline">{session.agentName}</Link>
            </p>
          </div>

          <div className="flex items-center gap-2">
            {session.phase === 'Running' ? (
              <button
                onClick={() => stopMutation.mutate()}
                disabled={stopMutation.isPending}
                className="px-3 py-1.5 text-xs font-medium text-amber-700 bg-amber-50 border border-amber-200 rounded-lg hover:bg-amber-100 disabled:opacity-50"
              >
                Pause / Stop
              </button>
            ) : (
              <button
                onClick={() => startMutation.mutate()}
                disabled={startMutation.isPending}
                className="px-3 py-1.5 text-xs font-medium text-white bg-emerald-600 rounded-lg hover:bg-emerald-700 disabled:opacity-50"
              >
                Start / Resume
              </button>
            )}
            <button
              onClick={() => restartMutation.mutate()}
              disabled={restartMutation.isPending}
              className="px-3 py-1.5 text-xs font-medium text-stone-700 bg-stone-100 hover:bg-stone-200 rounded-lg disabled:opacity-50"
            >
              Restart Process
            </button>
            <button
              onClick={() => deleteMutation.mutate()}
              disabled={deleteMutation.isPending}
              className="px-3 py-1.5 text-xs font-medium text-rose-600 bg-rose-50 border border-rose-200 hover:bg-rose-100 rounded-lg disabled:opacity-50"
            >
              Delete
            </button>
          </div>
        </div>

        <div className="px-6 py-5 space-y-6">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div className="bg-stone-50 p-3 rounded-lg border border-stone-100">
              <dt className="text-[11px] text-stone-400">Process PID</dt>
              <dd className="mt-1 text-xs font-mono font-semibold text-stone-800">{session.pid ?? '-'}</dd>
            </div>
            <div className="bg-stone-50 p-3 rounded-lg border border-stone-100">
              <dt className="text-[11px] text-stone-400">Engine</dt>
              <dd className="mt-1 text-xs font-mono font-semibold text-stone-800">{session.engine || '-'}</dd>
            </div>
            <div className="bg-stone-50 p-3 rounded-lg border border-stone-100">
              <dt className="text-[11px] text-stone-400">Uptime</dt>
              <dd className="mt-1 text-xs font-mono text-stone-800">{session.uptimeSecs ? `${session.uptimeSecs}s` : '-'}</dd>
            </div>
            <div className="bg-stone-50 p-3 rounded-lg border border-stone-100">
              <dt className="text-[11px] text-stone-400">Restarts</dt>
              <dd className="mt-1 text-xs font-mono text-stone-800">{session.restartCount ?? 0}</dd>
            </div>
          </div>

          <div>
            <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider mb-2">
              Inject Prompt / Interactive Message
            </h3>
            <div className="flex gap-2">
              <input
                type="text"
                placeholder="Type message to send into session stdin / server..."
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && inputText.trim()) {
                    messageMutation.mutate(inputText);
                  }
                }}
                className="flex-1 px-3 py-2 text-sm border border-stone-200 rounded-lg font-mono focus:border-primary-500"
              />
              <button
                onClick={() => messageMutation.mutate(inputText)}
                disabled={!inputText.trim() || messageMutation.isPending}
                className="px-4 py-2 text-xs font-medium text-white bg-primary-600 rounded-lg hover:bg-primary-700 disabled:opacity-50"
              >
                Send Message
              </button>
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-xs font-display font-semibold text-stone-500 uppercase tracking-wider">
                Session Output Ring Log
              </h3>
              <button
                onClick={() => refetchLog()}
                className="text-xs text-stone-500 hover:text-stone-800 underline"
              >
                Refresh Log
              </button>
            </div>
            <pre className="bg-stone-900 text-stone-200 p-4 rounded-lg font-mono text-xs overflow-x-auto max-h-96 whitespace-pre-wrap leading-relaxed">
              {logText || 'No log output captured yet.'}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}

export default AgentSessionDetailPage;
