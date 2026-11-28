import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import api from '../api/client';
import { useNamespace } from '../contexts/NamespaceContext';
import TimeAgo from '../components/TimeAgo';
import EmptyState from '../components/EmptyState';
import { TableSkeleton } from '../components/Skeleton';

function AgentSessionsPage() {
  const { namespace, isAllNamespaces } = useNamespace();
  const [searchTerm, setSearchTerm] = useState('');

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['agentsessions', namespace, isAllNamespaces],
    queryFn: () =>
      isAllNamespaces
        ? api.listAllAgentSessions({ limit: 50, sortOrder: 'desc' })
        : api.listAgentSessions(namespace, { limit: 50, sortOrder: 'desc' }),
    refetchInterval: 5000,
  });

  const sessions = (data?.sessions || []).filter((s) =>
    s.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h2 className="font-display text-2xl font-bold text-stone-900 tracking-tight">Interactive Sessions</h2>
          <p className="mt-1 text-sm text-stone-500">
            Supervised interactive agent sessions and persistent remote-control runtimes
          </p>
        </div>
        <div>
          <Link
            to="/sessions/create"
            className="inline-flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-white bg-primary-600 rounded-lg hover:bg-primary-700 transition-colors shadow-sm"
          >
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M12 5v14M5 12h14" strokeLinecap="round" />
            </svg>
            New Session
          </Link>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <input
          type="text"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          placeholder="Filter sessions by name..."
          className="block w-64 px-3 py-2 rounded-lg border border-stone-200 text-sm placeholder:text-stone-300 focus:border-primary-500 focus:ring-primary-500 bg-white"
        />
        <button
          onClick={() => refetch()}
          className="px-3 py-2 text-xs font-medium text-stone-600 bg-stone-100 hover:bg-stone-200 rounded-lg transition-colors"
        >
          Refresh
        </button>
      </div>

      {isLoading ? (
        <div className="bg-white rounded-xl border-0 overflow-hidden shadow-card">
          <TableSkeleton rows={5} cols={isAllNamespaces ? 7 : 6} />
        </div>
      ) : error ? (
        <div className="bg-red-50 border border-red-200 rounded-xl p-5">
          <p className="text-red-700 text-sm">Error loading sessions: {(error as Error).message}</p>
        </div>
      ) : (
        <div className="bg-white rounded-xl border-0 overflow-hidden shadow-card">
          <table className="min-w-full divide-y divide-stone-100">
            <thead className="bg-stone-50/60">
              <tr>
                <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Name</th>
                {isAllNamespaces && (
                  <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Namespace</th>
                )}
                <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Agent</th>
                <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Status</th>
                <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Engine</th>
                <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Uptime</th>
                <th className="px-5 py-3 text-left text-[11px] font-display font-medium text-stone-400 uppercase tracking-wider">Created</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-stone-100">
              {sessions.length === 0 ? (
                <tr>
                  <td colSpan={isAllNamespaces ? 7 : 6} className="px-6 py-12 text-center text-stone-400 text-sm">
                    No active agent sessions found. Create a session to launch an interactive agent supervisor.
                  </td>
                </tr>
              ) : (
                sessions.map((s) => (
                  <tr key={`${s.namespace}/${s.name}`} className="hover:bg-stone-50/60 transition-colors">
                    <td className="px-5 py-3.5 whitespace-nowrap">
                      <Link
                        to={`/sessions/${s.namespace}/${s.name}`}
                        className="text-stone-800 hover:text-primary-600 font-medium text-sm transition-colors"
                      >
                        {s.name}
                      </Link>
                    </td>
                    {isAllNamespaces && (
                      <td className="px-5 py-3.5 whitespace-nowrap text-sm text-stone-400">{s.namespace}</td>
                    )}
                    <td className="px-5 py-3.5 whitespace-nowrap text-xs font-mono text-stone-600">
                      <Link to={`/agents/${s.namespace}/${s.agentName}`} className="hover:underline">
                        {s.agentName}
                      </Link>
                    </td>
                    <td className="px-5 py-3.5 whitespace-nowrap">
                      <span className={`px-2 py-0.5 rounded text-xs font-semibold uppercase font-mono ${
                        s.phase === 'Running' ? 'bg-emerald-100 text-emerald-700' :
                        s.phase === 'Stopped' ? 'bg-stone-100 text-stone-600' :
                        s.phase === 'Failed' ? 'bg-rose-100 text-rose-700' :
                        'bg-blue-100 text-blue-700'
                      }`}>
                        {s.phase}
                      </span>
                    </td>
                    <td className="px-5 py-3.5 whitespace-nowrap text-xs font-mono text-stone-600">
                      {s.engine || '-'}
                    </td>
                    <td className="px-5 py-3.5 whitespace-nowrap text-xs font-mono text-stone-400">
                      {s.uptimeSecs ? `${s.uptimeSecs}s` : '-'}
                    </td>
                    <td className="px-5 py-3.5 whitespace-nowrap text-xs text-stone-400">
                      <TimeAgo date={s.createdAt} />
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default AgentSessionsPage;
