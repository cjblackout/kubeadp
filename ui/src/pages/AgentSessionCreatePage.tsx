import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery, useMutation } from '@tanstack/react-query';
import api from '../api/client';
import { useToast } from '../contexts/ToastContext';
import { useNamespace } from '../contexts/NamespaceContext';
import Breadcrumbs from '../components/Breadcrumbs';

function AgentSessionCreatePage() {
  const navigate = useNavigate();
  const { addToast } = useToast();
  const { namespace, isAllNamespaces } = useNamespace();
  const targetNamespace = isAllNamespaces ? 'default' : namespace;

  const [name, setName] = useState('');
  const [selectedAgent, setSelectedAgent] = useState('');
  const [engine, setEngine] = useState('');
  const [instructionsFile, setInstructionsFile] = useState('');
  const [sandbox, setSandbox] = useState(true);

  const { data: agentsData } = useQuery({
    queryKey: ['agents-for-session-create', targetNamespace],
    queryFn: () => api.listAgents(targetNamespace, { limit: 100 }),
  });

  const createMutation = useMutation({
    mutationFn: () =>
      api.createAgentSession(targetNamespace, {
        name,
        agentName: selectedAgent,
        engine: engine || undefined,
        instructionsFile: instructionsFile || undefined,
        sandbox,
      }),
    onSuccess: (session) => {
      addToast(`Session "${session.name}" created`, 'success');
      navigate(`/sessions/${session.namespace}/${session.name}`);
    },
    onError: (err: Error) => addToast(`Failed: ${err.message}`, 'error'),
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name || !selectedAgent) return;
    createMutation.mutate();
  };

  return (
    <div className="space-y-6 animate-fade-in max-w-2xl">
      <Breadcrumbs items={[
        { label: 'Sessions', to: '/sessions' },
        { label: 'Create Session' },
      ]} />

      <div className="bg-white rounded-xl border-0 overflow-hidden shadow-card">
        <div className="px-6 py-5 border-b border-stone-100">
          <h2 className="font-display text-xl font-bold text-stone-900">Create Agent Session</h2>
          <p className="text-sm text-stone-400 mt-0.5">Start an interactive supervisor runtime on an Agent</p>
        </div>

        <form onSubmit={handleSubmit} className="px-6 py-5 space-y-4">
          <div>
            <label className="block text-xs font-semibold uppercase text-stone-500 mb-1">Session Name</label>
            <input
              type="text"
              required
              value={name}
              onChange={(e) => setName(e.target.value.toLowerCase().replace(/[^a-z0-9\-]/g, ''))}
              placeholder="e.g. dev-session-1"
              className="w-full px-3 py-2 border border-stone-200 rounded-lg text-sm font-mono"
            />
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase text-stone-500 mb-1">Target Agent</label>
            <select
              required
              value={selectedAgent}
              onChange={(e) => setSelectedAgent(e.target.value)}
              className="w-full px-3 py-2 border border-stone-200 rounded-lg text-sm bg-white"
            >
              <option value="">Select an Agent...</option>
              {(agentsData?.agents || []).map((ag) => (
                <option key={ag.name} value={ag.name}>
                  {ag.name} ({ag.engine || 'opencode'})
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase text-stone-500 mb-1">Engine Override (optional)</label>
            <select
              value={engine}
              onChange={(e) => setEngine(e.target.value)}
              className="w-full px-3 py-2 border border-stone-200 rounded-lg text-sm bg-white"
            >
              <option value="">Agent Default</option>
              <option value="opencode">OpenCode</option>
              <option value="agy">Antigravity (agy)</option>
              <option value="claude">Claude Code</option>
            </select>
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase text-stone-500 mb-1">Instructions File Path (optional)</label>
            <input
              type="text"
              value={instructionsFile}
              onChange={(e) => setInstructionsFile(e.target.value)}
              placeholder="/workspace/.agent-instructions.md"
              className="w-full px-3 py-2 border border-stone-200 rounded-lg text-sm font-mono"
            />
          </div>

          <div className="flex items-center gap-2 pt-2">
            <input
              type="checkbox"
              id="sandbox"
              checked={sandbox}
              onChange={(e) => setSandbox(e.target.checked)}
              className="rounded text-primary-600"
            />
            <label htmlFor="sandbox" className="text-sm text-stone-700">
              Run in isolated sandbox container environment
            </label>
          </div>

          <div className="pt-4 flex justify-end gap-3 border-t border-stone-100">
            <button
              type="button"
              onClick={() => navigate('/sessions')}
              className="px-4 py-2 text-xs font-medium text-stone-600 bg-stone-100 rounded-lg hover:bg-stone-200"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={!name || !selectedAgent || createMutation.isPending}
              className="px-4 py-2 text-xs font-medium text-white bg-primary-600 rounded-lg hover:bg-primary-700 disabled:opacity-50"
            >
              {createMutation.isPending ? 'Creating...' : 'Create Session'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default AgentSessionCreatePage;
