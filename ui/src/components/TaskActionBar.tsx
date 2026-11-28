import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '../api/client';
import ConfirmDialog from './ConfirmDialog';
import { useToast } from '../contexts/ToastContext';

interface TaskActionBarProps {
  namespace: string;
  name: string;
  agentName?: string;
  isCompleted?: boolean;
}

export default function TaskActionBar({ namespace, name, agentName, isCompleted }: TaskActionBarProps) {
  const queryClient = useQueryClient();
  const { addToast } = useToast();

  const [confirmAction, setConfirmAction] = useState<string | null>(null);
  const [promptText, setPromptText] = useState('');
  const [showPromptModal, setShowPromptModal] = useState<string | null>(null);

  // Fetch agent worker capabilities if agentName is present
  const { data: caps } = useQuery({
    queryKey: ['agentWorkerCaps', namespace, agentName],
    queryFn: () => (agentName ? api.getAgentWorkerCapabilities(namespace, agentName) : null),
    enabled: !!agentName,
  });

  const endpoints = caps?.endpoints || {};

  // Mutations
  const retryMut = useMutation({
    mutationFn: (p?: string) => api.retryTask(namespace, name, p),
    onSuccess: () => {
      addToast('Task Retried: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const killMut = useMutation({
    mutationFn: () => api.killTask(namespace, name),
    onSuccess: () => {
      addToast('Task Killed: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const commitMut = useMutation({
    mutationFn: (msg: string) => api.commitTask(namespace, name, msg),
    onSuccess: () => {
      addToast('Changes Committed: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const discardMut = useMutation({
    mutationFn: () => api.discardTask(namespace, name, []),
    onSuccess: () => {
      addToast('Changes Discarded: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const reviewMut = useMutation({
    mutationFn: () => api.reviewTask(namespace, name),
    onSuccess: () => {
      addToast('Review Triggered: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const interruptMut = useMutation({
    mutationFn: () => api.interruptTask(namespace, name),
    onSuccess: () => {
      addToast('Task Interrupted: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const compactMut = useMutation({
    mutationFn: () => api.compactTask(namespace, name),
    onSuccess: () => {
      addToast('Task Context Compacted: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const forkMut = useMutation({
    mutationFn: () => api.forkTask(namespace, name),
    onSuccess: (res) => {
      addToast(`Task Forked: New job id: ${res.job_id}`, 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const messageMut = useMutation({
    mutationFn: (text: string) => api.messageTask(namespace, name, text),
    onSuccess: () => {
      addToast('Message Sent: Action succeeded', 'success');
      queryClient.invalidateQueries({ queryKey: ['task', namespace, name] });
    },
    onError: (err: any) => addToast(`Action Failed: ${err.message}`, 'error'),
  });

  const isSupported = (endpoint: string) => {
    if (!caps) return true; // optimistic if not loaded yet
    return endpoints[endpoint] !== 'unsupported';
  };

  const isEmulated = (endpoint: string) => caps && endpoints[endpoint] === 'emulated';

  return (
    <div className="flex flex-wrap items-center gap-2 p-3 bg-stone-50 border border-stone-200 rounded-xl my-4">
      {/* Retry */}
      {isSupported('retry') && (
        <button
          onClick={() => setShowPromptModal('retry')}
          className="px-2.5 py-1 text-xs font-medium text-stone-700 bg-white border border-stone-200 rounded-lg hover:bg-stone-50 transition-colors shadow-sm"
        >
          Retry {isEmulated('retry') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Message */}
      {isSupported('message') && (
        <button
          onClick={() => setShowPromptModal('message')}
          className="px-2.5 py-1 text-xs font-medium text-stone-700 bg-white border border-stone-200 rounded-lg hover:bg-stone-50 transition-colors shadow-sm"
        >
          Message {isEmulated('message') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Interrupt */}
      {isSupported('interrupt') && !isCompleted && (
        <button
          onClick={() => interruptMut.mutate()}
          disabled={interruptMut.isPending}
          className="px-2.5 py-1 text-xs font-medium text-amber-700 bg-amber-50 border border-amber-200 rounded-lg hover:bg-amber-100 transition-colors"
        >
          Interrupt {isEmulated('interrupt') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Kill */}
      {isSupported('kill') && !isCompleted && (
        <button
          onClick={() => setConfirmAction('kill')}
          className="px-2.5 py-1 text-xs font-medium text-red-600 bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition-colors"
        >
          Kill
        </button>
      )}

      {/* Commit */}
      {isSupported('commit') && (
        <button
          onClick={() => setShowPromptModal('commit')}
          className="px-2.5 py-1 text-xs font-medium text-emerald-700 bg-emerald-50 border border-emerald-200 rounded-lg hover:bg-emerald-100 transition-colors"
        >
          Commit {isEmulated('commit') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Discard */}
      {isSupported('discard') && (
        <button
          onClick={() => setConfirmAction('discard')}
          className="px-2.5 py-1 text-xs font-medium text-rose-700 bg-rose-50 border border-rose-200 rounded-lg hover:bg-rose-100 transition-colors"
        >
          Discard
        </button>
      )}

      {/* Review */}
      {isSupported('review') && (
        <button
          onClick={() => reviewMut.mutate()}
          disabled={reviewMut.isPending}
          className="px-2.5 py-1 text-xs font-medium text-stone-700 bg-white border border-stone-200 rounded-lg hover:bg-stone-50 transition-colors shadow-sm"
        >
          Review {isEmulated('review') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Compact */}
      {isSupported('compact') && (
        <button
          onClick={() => compactMut.mutate()}
          disabled={compactMut.isPending}
          className="px-2.5 py-1 text-xs font-medium text-stone-700 bg-white border border-stone-200 rounded-lg hover:bg-stone-50 transition-colors shadow-sm"
        >
          Compact {isEmulated('compact') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Fork */}
      {isSupported('fork') && (
        <button
          onClick={() => forkMut.mutate()}
          disabled={forkMut.isPending}
          className="px-2.5 py-1 text-xs font-medium text-stone-700 bg-white border border-stone-200 rounded-lg hover:bg-stone-50 transition-colors shadow-sm"
        >
          Fork {isEmulated('fork') && <span className="text-[10px] text-amber-600">(emulated)</span>}
        </button>
      )}

      {/* Export download */}
      <a
        href={api.getTaskExportUrl(namespace, name)}
        download
        className="px-2.5 py-1 text-xs font-medium text-stone-700 bg-white border border-stone-200 rounded-lg hover:bg-stone-50 transition-colors shadow-sm"
      >
        Export tar.gz
      </a>

      {/* Prompt / text input modal */}
      {showPromptModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="bg-white rounded-xl shadow-xl max-w-md w-full p-5 space-y-4">
            <h3 className="font-semibold text-stone-900 text-sm capitalize">
              {showPromptModal === 'retry' && 'Retry with prompt'}
              {showPromptModal === 'message' && 'Send message to task'}
              {showPromptModal === 'commit' && 'Commit changes'}
            </h3>
            <textarea
              value={promptText}
              onChange={(e) => setPromptText(e.target.value)}
              placeholder="Enter text..."
              rows={4}
              className="w-full text-xs font-mono p-2 border border-stone-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-primary-500"
            />
            <div className="flex justify-end gap-2">
              <button
                onClick={() => {
                  setShowPromptModal(null);
                  setPromptText('');
                }}
                className="px-3 py-1.5 text-xs font-medium text-stone-600 bg-stone-100 rounded-lg hover:bg-stone-200"
              >
                Cancel
              </button>
              <button
                onClick={() => {
                  if (showPromptModal === 'retry') retryMut.mutate(promptText);
                  if (showPromptModal === 'message') messageMut.mutate(promptText);
                  if (showPromptModal === 'commit') commitMut.mutate(promptText);
                  setShowPromptModal(null);
                  setPromptText('');
                }}
                className="px-3 py-1.5 text-xs font-medium text-white bg-primary-600 rounded-lg hover:bg-primary-700"
              >
                Submit
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Destructive confirm dialogs */}
      <ConfirmDialog
        open={confirmAction === 'kill'}
        title="Kill Task"
        message="Are you sure you want to kill this running job?"
        confirmLabel="Kill"
        variant="danger"
        onConfirm={() => {
          killMut.mutate();
          setConfirmAction(null);
        }}
        onCancel={() => setConfirmAction(null)}
      />

      <ConfirmDialog
        open={confirmAction === 'discard'}
        title="Discard Changes"
        message="Are you sure you want to discard all uncommitted worktree changes?"
        confirmLabel="Discard"
        variant="danger"
        onConfirm={() => {
          discardMut.mutate();
          setConfirmAction(null);
        }}
        onCancel={() => setConfirmAction(null)}
      />
    </div>
  );
}
