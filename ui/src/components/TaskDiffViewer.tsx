import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import api from '../api/client';

interface TaskDiffViewerProps {
  namespace: string;
  name: string;
}

export default function TaskDiffViewer({ namespace, name }: TaskDiffViewerProps) {
  const [showStat, setShowStat] = useState(false);

  const { data: diff, isLoading, error } = useQuery({
    queryKey: ['taskDiff', namespace, name, showStat],
    queryFn: () => api.getTaskDiff(namespace, name, showStat),
  });

  return (
    <div className="border border-stone-200 rounded-xl overflow-hidden bg-white">
      <div className="px-4 py-3 bg-stone-50 border-b border-stone-200 flex items-center justify-between">
        <h4 className="text-xs font-semibold text-stone-600 uppercase tracking-wider">Worktree Changes (Diff)</h4>
        <label className="flex items-center gap-1.5 text-xs text-stone-600 cursor-pointer">
          <input
            type="checkbox"
            checked={showStat}
            onChange={(e) => setShowStat(e.target.checked)}
            className="rounded border-stone-300 text-primary-600 focus:ring-primary-500"
          />
          Stat summary only
        </label>
      </div>

      <div className="bg-stone-900 text-stone-100 p-4 font-mono text-xs overflow-x-auto max-h-[500px]">
        {isLoading && <div className="text-stone-500">Loading diff...</div>}
        {error && <div className="text-rose-400">Error loading diff: {(error as Error).message}</div>}
        {!isLoading && !error && !diff && <div className="text-stone-500">No changes detected in worktree.</div>}
        {!isLoading && !error && diff && (
          <pre className="whitespace-pre">{diff}</pre>
        )}
      </div>
    </div>
  );
}
