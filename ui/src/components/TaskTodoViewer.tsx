import React from 'react';
import { useQuery } from '@tanstack/react-query';
import api from '../api/client';

interface TaskTodoViewerProps {
  namespace: string;
  name: string;
}

export default function TaskTodoViewer({ namespace, name }: TaskTodoViewerProps) {
  const { data: todos, isLoading } = useQuery({
    queryKey: ['taskTodo', namespace, name],
    queryFn: () => api.getTaskTodo(namespace, name),
  });

  return (
    <div className="bg-white border border-stone-200 rounded-xl p-4">
      <h4 className="text-xs font-semibold text-stone-600 uppercase tracking-wider mb-3">Agent Task Checklist</h4>
      {isLoading && <div className="text-xs text-stone-400">Loading checklist...</div>}
      {todos && todos.length === 0 && <div className="text-xs text-stone-400">No items reported.</div>}
      <ul className="space-y-2">
        {todos?.map((item, idx) => (
          <li key={idx} className="flex items-start gap-2.5 text-xs text-stone-800 bg-stone-50 p-2.5 rounded-lg border border-stone-100">
            <span
              className={`mt-0.5 px-1.5 py-0.5 text-[10px] rounded font-medium border ${
                item.status === 'completed' || item.status === 'done'
                  ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                  : item.status === 'in_progress'
                  ? 'bg-blue-50 text-blue-700 border-blue-200'
                  : 'bg-stone-100 text-stone-600 border-stone-200'
              }`}
            >
              {item.status}
            </span>
            <span className="flex-1 font-body">{item.content}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
