import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import api from '../api/client';

interface TaskFilesViewerProps {
  namespace: string;
  name: string;
}

export default function TaskFilesViewer({ namespace, name }: TaskFilesViewerProps) {
  const [selectedFile, setSelectedFile] = useState<string | null>(null);

  const { data: files, isLoading: filesLoading } = useQuery({
    queryKey: ['taskFiles', namespace, name],
    queryFn: () => api.getTaskFiles(namespace, name),
  });

  const { data: fileContent, isLoading: contentLoading } = useQuery({
    queryKey: ['taskCat', namespace, name, selectedFile],
    queryFn: () => (selectedFile ? api.getTaskCat(namespace, name, selectedFile) : null),
    enabled: !!selectedFile,
  });

  return (
    <div className="flex border border-stone-200 rounded-xl overflow-hidden bg-white h-[450px]">
      {/* File List */}
      <div className="w-1/3 border-r border-stone-200 overflow-y-auto p-3 bg-stone-50">
        <h4 className="text-xs font-semibold text-stone-500 uppercase tracking-wider mb-2">Worktree Files</h4>
        {filesLoading && <div className="text-xs text-stone-400">Loading files...</div>}
        {files && files.length === 0 && <div className="text-xs text-stone-400">No files found</div>}
        <ul className="space-y-1">
          {files?.map((file) => (
            <li key={file}>
              <button
                onClick={() => setSelectedFile(file)}
                className={`w-full text-left px-2 py-1.5 text-xs font-mono rounded-md truncate transition-colors ${
                  selectedFile === file
                    ? 'bg-primary-50 text-primary-700 font-semibold'
                    : 'text-stone-700 hover:bg-stone-200/60'
                }`}
                title={file}
              >
                {file}
              </button>
            </li>
          ))}
        </ul>
      </div>

      {/* File Viewer */}
      <div className="w-2/3 flex flex-col h-full bg-stone-900 text-stone-100 overflow-hidden">
        <div className="px-4 py-2 bg-stone-950 border-b border-stone-800 text-xs font-mono text-stone-400 flex items-center justify-between">
          <span>{selectedFile || 'Select a file to view'}</span>
        </div>
        <div className="flex-1 overflow-auto p-4 font-mono text-xs leading-relaxed">
          {contentLoading && <div className="text-stone-500">Loading content...</div>}
          {!selectedFile && <div className="text-stone-500">No file selected.</div>}
          {selectedFile && !contentLoading && (
            <pre className="whitespace-pre">{fileContent || '(empty file)'}</pre>
          )}
        </div>
      </div>
    </div>
  );
}
