import React from 'react';
import { FileText, Hash, BarChart2 } from 'lucide-react';

export default function StatsPanel({ stats }) {
  if (!stats) {
    return (
      <div className="stats-panel">
        <div className="stat-card loading-stat">Loading stats...</div>
      </div>
    );
  }

  return (
    <div className="stats-panel">
      <div className="stat-card">
        <FileText className="stat-icon" size={24} />
        <div className="stat-content">
          <span className="stat-value">{stats.total_documents.toLocaleString()}</span>
          <span className="stat-label">Total Documents</span>
        </div>
      </div>
      <div className="stat-card">
        <Hash className="stat-icon" size={24} />
        <div className="stat-content">
          <span className="stat-value">{stats.total_terms.toLocaleString()}</span>
          <span className="stat-label">Total Terms</span>
        </div>
      </div>
      <div className="stat-card">
        <BarChart2 className="stat-icon" size={24} />
        <div className="stat-content">
          <span className="stat-value">{stats.avg_terms_per_doc.toFixed(1)}</span>
          <span className="stat-label">Avg Terms/Doc</span>
        </div>
      </div>
    </div>
  );
}
