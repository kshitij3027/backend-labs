import React from 'react';
import { Clock, Tag } from 'lucide-react';

function formatTimestamp(timestamp) {
  const date = new Date(timestamp * 1000);
  return date.toLocaleString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function getLevelClass(level) {
  switch (level?.toUpperCase()) {
    case 'ERROR':
      return 'level-error';
    case 'WARN':
    case 'WARNING':
      return 'level-warn';
    case 'INFO':
      return 'level-info';
    case 'DEBUG':
      return 'level-debug';
    default:
      return 'level-default';
  }
}

export default function SearchResults({ results, searchTime, totalResults, query }) {
  if (!query.trim()) {
    return (
      <div className="results-placeholder">
        <p>Enter a search query to find log entries</p>
      </div>
    );
  }

  if (query.trim() && results.length === 0) {
    return (
      <div className="results-empty">
        <p>No results found for "{query}"</p>
      </div>
    );
  }

  return (
    <div className="search-results">
      <div className="results-header">
        <span className="results-count">
          {totalResults} result{totalResults !== 1 ? 's' : ''} in {searchTime.toFixed(1)} ms
        </span>
      </div>
      <div className="results-list">
        {results.map((result) => (
          <div key={result.doc_id} className="result-card">
            <div className="result-message">
              <div
                dangerouslySetInnerHTML={{ __html: result.highlighted_message }}
              />
            </div>
            <div className="result-meta">
              <span className={`badge level-badge ${getLevelClass(result.level)}`}>
                {result.level}
              </span>
              <span className="badge service-badge">
                <Tag size={12} />
                {result.service}
              </span>
              <span className="result-timestamp">
                <Clock size={12} />
                {formatTimestamp(result.timestamp)}
              </span>
              <span className="result-score">
                Score: {result.score.toFixed(3)}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
