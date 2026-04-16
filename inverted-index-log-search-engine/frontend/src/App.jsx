import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Activity } from 'lucide-react';
import SearchBar from './components/SearchBar';
import SearchResults from './components/SearchResults';
import StatsPanel from './components/StatsPanel';

export default function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [stats, setStats] = useState(null);
  const [suggestions, setSuggestions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchTime, setSearchTime] = useState(0);
  const [totalResults, setTotalResults] = useState(0);

  const abortControllerRef = useRef(null);
  const debounceTimerRef = useRef(null);

  // Fetch stats on mount
  useEffect(() => {
    fetch('/api/stats')
      .then((res) => res.json())
      .then((data) => setStats(data))
      .catch((err) => console.error('Failed to fetch stats:', err));
  }, []);

  const performSearch = useCallback((searchQuery) => {
    // Cancel any in-flight request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    if (!searchQuery.trim()) {
      setResults([]);
      setSuggestions([]);
      setTotalResults(0);
      setSearchTime(0);
      setLoading(false);
      return;
    }

    const controller = new AbortController();
    abortControllerRef.current = controller;
    setLoading(true);

    const searchPromise = fetch(
      `/api/search?q=${encodeURIComponent(searchQuery)}&limit=100`,
      { signal: controller.signal }
    )
      .then((res) => res.json())
      .then((data) => {
        setResults(data.results || []);
        setTotalResults(data.total_results || 0);
        setSearchTime(data.search_time_ms || 0);
      });

    const suggestionsPromise =
      searchQuery.trim().length >= 2
        ? fetch(
            `/api/suggestions?prefix=${encodeURIComponent(searchQuery)}&limit=8`,
            { signal: controller.signal }
          )
            .then((res) => res.json())
            .then((data) => setSuggestions(data.suggestions || []))
        : Promise.resolve(setSuggestions([]));

    Promise.all([searchPromise, suggestionsPromise])
      .catch((err) => {
        if (err.name !== 'AbortError') {
          console.error('Search failed:', err);
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
  }, []);

  // Debounced search when query changes
  useEffect(() => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
    }

    debounceTimerRef.current = setTimeout(() => {
      performSearch(query);
    }, 250);

    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, [query, performSearch]);

  const handleSuggestionClick = (suggestion) => {
    setQuery(suggestion);
    setSuggestions([]);
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-content">
          <Activity className="header-icon" size={32} />
          <h1>Log Search Engine</h1>
        </div>
        <p className="header-subtitle">
          High-performance full-text search for log entries
        </p>
      </header>

      <main className="app-main">
        <StatsPanel stats={stats} />

        <SearchBar
          query={query}
          onQueryChange={setQuery}
          suggestions={suggestions}
          onSuggestionClick={handleSuggestionClick}
        />

        {loading && (
          <div className="loading-indicator">
            <div className="spinner" />
            <span>Searching...</span>
          </div>
        )}

        <SearchResults
          results={results}
          searchTime={searchTime}
          totalResults={totalResults}
          query={query}
        />
      </main>
    </div>
  );
}
