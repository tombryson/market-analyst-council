import { useCallback, useEffect, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { api } from '../api';
import './PortfolioPositioningLab.css';

function getInitialRunId() {
  if (typeof window === 'undefined') return '';
  return new URLSearchParams(window.location.search).get('run_id') || '';
}

function navigateToRun(runId) {
  if (typeof window === 'undefined') return;
  const url = new URL(window.location.href);
  if (runId) {
    url.searchParams.set('run_id', runId);
  } else {
    url.searchParams.delete('run_id');
  }
  window.history.replaceState({}, '', `${url.pathname}${url.search}`);
}

function formatPct(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 'n/a';
  return `${num.toFixed(Math.abs(num) >= 10 ? 0 : 1)}%`;
}

function formatDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'n/a';
  try {
    return new Intl.DateTimeFormat(undefined, {
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    }).format(new Date(raw));
  } catch {
    return raw.slice(0, 16);
  }
}

function sentence(value, fallback = 'Not assessed') {
  const text = String(value || '').trim();
  return text || fallback;
}

function cleanRunLabel(value) {
  const text = String(value || '').trim();
  if (!text) return 'Portfolio memo';
  return text
    .replace(/^portfolio_positioning(?:_job)?/i, 'Portfolio memo')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function listFrom(value) {
  return Array.isArray(value) ? value.filter((item) => String(item || '').trim()) : [];
}

function toneForAction(action) {
  const normalized = String(action || '').trim().toLowerCase();
  if (normalized === 'add') return 'add';
  if (normalized === 'trim') return 'trim';
  if (normalized === 'review') return 'review';
  return 'hold';
}

function StatCard({ label, value, detail }) {
  return (
    <div className="portfolio-stat-card" role="listitem">
      <div className="portfolio-stat-label">{label}</div>
      <div className="portfolio-stat-value">{value}</div>
      {detail ? <div className="portfolio-stat-detail">{detail}</div> : null}
    </div>
  );
}

function AllocationTable({ rows }) {
  const targets = Array.isArray(rows) ? rows : [];
  const totalTarget = targets.reduce((sum, row) => {
    const value = Number(row?.target_pct);
    return sum + (Number.isFinite(value) && value > 0 ? value : 0);
  }, 0);
  return (
    <section className="portfolio-panel allocation-panel">
      <div className="portfolio-section-head">
        <div>
          <h2>Asset Class Targets</h2>
          <p>Target allocation grid. Rationales are separated below so the numbers stay readable.</p>
        </div>
      </div>

      {targets.length && totalTarget > 0 ? (
        <div className="portfolio-target-bar" aria-label="Target allocation stacked bar">
          {targets.map((row, idx) => {
            const target = Number(row?.target_pct);
            const width = Number.isFinite(target) && target > 0 ? (target / totalTarget) * 100 : 0;
            if (width <= 0) return null;
            return (
              <div
                className="portfolio-target-bar-segment"
                key={`${row?.asset_class || idx}-segment`}
                style={{
                  width: `${width}%`,
                  '--segment-index': idx,
                }}
                title={`${sentence(row?.display_name || row?.asset_class, 'Unnamed sleeve')}: ${formatPct(row?.target_pct)}`}
              >
                {width >= 7 ? <span>{sentence(row?.asset_class || row?.display_name, '')}</span> : null}
              </div>
            );
          })}
        </div>
      ) : null}

      <div className="portfolio-allocation-grid" role="table" aria-label="Asset class target allocation">
        <div className="portfolio-allocation-grid-row header" role="row">
          <span role="columnheader">Asset Class</span>
          <span role="columnheader">Current</span>
          <span role="columnheader">Range</span>
          <span role="columnheader">Target</span>
          <span role="columnheader">Direction</span>
          <span role="columnheader">Conviction</span>
        </div>
        {targets.length ? targets.map((row, idx) => {
          const action = String(row?.action || 'HOLD').toUpperCase();
          const conviction = String(row?.conviction || row?.implementation_priority || '').trim().toUpperCase() || 'N/A';
          return (
            <div className="portfolio-allocation-grid-row" role="row" key={`${row?.asset_class || idx}`}>
              <span role="cell">
                <strong>{sentence(row?.display_name || row?.asset_class, 'Unnamed sleeve')}</strong>
                <em>{sentence(row?.thesis_role, 'role not stated')}</em>
              </span>
              <span role="cell">{formatPct(row?.current_pct)}</span>
              <span role="cell">{formatPct(row?.min_pct)} - {formatPct(row?.max_pct)}</span>
              <span role="cell">{formatPct(row?.target_pct)}</span>
              <span role="cell"><mark className={`portfolio-action ${toneForAction(action)}`}>{action}</mark></span>
              <span role="cell">{conviction}</span>
            </div>
          );
        }) : (
          <div className="portfolio-empty-row">No asset-class target rows were saved in this memo.</div>
        )}
      </div>

      {targets.length ? (
        <div className="portfolio-allocation-rationales">
          <h3>Allocation Rationale</h3>
          {targets.map((row, idx) => {
            const action = String(row?.action || 'HOLD').toUpperCase();
            const conviction = String(row?.conviction || row?.implementation_priority || '').trim().toUpperCase() || 'N/A';
            return (
              <p key={`${row?.asset_class || idx}-rationale`}>
                <strong>{sentence(row?.display_name || row?.asset_class, 'Unnamed sleeve')}</strong>
                <span> ({sentence(row?.thesis_role, 'role not stated')}) - {action} / {conviction}: </span>
                {sentence(row?.rationale, 'No rationale supplied.')}
              </p>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}

function RunList({ runs, selectedRunId, onSelect, onRefresh, isLoading }) {
  return (
    <aside className="portfolio-run-list">
      <div className="portfolio-run-list-head">
        <div>
          <h2>Saved Memos</h2>
          <p>{runs.length} saved runs</p>
        </div>
        <button type="button" onClick={onRefresh} disabled={isLoading}>Refresh</button>
      </div>
      <div className="portfolio-run-scroll">
        {runs.length ? runs.map((run) => (
          <button
            type="button"
            key={run.id}
            className={`portfolio-run-card ${run.id === selectedRunId ? 'active' : ''}`}
            onClick={() => onSelect(run.id)}
          >
            <strong>{cleanRunLabel(run.label)}</strong>
            <span>{sentence(run.mode, 'mode n/a')} · {formatDate(run.updated_at)}</span>
            <small>{run.id}</small>
          </button>
        )) : (
          <div className="portfolio-empty-list">No portfolio positioning memos found yet.</div>
        )}
      </div>
    </aside>
  );
}

export default function PortfolioPositioningLab() {
  const [runs, setRuns] = useState([]);
  const [selectedRunId, setSelectedRunId] = useState(getInitialRunId);
  const [payload, setPayload] = useState(null);
  const [isLoadingRuns, setIsLoadingRuns] = useState(false);
  const [isLoadingRun, setIsLoadingRun] = useState(false);
  const [error, setError] = useState('');

  const loadRuns = useCallback(async () => {
    setIsLoadingRuns(true);
    setError('');
    try {
      const response = await api.listPortfolioPositioningRuns(50);
      const nextRuns = Array.isArray(response?.runs) ? response.runs : [];
      setRuns(nextRuns);
      if (!selectedRunId && nextRuns[0]?.id) {
        setSelectedRunId(nextRuns[0].id);
        navigateToRun(nextRuns[0].id);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load portfolio memo list');
    } finally {
      setIsLoadingRuns(false);
    }
  }, [selectedRunId]);

  const loadRun = useCallback(async (runId) => {
    if (!runId) {
      setPayload(null);
      return;
    }
    setIsLoadingRun(true);
    setError('');
    try {
      const response = await api.getPortfolioPositioningRun(runId);
      setPayload(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load portfolio memo');
      setPayload(null);
    } finally {
      setIsLoadingRun(false);
    }
  }, []);

  useEffect(() => {
    loadRuns();
  }, [loadRuns]);

  useEffect(() => {
    loadRun(selectedRunId);
  }, [selectedRunId, loadRun]);

  const structured = payload?.structured_data || {};
  const diagnosis = structured.portfolio_diagnosis || {};
  const strategic = structured.strategic_view || {};
  const macro = structured.macro_scorecard || {};
  const memoMarkdown = payload?.memo_markdown || payload?.chairman_memo_markdown || payload?.analyst_memo_markdown || '';

  const marketSummaryParts = [];
  if (macro.growth) marketSummaryParts.push(`Growth: ${macro.growth}`);
  if (macro.inflation) marketSummaryParts.push(`Inflation: ${macro.inflation}`);
  if (macro.rates) marketSummaryParts.push(`Rates: ${macro.rates}`);
  if (macro.liquidity) marketSummaryParts.push(`Liquidity: ${macro.liquidity}`);
  const marketSummary = marketSummaryParts.join(' · ');

  const handleSelectRun = (runId) => {
    setSelectedRunId(runId);
    navigateToRun(runId);
  };

  const handleDelete = async () => {
    if (!selectedRunId) return;
    const ok = window.confirm('Delete this portfolio positioning memo?');
    if (!ok) return;
    try {
      await api.deletePortfolioPositioningRun(selectedRunId);
      setPayload(null);
      setSelectedRunId('');
      navigateToRun('');
      await loadRuns();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete portfolio memo');
    }
  };

  return (
    <div className="portfolio-lab-root">
      <header className="portfolio-lab-header">
        <div>
          <p className="portfolio-kicker">LLM Council</p>
          <h1>Portfolio Memos</h1>
          <p>Portfolio-level allocation reports, separate from single-stock analysis.</p>
        </div>
        <div className="portfolio-header-actions">
          <button type="button" onClick={() => window.history.back()}>Back</button>
          <button type="button" onClick={handleDelete} disabled={!selectedRunId}>Delete Memo</button>
        </div>
      </header>

      {error ? <div className="portfolio-error">{error}</div> : null}

      <main className="portfolio-lab-shell">
        <RunList
          runs={runs}
          selectedRunId={selectedRunId}
          onSelect={handleSelectRun}
          onRefresh={loadRuns}
          isLoading={isLoadingRuns}
        />

        <section className="portfolio-workspace">
          {isLoadingRun ? <div className="portfolio-loading">Loading selected memo...</div> : null}
          {!payload && !isLoadingRun ? (
            <div className="portfolio-empty-state">
              <h2>No portfolio memo selected</h2>
              <p>Select a saved portfolio positioning memo from the list.</p>
            </div>
          ) : null}

          {payload ? (
            <>
              <section className="portfolio-hero-card">
                <div>
                  <p className="portfolio-kicker">{sentence(structured.mode, 'mode n/a')} · {formatDate(payload.updated_at)}</p>
                  <h2>{cleanRunLabel(payload.label)}</h2>
                  <p>{sentence(structured.executive_summary, 'No executive summary was saved in this memo.')}</p>
                </div>
                <div className="portfolio-hero-meta" role="list" aria-label="Portfolio memo key metrics">
                  <StatCard label="Cash Now" value={formatPct(diagnosis.current_cash_pct)} detail="portfolio snapshot" />
                  <StatCard label="Cash Target" value={formatPct(strategic.cash_target_pct)} detail={sentence(strategic.cash_role, 'target role')} />
                  <StatCard label="Primary Theme" value={sentence(strategic.primary_theme)} detail={sentence(strategic.secondary_theme, 'secondary theme n/a')} />
                </div>
              </section>

              <div className="portfolio-grid two">
                <section className="portfolio-panel">
                  <h3>Current Shape</h3>
                  <p>{sentence(diagnosis.current_structure, 'No current-structure summary was saved.')}</p>
                  <div className="portfolio-chip-row">
                    {listFrom(diagnosis.dominant_asset_classes).slice(0, 6).map((item) => (
                      <span key={item}>{item}</span>
                    ))}
                  </div>
                </section>
                <section className="portfolio-panel">
                  <h3>Strategic View</h3>
                  <p>{sentence(strategic.notes?.[0] || structured.confidence_note, 'No strategic note was saved.')}</p>
                  {marketSummary ? <p className="portfolio-muted">{marketSummary}</p> : null}
                </section>
              </div>

              <AllocationTable rows={structured.asset_class_targets} />

              <section className="portfolio-panel memo-panel">
                <div className="portfolio-section-head">
                  <div>
                    <h2>Memo Text</h2>
                    <p>Full generated memo.</p>
                  </div>
                </div>
                {memoMarkdown ? <ReactMarkdown>{memoMarkdown}</ReactMarkdown> : <p className="portfolio-muted">No memo markdown was saved.</p>}
              </section>
            </>
          ) : null}
        </section>
      </main>
    </div>
  );
}
