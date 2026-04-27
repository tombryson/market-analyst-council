import { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api';
import './GanttIntelligenceLab.css';

function navigateTo(pathname) {
  if (typeof window === 'undefined') return;
  const current = window.location.pathname;
  if (current === pathname) return;
  window.history.pushState({}, '', pathname);
  window.dispatchEvent(new Event('app:navigate'));
}

function openAnnouncementRouter() {
  navigateTo('/announcement-router');
}

function titleizeKey(key) {
  return String(key || '')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function fmtPct(value) {
  if (value == null || value === '') return 'n/a';
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  const pct = n <= 1 ? n * 100 : n;
  return `${Math.round(pct)}%`;
}

function fmtNum(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  return n.toFixed(digits);
}

function fmtMs(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 'n/a';
  if (n < 1000) return `${Math.round(n)}ms`;
  return `${(n / 1000).toFixed(2)}s`;
}

function parseIsoDateOrNull(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const dt = new Date(raw);
  if (!Number.isFinite(dt.getTime())) return null;
  return dt;
}

function fmtRelativeSince(value) {
  const dt = parseIsoDateOrNull(value);
  if (!dt) return 'n/a';
  const ms = Date.now() - dt.getTime();
  if (!Number.isFinite(ms) || ms < 0) return 'just now';
  const mins = Math.floor(ms / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function labelScenarioPath(value) {
  const path = String(value || '').trim().toLowerCase();
  if (path === 'bull') return 'Bull case';
  if (path === 'base') return 'Base case';
  if (path === 'bear') return 'Bear case';
  if (path === 'mixed') return 'Mixed / unclear';
  return 'Not assessed';
}

function labelScenarioAction(value) {
  const action = String(value || '').trim().toLowerCase();
  const labels = {
    ignore: 'No action',
    watch: 'Watch only',
    annotate_run: 'Attach note to run',
    run_delta_only: 'Run delta check',
    rerun_stage1: 'Refresh evidence only',
    full_rerun: 'Full rerun recommended',
    urgent_human_review: 'Urgent human review',
  };
  return labels[action] || (action ? titleizeKey(action) : 'Not assessed');
}

function labelScenarioTransition(value) {
  const transition = String(value || '').trim().toLowerCase();
  if (!transition || !transition.includes('->')) return 'No scenario change';
  const [from, to] = transition.split('->').map((part) => labelScenarioPath(part));
  return `${from} to ${to}`;
}

function scenarioTone(name) {
  const k = String(name || '').toLowerCase();
  if (k === 'bull') return 'bull';
  if (k === 'bear') return 'bear';
  return 'base';
}

function topCountEntries(counts, limit = 4) {
  return Object.entries(counts || {})
    .filter(([, value]) => Number(value) > 0)
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .slice(0, limit);
}

function hasRouterDecision(router) {
  return Boolean(
    router &&
    typeof router === 'object' &&
    (router.action || router.current_path || router.announcement_title || router.title || router.source_url)
  );
}

function routerDetailItems(router, detailKey, fallbackKey) {
  const details = router?.[detailKey];
  if (Array.isArray(details) && details.length) return details;
  const fallback = router?.[fallbackKey];
  return Array.isArray(fallback) ? fallback : [];
}

function routerItemLabel(item) {
  if (item && typeof item === 'object') return String(item.label || item.summary || item.condition_id || '').trim();
  return String(item || '').trim();
}

function routerItemMeta(item) {
  if (!item || typeof item !== 'object') return '';
  const parts = [];
  if (item.scenario) parts.push(labelScenarioPath(item.scenario));
  if (item.group) parts.push(titleizeKey(item.group));
  if (item.status) parts.push(titleizeKey(item.status));
  if (item.reason) parts.push(String(item.reason));
  return parts.filter(Boolean).join(' | ');
}

function formatRouterMarketCondition(item) {
  if (!item || typeof item !== 'object') return '';
  const observed = typeof item.observed_value === 'number'
    ? fmtNum(item.observed_value, item.observed_value >= 100 ? 0 : 2)
    : String(item.observed_value ?? 'n/a');
  const threshold = typeof item.threshold_value === 'number'
    ? fmtNum(item.threshold_value, item.threshold_value >= 100 ? 0 : 2)
    : String(item.threshold_value ?? 'n/a');
  const field = item.field || item.market_field || 'market fact';
  return `${titleizeKey(field)} observed ${observed}; condition ${item.comparator || '?'} ${threshold} was ${item.status || 'checked'}.`;
}

function routerTitle(router) {
  return String(router?.announcement_title || router?.title || 'Untitled announcement').trim();
}

function routerReason(router) {
  return String(router?.reason || router?.action_reason || '').trim();
}

function explainRouterDecision(router) {
  if (!hasRouterDecision(router)) return '';
  const action = String(router.action || '').trim().toLowerCase();
  const transition = String(router.path_transition || '').trim();
  const matchedCount = routerDetailItems(router, 'matched_condition_details', 'matched_conditions').length;
  const watchCount = routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist').length;
  const marketCount = Array.isArray(router.market_context_conditions)
    ? router.market_context_conditions.length
    : Object.keys(router.market_facts_used || {}).length;

  if (!matchedCount && !watchCount && marketCount) {
    return 'The filing did not directly match the thesis map. Market facts were checked as backdrop only.';
  }
  if (!matchedCount && !watchCount && action === 'ignore') {
    return 'The filing was resolved to a primary source and did not change the saved thesis path.';
  }
  if (transition) {
    return `The filing maps to ${labelScenarioTransition(transition)}. ${labelScenarioAction(action)}.`;
  }
  if (matchedCount || watchCount) {
    return `The filing hit ${matchedCount + watchCount} monitored condition(s), but did not move the thesis path.`;
  }
  return routerReason(router);
}

function metricLabel(value, fallback = 'n/a') {
  const text = String(value || '').trim();
  return text || fallback;
}

function DetailRow({ label, value, tone = '' }) {
  return (
    <div className="scenario-router-detail-row">
      <span>{label}</span>
      <strong className={tone ? `tone-${tone}` : ''}>{value}</strong>
    </div>
  );
}

function DetailList({ title, items, conflict = false, market = false }) {
  if (!items.length) return null;
  return (
    <>
      <h4>{title}</h4>
      <div className="scenario-router-detail-list">
        {items.map((item, idx) => (
          <div key={`${title}-${idx}`} className={`scenario-router-detail-item ${conflict ? 'is-conflict' : ''}`}>
            <strong>{routerItemLabel(item)}</strong>
            <span>{market ? formatRouterMarketCondition(item) : routerItemMeta(item)}</span>
          </div>
        ))}
      </div>
    </>
  );
}

function PipelineTrace({ router }) {
  const trace = Array.isArray(router?.processing_trace) ? router.processing_trace : [];
  if (!trace.length) {
    return (
      <div className="announcement-router-flow">
        {['Email trigger received', 'Official filing resolved', 'Filing facts extracted', 'Thesis map checked', 'Decision saved'].map((label) => (
          <div key={label} className="announcement-router-flow-step is-muted">
            <span />
            <strong>{label}</strong>
          </div>
        ))}
      </div>
    );
  }
  return (
    <div className="announcement-router-flow">
      {trace.map((step, idx) => (
        <div key={`${step.stage || 'stage'}-${idx}`} className={`announcement-router-flow-step is-${step.outcome || 'ok'}`}>
          <span />
          <strong>{titleizeKey(step.stage || `Stage ${idx + 1}`)}</strong>
          <em>{step.outcome || 'ok'} | {fmtMs(step.duration_ms)}</em>
        </div>
      ))}
    </div>
  );
}

function DecisionPanel({ router, emptyTitle = 'No announcement decision attached', emptyCopy = '' }) {
  const decisionAvailable = hasRouterDecision(router);
  const matchedConditions = routerDetailItems(router, 'matched_condition_details', 'matched_conditions');
  const watchHits = routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist');
  const findings = Array.isArray(router?.key_findings) ? router.key_findings : [];
  const conflicts = Array.isArray(router?.conflicts_with_run) ? router.conflicts_with_run : [];
  const marketConditions = Array.isArray(router?.market_context_conditions) ? router.market_context_conditions : [];
  const followUps = Array.isArray(router?.follow_up_steps) ? router.follow_up_steps : [];

  if (!decisionAvailable) {
    return (
      <article className="scenario-router-column announcement-router-empty-panel">
        <h4>{emptyTitle}</h4>
        <p>{emptyCopy || 'This run has no saved announcement-router artifact. The lab view will not show unrelated global router events.'}</p>
      </article>
    );
  }

  return (
    <article className="scenario-router-column announcement-router-decision-panel">
      <h4>Selected Filing Decision</h4>
      <DetailRow label="Announcement" value={routerTitle(router)} />
      <DetailRow label="Source" value={router?.source_type ? titleizeKey(router.source_type) : 'Unknown'} />
      <DetailRow label="Current thesis path" value={labelScenarioPath(router?.current_path)} tone={scenarioTone(router?.current_path)} />
      <DetailRow label="Scenario movement" value={labelScenarioTransition(router?.path_transition)} />
      <DetailRow label="Recommended action" value={labelScenarioAction(router?.action)} />
      <DetailRow label="Materiality" value={router?.impact_level ? titleizeKey(router.impact_level) : 'Not assessed'} />
      <DetailRow label="Last evaluated" value={router?.saved_at_utc ? fmtRelativeSince(router.saved_at_utc) : 'n/a'} />
      {explainRouterDecision(router) && <div className="scenario-router-detail-note">{explainRouterDecision(router)}</div>}
      {routerReason(router) && <div className="scenario-router-detail-note"><strong>Decision reason:</strong> {routerReason(router)}</div>}
      {router?.source_url && (
        <div className="scenario-router-detail-note">
          <strong>Primary source:</strong> <a href={router.source_url} target="_blank" rel="noreferrer">Open filing</a>
        </div>
      )}
      <DetailList title="Matched Thesis Conditions" items={matchedConditions} />
      <DetailList title="Watchlist Hits" items={watchHits} />
      <DetailList title="Conflicts With Saved Run" items={conflicts} conflict />
      <DetailList title="Supporting Findings" items={findings} />
      <DetailList title="Market Conditions Checked" items={marketConditions} market />
      {!!Object.keys(router?.market_facts_used || {}).length && (
        <>
          <h4>Market Facts Used</h4>
          <div className="scenario-router-chip-list">
            {Object.entries(router.market_facts_used || {}).map(([key, value]) => (
              <span key={`market-${key}`} className="scenario-router-chip">
                {titleizeKey(key)} | {typeof value === 'number' ? fmtNum(value, value >= 100 ? 0 : 2) : String(value)}
              </span>
            ))}
          </div>
        </>
      )}
      {!!followUps.length && (
        <>
          <h4>Next Steps</h4>
          <div className="scenario-router-detail-list">
            {followUps.map((item, idx) => (
              <div key={`follow-up-${idx}`} className="scenario-router-detail-item">
                <strong>{item}</strong>
              </div>
            ))}
          </div>
        </>
      )}
    </article>
  );
}

export default function AnnouncementRouterMonitor({
  embedded = false,
  runRouter = null,
  selectedRunId = '',
  selectedTicker = '',
  onOpenFullMonitor = null,
}) {
  const [overview, setOverview] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [tickerFilter, setTickerFilter] = useState('');
  const [reloadToken, setReloadToken] = useState(0);
  const [selectedEventId, setSelectedEventId] = useState('');

  const effectiveTicker = useMemo(
    () => String(tickerFilter || selectedTicker || '').trim().toUpperCase(),
    [tickerFilter, selectedTicker]
  );

  useEffect(() => {
    if (embedded) return undefined;
    let cancelled = false;
    const loadMonitor = async () => {
      try {
        setLoading(true);
        setError('');
        const payload = await api.getAnnouncementRouterOverview(120, effectiveTicker);
        if (!cancelled) setOverview(payload || null);
      } catch (err) {
        if (!cancelled) setError(err?.message || 'Failed to load announcement router monitor');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    loadMonitor();
    return () => {
      cancelled = true;
    };
  }, [embedded, effectiveTicker, reloadToken]);

  const recentEvents = Array.isArray(overview?.recent_events) ? overview.recent_events : [];
  const selectedEvent = recentEvents.find((row) => row?.event_id === selectedEventId) || recentEvents[0] || {};
  const selectedRouter = embedded ? (runRouter || {}) : selectedEvent;
  const actionCounts = topCountEntries(overview?.action_counts, 4);
  const statusCounts = topCountEntries(overview?.status_counts, 4);
  const transitionCounts = topCountEntries(overview?.path_transition_counts, 4);

  const openFullMonitor = useCallback(() => {
    if (onOpenFullMonitor) {
      onOpenFullMonitor();
      return;
    }
    openAnnouncementRouter();
  }, [onOpenFullMonitor]);

  if (embedded) {
    return (
      <section className="lab-panel scenario-router-monitor-panel announcement-router-embedded-panel">
        <div className="scenario-router-monitor-head">
          <div>
            <h3>Announcement Thesis Router</h3>
            <p className="scenario-router-monitor-copy">
              Run-specific announcement checks. Global router events are not mixed into this lab view.
            </p>
          </div>
          <div className="scenario-router-monitor-actions">
            <button type="button" className="gantt-lab-inline-retry" onClick={openFullMonitor}>
              Open Monitor
            </button>
          </div>
        </div>
        <div className="scenario-router-columns announcement-router-embedded-columns">
          <DecisionPanel
            router={selectedRouter}
            emptyTitle="No router decision for this run"
            emptyCopy={`No announcement-routing artifact is attached to ${selectedRunId || 'this selected run'}. This panel is intentionally blank rather than showing unrelated global events.`}
          />
          {hasRouterDecision(selectedRouter) && (
            <article className="scenario-router-column">
              <h4>Processing Trace</h4>
              <PipelineTrace router={selectedRouter} />
            </article>
          )}
        </div>
      </section>
    );
  }

  return (
    <main className="gantt-lab-root announcement-router-root">
      <div className="announcement-router-shell">
        <section className="lab-panel scenario-router-monitor-panel">
          <div className="scenario-router-monitor-head">
            <div>
              <h3>Announcement Thesis Router</h3>
              <p className="scenario-router-monitor-copy">
                Tracks ticker-coded announcement emails, resolves official filings, checks saved thesis-map conditions, and records the routing decision.
              </p>
            </div>
            <div className="scenario-router-monitor-actions">
              <input
                type="text"
                className="scenario-router-filter-input"
                placeholder="Filter by ticker, e.g. ASX:TOR"
                value={tickerFilter}
                onChange={(e) => {
                  setTickerFilter(String(e.target.value || '').toUpperCase());
                  setSelectedEventId('');
                }}
              />
              <button
                type="button"
                className="gantt-lab-inline-retry"
                onClick={() => setReloadToken((prev) => prev + 1)}
                disabled={loading}
              >
                {loading ? 'Refreshing...' : 'Refresh'}
              </button>
              <button type="button" className="gantt-lab-inline-retry" onClick={() => navigateTo('/gantt-lab')}>
                Open Timeline Lab
              </button>
            </div>
          </div>

          <div className="scenario-router-monitor-grid">
            <div className="scenario-router-card">
              <label>Total Routed Events</label>
              <strong>{overview?.total_events ?? 'n/a'}</strong>
              <span>{overview?.unique_tickers ?? 'n/a'} tickers tracked</span>
            </div>
            <div className="scenario-router-card">
              <label>Primary Source Coverage</label>
              <strong>{fmtPct(overview?.official_source_rate_pct)}</strong>
              <span>official filing resolved</span>
            </div>
            <div className="scenario-router-card">
              <label>Average Processing</label>
              <strong>{fmtMs(overview?.average_processing_ms)}</strong>
              <span>email to saved decision</span>
            </div>
            <div className="scenario-router-card">
              <label>Latest Status</label>
              <strong>{metricLabel(statusCounts[0]?.[0])}</strong>
              <span>{statusCounts[0]?.[1] || 0} event(s)</span>
            </div>
          </div>

          <div className="scenario-router-columns announcement-router-monitor-columns">
            <DecisionPanel router={selectedRouter} emptyTitle="No routed announcements yet" />

            <article className="scenario-router-column">
              <h4>How This Decision Was Produced</h4>
              <PipelineTrace router={selectedRouter} />
              <h4>Action Distribution</h4>
              <div className="scenario-router-chip-list">
                {actionCounts.map(([key, value]) => (
                  <span key={`action-${key}`} className="scenario-router-chip">{labelScenarioAction(key)} | {value}</span>
                ))}
                {!actionCounts.length && <span className="watch-empty">No actions yet.</span>}
              </div>
              <h4>Scenario Movement</h4>
              <div className="scenario-router-chip-list">
                {transitionCounts.map(([key, value]) => (
                  <span key={`transition-${key}`} className="scenario-router-chip">{labelScenarioTransition(key)} | {value}</span>
                ))}
                {!transitionCounts.length && <span className="watch-empty">No scenario movements yet.</span>}
              </div>
            </article>

            <article className="scenario-router-column scenario-router-column-wide">
              <h4>Recent Routed Announcements</h4>
              <div className="scenario-router-event-list">
                {recentEvents.slice(0, 12).map((row) => (
                  <button
                    type="button"
                    className={`scenario-router-event ${selectedEvent?.event_id === row.event_id ? 'is-selected' : ''}`}
                    key={row.event_id || `${row.ticker}-${row.saved_at_utc}`}
                    onClick={() => setSelectedEventId(row.event_id || '')}
                  >
                    <div className="scenario-router-event-top">
                      <strong>{row.ticker || 'n/a'}</strong>
                      <span>{row.status || 'ok'}</span>
                      <span>{labelScenarioAction(row.action)}</span>
                      <span className={`tone-${scenarioTone(row.current_path)}`}>{labelScenarioPath(row.current_path)}</span>
                    </div>
                    <div className="scenario-router-event-title">{row.title || 'Untitled announcement'}</div>
                    <div className="scenario-router-event-meta">
                      {labelScenarioTransition(row.path_transition)} | {row.source_type || 'unknown source'} | {fmtMs(row.processing_duration_ms)} | {row.saved_at_utc ? fmtRelativeSince(row.saved_at_utc) : 'n/a'}
                    </div>
                    {row.action_reason && <div className="scenario-router-event-reason">{row.action_reason}</div>}
                    {row.error_reason && <div className="scenario-router-detail-note">{row.error_reason}</div>}
                  </button>
                ))}
                {!recentEvents.length && <div className="watch-empty">No routed announcements yet.</div>}
              </div>
            </article>
          </div>

          {error && <div className="run-meta-note run-meta-note-error">Announcement router monitor error: {error}</div>}
        </section>
      </div>
    </main>
  );
}
