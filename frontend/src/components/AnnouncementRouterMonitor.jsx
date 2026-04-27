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
    return 'No announcement-based thesis condition matched. The saved lab path below is the pre-existing lab view, not a fresh recommendation from this filing.';
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

function routerOutcomeLabel(router) {
  if (!hasRouterDecision(router)) return 'Not assessed';
  const transition = String(router.path_transition || '').trim();
  const matched = Number(router.matched_conditions_count || 0);
  const watched = Number(router.triggered_watchlist_count || 0);
  const action = String(router.action || '').trim().toLowerCase();
  if (transition) return labelScenarioTransition(transition);
  if (matched || watched) return `${matched + watched} thesis condition${matched + watched === 1 ? '' : 's'} hit`;
  if (action === 'ignore') return 'No thesis impact';
  if (action === 'watch') return 'Watch only';
  return labelScenarioAction(action);
}

function pathExplanation(router) {
  const path = String(router?.baseline_path || router?.current_path || '').trim();
  if (!path) return '';
  return `${labelScenarioPath(path)} is the saved path from the latest lab run. It only changes here when the announcement hits mapped bull/base/bear conditions.`;
}

function conditionTone(status, group = '') {
  const normalized = String(status || '').trim().toLowerCase();
  const normalizedGroup = String(group || '').trim().toLowerCase();
  if (normalized === 'matched' && ['failure', 'red_flag'].includes(normalizedGroup)) return 'bear';
  if (normalized === 'matched') return 'bull';
  if (normalized === 'contradicted') return 'bear';
  if (normalized === 'unclear') return 'base';
  return 'muted';
}

function conditionStatusLabel(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'not_matched') return 'Not hit';
  if (normalized === 'matched') return 'Hit';
  if (normalized === 'contradicted') return 'Contradicted';
  if (normalized === 'unclear') return 'Unclear';
  return normalized ? titleizeKey(normalized) : 'Monitor';
}

function scenarioTargetText(block) {
  const parts = [];
  if (block?.target_12m != null && block.target_12m !== '') parts.push(`12M ${block.target_12m}`);
  if (block?.target_24m != null && block.target_24m !== '') parts.push(`24M ${block.target_24m}`);
  if (block?.probability_pct != null && block.probability_pct !== '') parts.push(`Prob ${block.probability_pct}%`);
  return parts.join(' | ');
}

function countStatuses(items) {
  const rows = Array.isArray(items) ? items : [];
  return rows.reduce(
    (acc, item) => {
      const status = String(item?.status || '').trim().toLowerCase() || 'unknown';
      acc.total += 1;
      acc[status] = (acc[status] || 0) + 1;
      return acc;
    },
    { total: 0, matched: 0, not_matched: 0, contradicted: 0, unclear: 0, unknown: 0 }
  );
}

function compactStatusSummary(items, empty = 'none checked') {
  const counts = countStatuses(items);
  if (!counts.total) return empty;
  const parts = [
    counts.matched && `${counts.matched} hit`,
    counts.contradicted && `${counts.contradicted} contradicted`,
    counts.unclear && `${counts.unclear} unclear`,
    counts.not_matched && `${counts.not_matched} not hit`,
  ].filter(Boolean);
  return parts.length ? parts.join(' | ') : `${counts.total} checked`;
}

function scenarioHitSummary(items) {
  const rows = Array.isArray(items) ? items : [];
  const totals = { bull: 0, base: 0, bear: 0 };
  const hits = { bull: 0, base: 0, bear: 0 };
  rows.forEach((item) => {
    const scenario = String(item?.scenario || '').trim().toLowerCase();
    if (!Object.prototype.hasOwnProperty.call(totals, scenario)) return;
    totals[scenario] += 1;
    if (String(item?.status || '').trim().toLowerCase() === 'matched') hits[scenario] += 1;
  });
  return ['bull', 'base', 'bear'].map((name) => ({
    name,
    label: labelScenarioPath(name),
    hits: hits[name],
    total: totals[name],
  }));
}

function conditionStateLabel(item, fallback) {
  const status = String(item?.status || '').trim();
  const severity = String(item?.severity || '').trim();
  return status || severity || fallback;
}

function conditionMetaText(item) {
  const parts = [
    item?.target_period && `by ${item.target_period}`,
    item?.source_to_monitor,
  ];
  return parts.filter(Boolean).join(' | ');
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
      {title && <h4>{title}</h4>}
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

function CompactStat({ label, value, tone = '' }) {
  return (
    <div className={`announcement-router-compact-stat ${tone ? `tone-${tone}` : ''}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function CollapsibleSection({ title, summary, open = false, children }) {
  return (
    <details className="announcement-router-drilldown" open={open}>
      <summary>
        <strong>{title}</strong>
        {summary && <span>{summary}</span>}
      </summary>
      <div className="announcement-router-drilldown-body">{children}</div>
    </details>
  );
}

function ConditionChecks({ title = '', items }) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) return null;
  return (
    <>
      {title && <h4>{title}</h4>}
      <div className="announcement-router-condition-checks">
        {rows.map((item, idx) => (
          <div key={`${title}-${idx}`} className="announcement-router-condition-check">
            <span className={`announcement-router-status tone-${conditionTone(item?.status, item?.group)}`}>
              {conditionStatusLabel(item?.status)}
            </span>
            <div>
              <strong>{routerItemLabel(item)}</strong>
              {routerItemMeta(item) && <em>{routerItemMeta(item)}</em>}
            </div>
          </div>
        ))}
      </div>
    </>
  );
}

function ConditionSummary({ announcementChecks, watchlistChecks, marketConditions }) {
  const scenarioRows = scenarioHitSummary(announcementChecks);
  return (
    <div className="announcement-router-condition-summary">
      <div className="announcement-router-scenario-hit-strip">
        {scenarioRows.map((row) => (
          <CompactStat
            key={row.name}
            label={row.label}
            value={`${row.hits}/${row.total || 0} hit`}
            tone={row.name}
          />
        ))}
      </div>
      <div className="announcement-router-compact-grid">
        <CompactStat label="Announcement checks" value={compactStatusSummary(announcementChecks)} />
        <CompactStat label="Watchlist checks" value={compactStatusSummary(watchlistChecks)} />
        <CompactStat label="Market backdrop" value={compactStatusSummary(marketConditions, 'not checked')} />
      </div>
    </div>
  );
}

function ThesisSnapshot({ snapshot, router }) {
  const scenarios = snapshot?.scenarios && typeof snapshot.scenarios === 'object' ? snapshot.scenarios : {};
  const watchlist = snapshot?.monitoring_watchlist && typeof snapshot.monitoring_watchlist === 'object'
    ? snapshot.monitoring_watchlist
    : {};
  const verification = Array.isArray(snapshot?.verification_queue) ? snapshot.verification_queue : [];
  const redFlags = Array.isArray(watchlist.red_flags) ? watchlist.red_flags : [];
  const confirmatorySignals = Array.isArray(watchlist.confirmatory_signals) ? watchlist.confirmatory_signals : [];
  if (!Object.keys(scenarios).length && !redFlags.length && !confirmatorySignals.length && !verification.length) return null;
  const hasRouterHit = Boolean(
    Number(router?.matched_conditions_count || 0) ||
    Number(router?.triggered_watchlist_count || 0) ||
    String(router?.path_transition || '').trim()
  );

  return (
    <details className="announcement-router-thesis-snapshot" open={hasRouterHit}>
      <summary>
        <strong>Thesis Map Used For This Decision</strong>
        <span>bull/base/bear conditions from the saved lab run</span>
      </summary>
      <div className="announcement-router-path-explainer">
        {pathExplanation(router)}
      </div>
      <div className="announcement-router-scenario-grid">
        {['bull', 'base', 'bear'].map((name) => {
          const block = scenarios[name] || {};
          const required = Array.isArray(block.required_conditions) ? block.required_conditions : [];
          const failures = Array.isArray(block.failure_conditions) ? block.failure_conditions : [];
          return (
            <article key={name} className={`announcement-router-scenario-card tone-${scenarioTone(name)}`}>
              <header>
                <strong>{name.toUpperCase()}</strong>
                <span>{scenarioTargetText(block)}</span>
              </header>
              {block.summary && <p>{block.summary}</p>}
              {(block.current_positioning || block.why_current_positioning) && (
                <p className="announcement-router-scenario-sub">
                  {block.current_positioning}{block.current_positioning && block.why_current_positioning ? ' | ' : ''}{block.why_current_positioning}
                </p>
              )}
              {!!(required.length || failures.length) && (
                <details className="announcement-router-mini-details">
                  <summary>{required.length} required | {failures.length} failure</summary>
                  {!!required.length && <h5>Required</h5>}
                  {required.map((item, idx) => (
                    <div key={`req-${name}-${idx}`} className="announcement-router-thesis-condition">
                      <span>{conditionStateLabel(item, 'monitor')}</span>
                      <strong>{item.condition}</strong>
                      {conditionMetaText(item) && <em>{conditionMetaText(item)}</em>}
                    </div>
                  ))}
                  {!!failures.length && <h5>Failure</h5>}
                  {failures.map((item, idx) => (
                    <div key={`fail-${name}-${idx}`} className="announcement-router-thesis-condition is-risk">
                      <span>{conditionStateLabel(item, 'at-risk')}</span>
                      <strong>{item.condition}</strong>
                      {conditionMetaText(item) && <em>{conditionMetaText(item)}</em>}
                    </div>
                  ))}
                </details>
              )}
            </article>
          );
        })}
      </div>

      {!!(redFlags.length || confirmatorySignals.length) && (
        <details className="announcement-router-mini-details">
          <summary>Monitoring Watchlist | {redFlags.length} red flags, {confirmatorySignals.length} confirmatory</summary>
          <div className="announcement-router-watchlist-grid">
            {redFlags.map((item, idx) => (
              <div key={`red-${idx}`} className="announcement-router-thesis-condition is-risk">
                <span>{conditionStateLabel(item, 'red flag')}</span>
                <strong>{item.condition}</strong>
                {conditionMetaText(item) && <em>{conditionMetaText(item)}</em>}
              </div>
            ))}
            {confirmatorySignals.map((item, idx) => (
              <div key={`confirm-${idx}`} className="announcement-router-thesis-condition">
                <span>{conditionStateLabel(item, 'confirm')}</span>
                <strong>{item.condition}</strong>
                {conditionMetaText(item) && <em>{conditionMetaText(item)}</em>}
              </div>
            ))}
          </div>
        </details>
      )}

      {!!verification.length && (
        <details className="announcement-router-mini-details">
          <summary>Verification Queue | {verification.length} item{verification.length === 1 ? '' : 's'}</summary>
          <div className="announcement-router-condition-checks">
            {verification.map((item, idx) => (
              <div key={`verify-${idx}`} className="announcement-router-condition-check">
                <span className={`announcement-router-status tone-${String(item.priority || '').toLowerCase() === 'high' ? 'bear' : 'base'}`}>
                  {String(item.priority || 'check').toUpperCase()}
                </span>
                <div>
                  <strong>{item.field}</strong>
                  {(item.reason || item.required_source) && <em>{[item.reason, item.required_source && `Need: ${item.required_source}`].filter(Boolean).join(' | ')}</em>}
                </div>
              </div>
            ))}
          </div>
        </details>
      )}
    </details>
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
  const announcementChecks = Array.isArray(router?.announcement_condition_checks) ? router.announcement_condition_checks : [];
  const watchlistChecks = Array.isArray(router?.watchlist_condition_checks) ? router.watchlist_condition_checks : [];
  const followUps = Array.isArray(router?.follow_up_steps) ? router.follow_up_steps : [];
  const directHits = matchedConditions.length + watchHits.length;

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
      <div className="announcement-router-decision-hero">
        <div>
          <span>{router?.ticker || 'Selected filing'}</span>
          <strong>{routerOutcomeLabel(router)}</strong>
          <em>{routerTitle(router)}</em>
        </div>
        <b className={`announcement-router-action-pill action-${String(router?.action || 'watch').toLowerCase()}`}>
          {labelScenarioAction(router?.action)}
        </b>
      </div>
      <div className="announcement-router-metric-grid">
        <DetailRow label="Saved lab path" value={labelScenarioPath(router?.baseline_path || router?.current_path)} tone={scenarioTone(router?.baseline_path || router?.current_path)} />
        <DetailRow label="Materiality" value={router?.impact_level ? titleizeKey(router.impact_level) : 'Not assessed'} />
        <DetailRow label="Official source" value={router?.source_type ? titleizeKey(router.source_type) : 'Unknown'} />
        <DetailRow label="Last evaluated" value={router?.saved_at_utc ? fmtRelativeSince(router.saved_at_utc) : 'n/a'} />
      </div>
      <div className={`announcement-router-impact-callout ${directHits ? 'has-hit' : 'no-hit'}`}>
        <strong>{directHits ? `${directHits} announcement thesis hit${directHits === 1 ? '' : 's'}` : 'No announcement-thesis hit'}</strong>
        <span>{directHits ? 'The filing matched mapped lab conditions below.' : 'The filing did not match the bull/base/bear conditions. Market facts are shown separately as backdrop.'}</span>
      </div>
      {explainRouterDecision(router) && <div className="scenario-router-detail-note">{explainRouterDecision(router)}</div>}
      {routerReason(router) && <div className="scenario-router-detail-note"><strong>Decision reason:</strong> {routerReason(router)}</div>}
      {router?.source_url && (
        <div className="scenario-router-detail-note">
          <strong>Primary source:</strong> <a href={router.source_url} target="_blank" rel="noreferrer">Open filing</a>
        </div>
      )}
      <ConditionSummary
        announcementChecks={announcementChecks}
        watchlistChecks={watchlistChecks}
        marketConditions={marketConditions}
      />
      <DetailList title="Matched Thesis Conditions" items={matchedConditions} />
      <DetailList title="Watchlist Hits" items={watchHits} />
      <DetailList title="Conflicts With Saved Run" items={conflicts} conflict />
      <DetailList title="Supporting Findings" items={findings} />
      {!!announcementChecks.length && (
        <CollapsibleSection
          title="Announcement Conditions Checked"
          summary={compactStatusSummary(announcementChecks)}
          open={directHits > 0}
        >
          <ConditionChecks items={announcementChecks} />
        </CollapsibleSection>
      )}
      {!!watchlistChecks.length && (
        <CollapsibleSection
          title="Watchlist Conditions Checked"
          summary={compactStatusSummary(watchlistChecks)}
          open={watchHits.length > 0}
        >
          <ConditionChecks items={watchlistChecks} />
        </CollapsibleSection>
      )}
      {!!marketConditions.length && (
        <CollapsibleSection
          title="Market Conditions Checked"
          summary={compactStatusSummary(marketConditions)}
        >
          <DetailList title="" items={marketConditions} market />
        </CollapsibleSection>
      )}
      {!!Object.keys(router?.market_facts_used || {}).length && (
        <CollapsibleSection
          title="Market Facts Used"
          summary={`${Object.keys(router.market_facts_used || {}).length} value${Object.keys(router.market_facts_used || {}).length === 1 ? '' : 's'}`}
        >
          <div className="scenario-router-chip-list">
            {Object.entries(router.market_facts_used || {}).map(([key, value]) => (
              <span key={`market-${key}`} className="scenario-router-chip">
                {titleizeKey(key)} | {typeof value === 'number' ? fmtNum(value, value >= 100 ? 0 : 2) : String(value)}
              </span>
            ))}
          </div>
        </CollapsibleSection>
      )}
      {!!followUps.length && (
        <CollapsibleSection title="Next Steps" summary={`${followUps.length} suggested`}>
          <div className="scenario-router-detail-list">
            {followUps.map((item, idx) => (
              <div key={`follow-up-${idx}`} className="scenario-router-detail-item">
                <strong>{item}</strong>
              </div>
            ))}
          </div>
        </CollapsibleSection>
      )}
      <ThesisSnapshot snapshot={router?.thesis_snapshot} router={router} />
      <details className="announcement-router-trace-details">
        <summary>Processing trace</summary>
        <PipelineTrace router={router} />
      </details>
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
    if (embedded || typeof document === 'undefined') return undefined;
    const root = document.getElementById('root');
    const previousRootOverflow = root?.style.overflow ?? '';
    const previousBodyOverflow = document.body.style.overflow;
    const previousHtmlOverflow = document.documentElement.style.overflow;
    if (root) root.style.overflow = 'auto';
    document.body.style.overflow = 'auto';
    document.documentElement.style.overflow = 'auto';
    return () => {
      if (root) root.style.overflow = previousRootOverflow;
      document.body.style.overflow = previousBodyOverflow;
      document.documentElement.style.overflow = previousHtmlOverflow;
    };
  }, [embedded]);

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
  const statusCounts = topCountEntries(overview?.status_counts, 4);

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

            <article className="scenario-router-column announcement-router-list-column">
              <div className="announcement-router-list-head">
                <h4>Routed Announcements</h4>
                <span>latest {Math.min(recentEvents.length, 10)} shown</span>
              </div>
              <div className="scenario-router-event-list">
                {recentEvents.slice(0, 10).map((row) => (
                  <button
                    type="button"
                    className={`scenario-router-event ${selectedEvent?.event_id === row.event_id ? 'is-selected' : ''}`}
                    key={row.event_id || `${row.ticker}-${row.saved_at_utc}`}
                    onClick={() => setSelectedEventId(row.event_id || '')}
                  >
                    <div className="scenario-router-event-top">
                      <strong>{row.ticker || 'n/a'}</strong>
                      <span>{row.status || 'ok'}</span>
                      <span>{routerOutcomeLabel(row)}</span>
                    </div>
                    <div className="scenario-router-event-title">{row.title || 'Untitled announcement'}</div>
                    <div className="scenario-router-event-meta">
                      Saved path: {labelScenarioPath(row.baseline_path || row.current_path)} | {row.source_type || 'unknown source'} | {fmtMs(row.processing_duration_ms)} | {row.saved_at_utc ? fmtRelativeSince(row.saved_at_utc) : 'n/a'}
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
