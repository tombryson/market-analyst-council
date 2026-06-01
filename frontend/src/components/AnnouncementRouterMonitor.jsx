import { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api';
import ScenarioTimelineUnit from './ScenarioTimelineUnit';
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

function fmtPrice(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  if (Math.abs(n) >= 10) return n.toFixed(2);
  if (Math.abs(n) >= 1) return n.toFixed(3).replace(/0+$/, '').replace(/\.$/, '');
  return n.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
}

function numericOrNull(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeCurrencyCode(value) {
  const code = String(value || '').trim().toUpperCase();
  if (['AUD', 'A$', 'AU$'].includes(code)) return 'AUD';
  if (['USD', 'US$'].includes(code)) return 'USD';
  if (['GBP', 'GBX', 'GBP_PENCE', 'PENCE'].includes(code)) return code;
  return code || 'AUD';
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
  if (path === 'bull') return 'Bull scenario';
  if (path === 'base') return 'Base scenario';
  if (path === 'bear') return 'Bear scenario';
  if (path === 'mixed') return 'Mixed / unclear';
  return 'Not assessed';
}

function labelScenarioPathShort(value) {
  return labelScenarioPath(value).replace(' scenario', '');
}

function labelScenarioAction(value) {
  const action = String(value || '').trim().toLowerCase();
  const labels = {
    ignore: 'No workflow action',
    watch: 'Monitor only',
    annotate_run: 'Add note',
    run_delta_only: 'Update section',
    rerun_stage1: 'Refresh evidence',
    full_rerun: 'Rebuild run',
    urgent_human_review: 'Needs you now',
  };
  return labels[action] || (action ? titleizeKey(action) : 'Not assessed');
}

function labelProjectionRerunSignal(value) {
  const signal = String(value || '').trim().toLowerCase();
  const labels = {
    rebuild_analysis: 'Rebuild analysis',
    refresh_evidence: 'Refresh evidence',
    review_thesis_map: 'Review thesis map',
    annotate_evidence: 'Annotate evidence',
    none: 'No rerun signal',
  };
  return labels[signal] || (signal ? titleizeKey(signal) : 'No rerun signal');
}

function labelMarketPath(value) {
  const path = String(value || '').trim().toLowerCase();
  if (path === 'bull') return 'Closest to bull';
  if (path === 'base') return 'Closest to base';
  if (path === 'bear') return 'Closest to bear';
  if (path.startsWith('above_')) return `Above ${labelScenarioPathShort(path.replace('above_', ''))}`;
  if (path.startsWith('below_')) return `Below ${labelScenarioPathShort(path.replace('below_', ''))}`;
  return 'Market path unknown';
}

function monthOffsetFromProjectionTiming(value) {
  const text = String(value || '').trim().toUpperCase();
  if (!text) return null;
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth();
  const quarter = text.match(/\bQ([1-4])\s*(20\d{2})\b/);
  if (quarter) {
    const q = Number(quarter[1]);
    const y = Number(quarter[2]);
    const targetMonth = (q - 1) * 3;
    return Math.max(0, Math.min(24, (y - year) * 12 + (targetMonth - month)));
  }
  const half = text.match(/\bH([12])\s*(20\d{2})\b/);
  if (half) {
    const h = Number(half[1]);
    const y = Number(half[2]);
    const targetMonth = h === 1 ? 2 : 8;
    return Math.max(0, Math.min(24, (y - year) * 12 + (targetMonth - month)));
  }
  const yearOnly = text.match(/\b(20\d{2})\b/);
  if (yearOnly) {
    const y = Number(yearOnly[1]);
    return Math.max(0, Math.min(24, (y - year) * 12 + (6 - month)));
  }
  return null;
}

function routerTrajectoryState(router) {
  return String(router?.trajectory_state || '').trim().toLowerCase();
}

function labelTrajectoryState(value) {
  const state = String(value || '').trim().toLowerCase();
  const labels = {
    thesis_strengthened: 'Thesis strengthened',
    thesis_weakened: 'Thesis weakened',
    timeline_accelerated: 'Timeline accelerated',
    timeline_delayed: 'Timeline delayed',
    risk_reduced: 'Risk reduced',
    risk_increased: 'Risk increased',
    material_unmapped: 'Material, unmapped',
    market_backdrop_only: 'Market backdrop only',
    administrative_filing: 'Administrative filing',
    no_thesis_change: 'No thesis change',
    needs_classification: 'Needs classification',
  };
  return labels[state] || (state ? titleizeKey(state) : 'Not assessed');
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

function routerVerificationItems(router) {
  return routerDetailItems(router, 'triggered_verification_details', 'triggered_verification');
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

function routerFilingSummary(router) {
  const summary = String(router?.filing_summary || '').trim();
  if (summary) return summary;
  const semantic = String(router?.semantic_summary || '').trim();
  if (semantic) return semantic;
  return '';
}

function routerReason(router) {
  return String(router?.reason || router?.action_reason || '').trim();
}

function routerActionKey(router) {
  return String(router?.action || '').trim().toLowerCase();
}

function routerTopicSummary(router) {
  const domains = Array.isArray(router?.affected_domains) ? router.affected_domains : [];
  const labels = domains
    .map((item) => titleizeKey(item))
    .filter(Boolean);
  if (!labels.length) return 'Unclassified';
  return labels.slice(0, 3).join(', ');
}

function routerMateriality(router) {
  const materiality = String(router?.materiality || '').trim().toLowerCase();
  if (materiality) return materiality;
  const impact = String(router?.impact_level || '').trim().toLowerCase();
  if (['none', 'low', 'medium', 'high', 'critical'].includes(impact)) return impact;
  return '';
}

function routerMaterialityLabel(router) {
  const materiality = routerMateriality(router);
  if (!materiality) return 'Not assessed';
  if (materiality === 'none') return 'None';
  return `${titleizeKey(materiality)} materiality`;
}

function routerDriverLabel(router) {
  const klass = String(router?.announcement_class || '').trim();
  if (['needs_classification', 'unknown', 'unclassified'].includes(klass.toLowerCase())) return 'Unclassified';
  if (klass) return titleizeKey(klass);
  return routerTopicSummary(router);
}

function routerVerdictCopy(router) {
  const state = routerTrajectoryState(router);
  const directHits = routerDirectHitCount(router);
  const transition = String(router?.path_transition || '').trim();
  const labels = {
    thesis_strengthened: 'The filing improves the saved thesis path. Check the mapped conditions and evidence before updating the run narrative.',
    thesis_weakened: 'The filing weakens the saved thesis path. This deserves human review before the thesis stays unchanged.',
    timeline_accelerated: 'The filing pulls the expected path forward. The timing assumptions in the saved thesis may need updating.',
    timeline_delayed: 'The filing pushes the expected path out. The saved timeline should be reviewed before this is dismissed.',
    risk_reduced: 'The filing reduces a tracked risk in the saved thesis map.',
    risk_increased: 'The filing increases a tracked risk in the saved thesis map.',
    material_unmapped: 'This looks material, but no saved thesis condition covers it. Treat it as a thesis-map gap until reviewed.',
    market_backdrop_only: 'No filing-led thesis movement was found. Market context was checked separately and is not the announcement verdict.',
    administrative_filing: 'Administrative filing. Recorded for completeness; no direct thesis or price/time change detected.',
    no_thesis_change: 'Checked against the saved thesis map. No direct change to the thesis path was detected.',
    needs_classification: 'The filing was captured, but the router could not confidently classify its thesis impact.',
  };
  if (labels[state]) return labels[state];
  if (transition) return `Moved saved thesis view: ${labelScenarioTransition(transition)}.`;
  if (directHits) {
    return `Matched ${directHits} saved thesis, watchlist, or verification item${directHits === 1 ? '' : 's'}.`;
  }
  if (router?.semantic_summary) return String(router.semantic_summary);
  if (router?.price_time_effect) return String(router.price_time_effect);
  return routerReason(router);
}

function routerOutcomeLabel(router) {
  if (!hasRouterDecision(router)) return 'Not assessed';
  const state = routerTrajectoryState(router);
  if (state) return labelTrajectoryState(state);
  const transition = String(router.path_transition || '').trim();
  const matched = Number(router.matched_conditions_count || 0);
  const watched = Number(router.triggered_watchlist_count || 0);
  const verified = Number(router.triggered_verification_count || routerVerificationItems(router).length || 0);
  const action = routerActionKey(router);
  if (transition) return labelScenarioTransition(transition);
  if (matched || watched || verified) return `${matched + watched + verified} evidence check${matched + watched + verified === 1 ? '' : 's'} matched`;
  if (action === 'ignore') return 'No thesis impact';
  if (action === 'watch') return 'Watch';
  return labelScenarioAction(action);
}

function routerActionBucket(router) {
  const state = routerTrajectoryState(router);
  if (['needs_classification', 'material_unmapped', 'thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'attention';
  if (routerVerificationItems(router).length) return 'attention';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'trajectory';
  if (state === 'market_backdrop_only' || state === 'no_thesis_change') return 'no_change';
  if (state === 'administrative_filing') return 'administrative';
  return 'all';
}

function routerDirectHitCount(router) {
  return routerDetailItems(router, 'matched_condition_details', 'matched_conditions').length +
    routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist').length +
    routerVerificationItems(router).length;
}

function routerConflictCount(router) {
  return Array.isArray(router?.conflicts_with_run) ? router.conflicts_with_run.length : 0;
}

function routerPriorityTone(router) {
  const state = routerTrajectoryState(router);
  const action = routerActionKey(router);
  const status = String(router?.status || '').trim().toLowerCase();
  if (['thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'urgent';
  if (['needs_classification', 'material_unmapped'].includes(state)) return 'warn';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'positive';
  if (['market_backdrop_only', 'no_thesis_change', 'administrative_filing'].includes(state)) return 'neutral';
  if (status === 'error' || action === 'urgent_human_review' || action === 'full_rerun') return 'urgent';
  if (action === 'rerun_stage1' || action === 'run_delta_only' || routerConflictCount(router)) return 'alarm';
  if (routerDirectHitCount(router) || action === 'annotate_run') return 'warn';
  if (action === 'watch') return 'info';
  return 'neutral';
}

function routerPriorityRank(router) {
  const ranks = { urgent: 0, alarm: 1, warn: 2, positive: 3, info: 4, neutral: 5 };
  return ranks[routerPriorityTone(router)] ?? 4;
}

function routerNeedsAttention(router) {
  const state = routerTrajectoryState(router);
  const action = routerActionKey(router);
  return Boolean(
    ['needs_classification', 'material_unmapped', 'thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state) ||
    ['urgent_human_review', 'full_rerun', 'rerun_stage1', 'run_delta_only', 'annotate_run', 'watch'].includes(action) ||
    routerDirectHitCount(router) ||
    routerConflictCount(router) ||
    String(router?.status || '').trim().toLowerCase() === 'error'
  );
}

function routerEvidenceState(router) {
  const state = routerTrajectoryState(router);
  if (routerConflictCount(router)) return 'Saved thesis conflict';
  if (routerDetailItems(router, 'matched_condition_details', 'matched_conditions').length) return 'Thesis condition matched';
  if (routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist').length) return 'Watchlist condition matched';
  if (routerVerificationItems(router).length) return 'Verification item matched';
  if (state === 'needs_classification') return 'Classification unresolved';
  if (state === 'market_backdrop_only') return 'Market backdrop only';
  if (Array.isArray(router?.market_context_conditions) && router.market_context_conditions.length) return 'Market checked';
  if (!router?.source_url && String(router?.source_type || '').toLowerCase() !== 'exchange_filing') return 'Source not resolved';
  return 'No condition match';
}

function routerPresentation(router) {
  const tone = routerPriorityTone(router);
  const action = routerActionKey(router);
  return {
    tone,
    action,
    bucket: routerActionBucket(router),
    trajectoryState: routerTrajectoryState(router),
    trajectoryLabel: labelTrajectoryState(routerTrajectoryState(router)),
    actionLabel: labelScenarioAction(action),
    outcomeLabel: routerOutcomeLabel(router),
    evidenceLabel: routerEvidenceState(router),
    needsAttention: routerNeedsAttention(router),
  };
}

function pathExplanation(router) {
  const path = String(router?.baseline_path || router?.current_path || '').trim();
  if (!path) return '';
  return `${labelScenarioPath(path)} is the saved view from the latest lab run before this announcement. The router only changes it when the filing matches mapped bull/base/bear conditions.`;
}

function conditionTone(status, group = '') {
  const normalized = String(status || '').trim().toLowerCase();
  const normalizedGroup = String(group || '').trim().toLowerCase();
  if (normalized === 'matched' && ['failure', 'red_flag'].includes(normalizedGroup)) return 'contradict';
  if (normalized === 'matched') return 'confirm';
  if (normalized === 'contradicted') return 'contradict';
  if (normalized === 'unclear') return 'unclear';
  return 'muted';
}

function conditionStatusLabel(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'not_matched') return 'Not matched';
  if (normalized === 'matched') return 'Matched';
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
    counts.matched && `${counts.matched} matched`,
    counts.contradicted && `${counts.contradicted} contradicted`,
    counts.unclear && `${counts.unclear} unclear`,
    counts.not_matched && `${counts.not_matched} not matched`,
  ].filter(Boolean);
  return parts.length ? parts.join(' | ') : `${counts.total} checked`;
}

function auditTrailSummary(announcementChecks, watchlistChecks, verificationChecks, marketConditions) {
  return [
    `Announcement: ${compactStatusSummary(announcementChecks)}`,
    `Watchlist: ${compactStatusSummary(watchlistChecks, 'none checked')}`,
    `Verification: ${compactStatusSummary(verificationChecks, 'none checked')}`,
    `Market: ${compactStatusSummary(marketConditions, 'none checked')}`,
  ].join(' | ');
}

function routerWhyHeadline(router, directHits = 0) {
  const state = routerTrajectoryState(router);
  if (directHits) return 'Mapped evidence was found';
  if (state === 'needs_classification') return 'Classification is unresolved';
  if (state === 'material_unmapped') return 'Material filing is not covered by the thesis map';
  if (state === 'administrative_filing') return 'Procedural filing with no mapped thesis impact';
  if (state === 'market_backdrop_only') return 'Market context only';
  if (state === 'no_thesis_change') return 'No mapped thesis condition changed';
  if (['thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'Saved thesis path is under pressure';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'Saved thesis path improved';
  return 'Router basis';
}

function routerWhyCopy(router, directHits = 0) {
  const state = routerTrajectoryState(router);
  const announcementChecks = Array.isArray(router?.announcement_condition_checks) ? router.announcement_condition_checks : [];
  const watchlistChecks = Array.isArray(router?.watchlist_condition_checks) ? router.watchlist_condition_checks : [];
  const verificationChecks = Array.isArray(router?.verification_condition_checks) ? router.verification_condition_checks : [];
  const marketConditions = Array.isArray(router?.market_context_conditions) ? router.market_context_conditions : [];
  const marketCount = marketConditions.length || Object.keys(router?.market_facts_used || {}).length;

  if (directHits) {
    return `The filing matched ${directHits} saved thesis, watchlist, or verification item${directHits === 1 ? '' : 's'}. Review the evidence rows below before changing the saved run.`;
  }
  if (state === 'needs_classification') {
    return router?.classification_reason || 'The filing was captured, but the classifier could not map it to a known filing type or saved thesis driver. It should be labelled by a human rather than treated as urgent thesis damage.';
  }
  if (state === 'material_unmapped') {
    return 'The filing appears material, but none of the saved thesis conditions matched it. That points to a thesis-map gap, not a clean no-impact result.';
  }
  if (state === 'administrative_filing') {
    return 'The filing was classified as procedural or administrative, and no saved thesis or watchlist condition matched it.';
  }
  if (state === 'market_backdrop_only' || (marketCount && !directHits)) {
    return 'The announcement itself did not match a saved thesis condition. Market facts were checked as backdrop only and are not the filing verdict.';
  }
  if (state === 'no_thesis_change') {
    return 'The filing was checked against the saved announcement and watchlist conditions; none changed the saved thesis path.';
  }
  if (announcementChecks.length || watchlistChecks.length) {
    return `Checked ${announcementChecks.length} announcement condition${announcementChecks.length === 1 ? '' : 's'}, ${watchlistChecks.length} watchlist condition${watchlistChecks.length === 1 ? '' : 's'}, and ${verificationChecks.length} verification item${verificationChecks.length === 1 ? '' : 's'} without a mapped thesis change.`;
  }
  return routerReason(router) || 'No additional router basis was recorded for this filing.';
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

function ConditionSummary({ announcementChecks, watchlistChecks, verificationChecks, marketConditions }) {
  const scenarioRows = scenarioHitSummary(announcementChecks);
  return (
    <div className="announcement-router-condition-summary">
      <div className="announcement-router-scenario-hit-strip">
        {scenarioRows.map((row) => (
          <CompactStat
            key={row.name}
            label={row.label}
            value={`${row.hits}/${row.total || 0} matched`}
            tone={row.name}
          />
        ))}
      </div>
      <div className="announcement-router-compact-grid">
        <CompactStat label="Announcement checks" value={compactStatusSummary(announcementChecks)} />
        <CompactStat label="Watchlist checks" value={compactStatusSummary(watchlistChecks)} />
        <CompactStat label="Verification checks" value={compactStatusSummary(verificationChecks)} />
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

function RouterSection({ title, count = '', children, muted = false }) {
  if (!children) return null;
  return (
    <section className={`announcement-router-section ${muted ? 'is-muted' : ''}`}>
      <h4>
        <span>{title}</span>
        {count && <em>{count}</em>}
      </h4>
      {children}
    </section>
  );
}

function splitScenarioTransition(value) {
  const transition = String(value || '').trim().toLowerCase();
  if (!transition.includes('->')) return null;
  const [from, to] = transition.split('->').map((part) => part.trim()).filter(Boolean);
  if (!from || !to) return null;
  return { from, to };
}

function PathTransition({ router }) {
  const transition = splitScenarioTransition(router?.path_transition);
  const baseline = transition?.from || String(router?.baseline_path || router?.current_path || '').trim().toLowerCase();
  const current = transition?.to || String(router?.current_path || router?.baseline_path || '').trim().toLowerCase();
  const changed = Boolean(transition && transition.from !== transition.to);
  return (
    <div className="announcement-router-path-bar">
      <span className="announcement-router-path-label">Saved thesis path</span>
      <div className="announcement-router-path-stack" aria-label="Saved thesis path">
        {['bull', 'base', 'bear'].map((name) => (
          <span
            key={name}
            className={`announcement-router-path-pill tone-${name} ${baseline === name ? 'is-on' : ''}`}
          >
            {labelScenarioPath(name).replace(' scenario', '')}
          </span>
        ))}
      </div>
      <span className="announcement-router-path-arrow">{changed ? 'moves to' : 'stays at'}</span>
      <span className={`announcement-router-path-pill tone-${scenarioTone(current)} is-current`}>
        {labelScenarioPath(current)}
      </span>
    </div>
  );
}

function projectionScenarioData(projection) {
  const target12 = projection?.target_12m && typeof projection.target_12m === 'object' ? projection.target_12m : {};
  const target24 = projection?.target_24m && typeof projection.target_24m === 'object' ? projection.target_24m : {};
  const current = numericOrNull(projection?.current_price);
  const value12 = (name) => {
    const direct = numericOrNull(target12[name]);
    if (direct != null) return direct;
    const terminal = numericOrNull(target24[name]);
    if (terminal != null && current != null) return current + ((terminal - current) * 0.5);
    return terminal;
  };
  const weighted24 = numericOrNull(projection?.prob_weighted_target_24m);
  return {
    current: current ?? 0,
    targets12: {
      bear: value12('bear') ?? 0,
      base: value12('base') ?? 0,
      bull: value12('bull') ?? 0,
    },
    targets24: {
      bear: numericOrNull(target24.bear) ?? value12('bear') ?? 0,
      base: numericOrNull(target24.base) ?? value12('base') ?? 0,
      bull: numericOrNull(target24.bull) ?? value12('bull') ?? 0,
    },
    weighted12: current != null && weighted24 != null ? current + ((weighted24 - current) * 0.5) : (weighted24 ?? 0),
    weighted24: weighted24 ?? 0,
  };
}

function projectionTimelineBars(projection) {
  const rows = Array.isArray(projection?.timeline_rows) ? projection.timeline_rows : [];
  return rows
    .map((row) => {
      const milestone = String(row?.title || row?.milestone || '').trim();
      if (!milestone) return null;
      const targetPeriod = String(row?.timing || row?.target_period || '').trim();
      return {
        milestone,
        target_period: targetPeriod,
        status: String(row?.status || '').trim(),
        primary_risk: String(row?.primary_risk || row?.risk || '').trim(),
        offset: monthOffsetFromProjectionTiming(targetPeriod),
      };
    })
    .filter(Boolean)
    .slice(0, 8);
}

function MarketPathProjection({ router }) {
  const projection = router?.trajectory_projection && typeof router.trajectory_projection === 'object'
    ? router.trajectory_projection
    : {};
  const chartData = projectionScenarioData(projection);
  const timelineBars = projectionTimelineBars(projection);
  const currentPrice = numericOrNull(projection?.current_price);
  const weighted = numericOrNull(projection?.prob_weighted_target_24m);
  const hasChart = [
    chartData.current,
    ...Object.values(chartData.targets12 || {}),
    ...Object.values(chartData.targets24 || {}),
  ].some((value) => Number.isFinite(value) && Number(value) > 0);
  if (!hasChart && !projection?.rerun_signal && !timelineBars.length) return null;

  const elapsed = Number(projection?.elapsed_days);
  const elapsedCopy = Number.isFinite(elapsed)
    ? `Day ${Math.round(elapsed)} of the saved 24M path`
    : 'Saved 24M path';
  const marketPath = String(projection?.market_implied_path_24m || '').trim().toLowerCase();
  const currency = normalizeCurrencyCode(projection?.currency);

  return (
    <section className="announcement-router-projection">
      <div className="announcement-router-projection-head">
        <div>
          <span>24M Market Path</span>
          <strong>{labelMarketPath(marketPath)}</strong>
        </div>
        <div>
          <span>Rerun signal</span>
          <strong>{labelProjectionRerunSignal(projection?.rerun_signal)}</strong>
        </div>
      </div>
      {hasChart && (
        <ScenarioTimelineUnit
          data={chartData}
          currency={currency}
          timelineBars={timelineBars}
          orientation="vertical"
        />
      )}
      <div className="announcement-router-projection-meta">
        <span>{elapsedCopy}</span>
        {currentPrice != null && <span>Current market {fmtPrice(currentPrice)}</span>}
        {weighted != null && <span>Probability-weighted 24M target {fmtPrice(weighted)}</span>}
        {projection?.rerun_reason && <span>{projection.rerun_reason}</span>}
      </div>
    </section>
  );
}

function EvidenceRows({ title = '', items, market = false }) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) return null;
  return (
    <div className="announcement-router-evidence-list">
      {title && <h5>{title}</h5>}
      {rows.map((item, idx) => (
        <div key={`${title || 'evidence'}-${idx}`} className={`announcement-router-evidence-row tone-${conditionTone(item?.status, item?.group)}`}>
          <span className="announcement-router-evidence-side">{item?.scenario ? labelScenarioPath(item.scenario).replace(' scenario', '') : titleizeKey(item?.group || item?.kind || 'Condition')}</span>
          <span className={`announcement-router-evidence-verdict tone-${conditionTone(item?.status, item?.group)}`}>
            {conditionStatusLabel(item?.status)}
          </span>
          <div>
            <strong>{routerItemLabel(item)}</strong>
            {(market ? formatRouterMarketCondition(item) : routerItemMeta(item)) && (
              <em>{market ? formatRouterMarketCondition(item) : routerItemMeta(item)}</em>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

function MarketFactsGrid({ facts }) {
  const entries = Object.entries(facts || {});
  if (!entries.length) return null;
  return (
    <div className="announcement-router-market-grid">
      {entries.map(([key, value]) => (
        <div key={`market-${key}`} className="announcement-router-market-tile">
          <span>{titleizeKey(key)}</span>
          <strong>{typeof value === 'number' ? fmtNum(value, value >= 100 ? 0 : 2) : String(value)}</strong>
        </div>
      ))}
    </div>
  );
}

function ConfidenceBreakdown({ router }) {
  const breakdown = router?.confidence_breakdown && typeof router.confidence_breakdown === 'object'
    ? router.confidence_breakdown
    : {};
  const rows = [
    {
      label: 'Source',
      value: router?.source_confidence ?? breakdown.source_confidence,
      detail: router?.source_url ? 'Official source resolved' : 'Source resolution limited',
    },
    {
      label: 'Text extraction',
      value: router?.extraction_confidence ?? breakdown.extraction_confidence,
      detail: breakdown?.parse_quality?.decoded_chars
        ? `${breakdown.parse_quality.decoded_chars} chars decoded`
        : 'Extraction quality unavailable',
    },
    {
      label: 'Classification',
      value: router?.classification_confidence ?? router?.parser_confidence ?? breakdown.classification_confidence,
      detail: router?.classification_reason || breakdown.classification_reason || 'Classification basis unavailable',
    },
    {
      label: 'Thesis match',
      value: router?.thesis_match_confidence ?? breakdown.thesis_match_confidence,
      detail: breakdown?.thesis_match
        ? `${breakdown.thesis_match.direct_matches || 0} direct match${breakdown.thesis_match.direct_matches === 1 ? '' : 'es'}`
        : 'No thesis-match detail recorded',
    },
  ];
  return (
    <div className="announcement-router-confidence-grid">
      {rows.map((row) => (
        <div key={row.label} className="announcement-router-confidence-card">
          <span>{row.label}</span>
          <strong>{row.value != null && row.value !== '' ? fmtPct(row.value) : 'n/a'}</strong>
          <em>{row.detail}</em>
        </div>
      ))}
    </div>
  );
}

function RouterKpiStrip({ filters, activeFilter, onSelect }) {
  return (
    <div className="announcement-router-kpi-strip" role="tablist" aria-label="Filing queue filters">
      {filters.map((item) => (
        <button
          type="button"
          key={item.key}
          className={`announcement-router-kpi-card ${activeFilter === item.key ? 'is-active' : ''}`}
          data-tone={item.tone}
          onClick={() => onSelect(item.key)}
        >
          <span className="announcement-router-kpi-head">
            <i />
            <span>{item.label}</span>
          </span>
          <strong>{item.count}</strong>
        </button>
      ))}
    </div>
  );
}

function routerEventDate(row) {
  return parseIsoDateOrNull(row?.saved_at_utc) || parseIsoDateOrNull(row?.received_at_utc);
}

function RouterTimeline({ events, selectedEventId, onSelect }) {
  const rows = Array.isArray(events) ? events : [];
  const datedRows = rows
    .map((row) => ({ row, date: routerEventDate(row) }))
    .filter((item) => item.date);
  if (!datedRows.length) return null;

  const now = Math.max(...datedRows.map((item) => item.date.getTime()));
  const start = now - 30 * 24 * 60 * 60 * 1000;
  const byTicker = new Map();
  datedRows.forEach((item) => {
    const ticker = String(item.row?.ticker || 'n/a').trim() || 'n/a';
    if (!byTicker.has(ticker)) byTicker.set(ticker, []);
    byTicker.get(ticker).push(item);
  });
  const lanes = [...byTicker.entries()]
    .sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]))
    .slice(0, 6);
  const range = Math.max(1, now - start);

  return (
    <section className="announcement-router-timeline" aria-label="Announcement timeline">
      <div className="announcement-router-timeline-head">
        <strong>Last 30 days</strong>
        <span>Click a marker to inspect the filing</span>
      </div>
      <div className="announcement-router-timeline-lanes">
        {lanes.map(([ticker, items]) => (
          <div key={`lane-${ticker}`} className="announcement-router-timeline-lane">
            <span className="announcement-router-timeline-ticker">{ticker}</span>
            <div className="announcement-router-timeline-track">
              {items.map(({ row, date }) => {
                const vm = routerPresentation(row);
                const pct = Math.max(0, Math.min(100, ((date.getTime() - start) / range) * 100));
                const selected = selectedEventId && selectedEventId === row.event_id;
                return (
                  <button
                    type="button"
                    key={`dot-${row.event_id || `${ticker}-${date.toISOString()}`}`}
                    className={`announcement-router-timeline-dot ${selected ? 'is-selected' : ''}`}
                    data-tone={vm.tone}
                    style={{ left: `${pct}%` }}
                    title={`${ticker}: ${labelScenarioAction(row.action)} - ${routerTitle(row)}`}
                    onClick={() => onSelect(row.event_id || '')}
                  />
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function RouterQueue({ events, selectedEventId, onSelect }) {
  return (
    <article className="announcement-router-queue">
      <div className="announcement-router-queue-head">
        <h4>Filing Queue</h4>
        <span>{events.length} shown</span>
      </div>
      <div className="announcement-router-queue-list">
        {events.map((row) => {
          const vm = routerPresentation(row);
          const selected = selectedEventId
            ? selectedEventId === row.event_id
            : events[0]?.event_id === row.event_id;
          return (
            <button
              type="button"
              className={`announcement-router-queue-row ${selected ? 'is-selected' : ''}`}
              data-tone={vm.tone}
              key={row.event_id || `${row.ticker}-${row.saved_at_utc}-${routerTitle(row)}`}
              onClick={() => onSelect(row.event_id || '')}
            >
              <span className="announcement-router-queue-severity" />
              <span className="announcement-router-queue-glyph" aria-hidden="true" />
              <span className="announcement-router-queue-body">
                <span className="announcement-router-queue-topline">
	                  <strong>{row.ticker || 'n/a'}</strong>
	                  <b>{vm.trajectoryLabel}</b>
	                </span>
	                <span className="announcement-router-queue-title">{routerTitle(row)}</span>
	                <span className="announcement-router-queue-meta">
	                  <span>{labelScenarioPathShort(row.baseline_path || row.current_path)}</span>
	                  <span>{routerMaterialityLabel(row)}</span>
	                  <span>{routerDriverLabel(row)}</span>
	                </span>
              </span>
              <span className="announcement-router-queue-time">{row.saved_at_utc ? fmtRelativeSince(row.saved_at_utc) : 'n/a'}</span>
            </button>
          );
        })}
        {!events.length && <div className="watch-empty">No routed announcements in this filter.</div>}
      </div>
    </article>
  );
}

function eventMatchesQueueFilter(row, filterKey) {
  const bucket = routerActionBucket(row);
  if (filterKey === 'attention') return bucket === 'attention';
  if (filterKey === 'trajectory') return bucket === 'trajectory';
  if (filterKey === 'no_change') return bucket === 'no_change';
  if (filterKey === 'administrative') return bucket === 'administrative';
  return true;
}

function DecisionPanel({ router, emptyTitle = 'No announcement decision attached', emptyCopy = '' }) {
  const decisionAvailable = hasRouterDecision(router);
  const matchedConditions = routerDetailItems(router, 'matched_condition_details', 'matched_conditions');
  const watchHits = routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist');
  const verificationHits = routerVerificationItems(router);
  const findings = Array.isArray(router?.key_findings) ? router.key_findings : [];
  const conflicts = Array.isArray(router?.conflicts_with_run) ? router.conflicts_with_run : [];
  const marketConditions = Array.isArray(router?.market_context_conditions) ? router.market_context_conditions : [];
  const announcementChecks = Array.isArray(router?.announcement_condition_checks) ? router.announcement_condition_checks : [];
  const watchlistChecks = Array.isArray(router?.watchlist_condition_checks) ? router.watchlist_condition_checks : [];
  const verificationChecks = Array.isArray(router?.verification_condition_checks) ? router.verification_condition_checks : [];
  const followUps = Array.isArray(router?.follow_up_steps) ? router.follow_up_steps : [];
  const directHits = matchedConditions.length + watchHits.length + verificationHits.length;
  const vm = routerPresentation(router);
  const classificationConfidence = router?.classification_confidence ?? router?.semantic_confidence ?? router?.parser_confidence;
  const filingSummary = routerFilingSummary(router);
  const whyCount = directHits
    ? `${directHits} mapped condition${directHits === 1 ? '' : 's'}`
    : 'no mapped condition';

  if (!decisionAvailable) {
    return (
      <article className="scenario-router-column announcement-router-empty-panel">
        <h4>{emptyTitle}</h4>
        <p>{emptyCopy || 'This run has no saved announcement-router artifact. The lab view will not show unrelated global router events.'}</p>
      </article>
    );
  }

  return (
    <article className="scenario-router-column announcement-router-decision-panel" data-tone={vm.tone}>
      <div className="announcement-router-decision-hero">
        <div className="announcement-router-hero-crumbs">
          <strong>{router?.ticker || 'Selected filing'}</strong>
          <span>{router?.source_type ? titleizeKey(router.source_type) : 'Unknown source'}</span>
          <span>{router?.saved_at_utc ? fmtRelativeSince(router.saved_at_utc) : 'n/a'}</span>
        </div>
        <h1>{routerTitle(router)}</h1>
        {filingSummary && (
          <p className="announcement-router-filing-summary">
            <strong>Filing summary</strong>
            <span>{filingSummary}</span>
          </p>
        )}
        <div className="announcement-router-verdict-card">
          <span className="announcement-router-verdict-kicker">Trajectory assessment</span>
          <strong>{vm.trajectoryLabel}</strong>
          <p>{routerVerdictCopy(router)}</p>
          <div className="announcement-router-verdict-grid">
            <span>
              <em>Driver</em>
              <b>{routerDriverLabel(router)}</b>
            </span>
            <span>
              <em>Materiality</em>
              <b>{routerMaterialityLabel(router)}</b>
            </span>
            <span>
              <em>Evidence</em>
              <b>{vm.evidenceLabel}</b>
            </span>
            <span>
              <em>System action</em>
              <b>{vm.actionLabel}</b>
            </span>
          </div>
        </div>
        <div className="announcement-router-hero-actions">
          {router?.source_url && (
            <a className="announcement-router-primary-link" href={router.source_url} target="_blank" rel="noreferrer">Open filing</a>
          )}
          <span className="announcement-router-hero-metric">{classificationConfidence != null && classificationConfidence !== '' ? `Classification confidence ${fmtPct(classificationConfidence)}` : 'Classification confidence n/a'}</span>
          <span className="announcement-router-hero-tag">{router?.source_type ? titleizeKey(router.source_type) : 'Source unresolved'}</span>
        </div>
      </div>
      <PathTransition router={router} />
      <MarketPathProjection router={router} />

      <RouterSection title="Why This Verdict" count={whyCount}>
        <div className={`announcement-router-impact-callout ${directHits ? 'has-hit' : 'no-hit'}`}>
          <strong>{routerWhyHeadline(router, directHits)}</strong>
          <span>{routerWhyCopy(router, directHits)}</span>
        </div>
        <ConfidenceBreakdown router={router} />
        <EvidenceRows title="Matched Thesis Conditions" items={matchedConditions} />
        <EvidenceRows title="Watchlist Matches" items={watchHits} />
        <EvidenceRows title="Verification Matches" items={verificationHits} />
      </RouterSection>

      {!!(conflicts.length || findings.length) && (
        <RouterSection title="Conflicts And Findings" count={`${conflicts.length + findings.length} item${conflicts.length + findings.length === 1 ? '' : 's'}`}>
          <DetailList title="Conflicts With Saved Run" items={conflicts} conflict />
          <DetailList title="Supporting Findings" items={findings} />
        </RouterSection>
      )}

      <details className="announcement-router-audit-shell">
        <summary>
          <strong>Audit trail</strong>
          <span>{auditTrailSummary(announcementChecks, watchlistChecks, verificationChecks, marketConditions)}</span>
        </summary>
        <div className="announcement-router-audit-body">
          <ConditionSummary
            announcementChecks={announcementChecks}
            watchlistChecks={watchlistChecks}
            verificationChecks={verificationChecks}
            marketConditions={marketConditions}
          />

          {!!announcementChecks.length && (
            <CollapsibleSection
              title="Announcement Conditions Checked"
              summary={compactStatusSummary(announcementChecks)}
              open={directHits > 0}
            >
              <EvidenceRows items={announcementChecks} />
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
          {!!verificationChecks.length && (
            <CollapsibleSection
              title="Verification Queue Checked"
              summary={compactStatusSummary(verificationChecks)}
              open={verificationHits.length > 0}
            >
              <ConditionChecks items={verificationChecks} />
            </CollapsibleSection>
          )}
          {!!marketConditions.length && (
            <CollapsibleSection
              title="Market Conditions Checked"
              summary={compactStatusSummary(marketConditions)}
            >
              <EvidenceRows items={marketConditions} market />
            </CollapsibleSection>
          )}
          {!!Object.keys(router?.market_facts_used || {}).length && (
            <CollapsibleSection
              title="Market Facts Used"
              summary={`${Object.keys(router.market_facts_used || {}).length} value${Object.keys(router.market_facts_used || {}).length === 1 ? '' : 's'}`}
            >
              <MarketFactsGrid facts={router.market_facts_used || {}} />
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
        </div>
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
  const [, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [tickerFilter, setTickerFilter] = useState('');
  const [selectedEventId, setSelectedEventId] = useState('');
  const [activeQueueFilter, setActiveQueueFilter] = useState('attention');

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
  }, [embedded, effectiveTicker]);

  const recentEvents = useMemo(
    () => (Array.isArray(overview?.recent_events) ? overview.recent_events : []),
    [overview]
  );
  const queueEvents = useMemo(
    () => [...recentEvents].sort((a, b) => {
      const priorityDiff = routerPriorityRank(a) - routerPriorityRank(b);
      if (priorityDiff) return priorityDiff;
      return (parseIsoDateOrNull(b?.saved_at_utc)?.getTime() || 0) - (parseIsoDateOrNull(a?.saved_at_utc)?.getTime() || 0);
    }),
    [recentEvents]
  );
	  const queueFilters = useMemo(() => {
	    const count = (key) => queueEvents.filter((row) => eventMatchesQueueFilter(row, key)).length;
	    return [
	      { key: 'attention', label: 'Needs Review', tone: 'warn', count: count('attention') },
	      { key: 'trajectory', label: 'Thesis Improved', tone: 'positive', count: count('trajectory') },
	      { key: 'no_change', label: 'No Thesis Change', tone: 'neutral', count: count('no_change') },
	      { key: 'administrative', label: 'Administrative', tone: 'neutral', count: count('administrative') },
	      { key: 'all', label: 'All Filings', tone: 'neutral', count: queueEvents.length },
	    ];
	  }, [queueEvents]);
  const filteredEvents = useMemo(
    () => queueEvents.filter((row) => eventMatchesQueueFilter(row, activeQueueFilter)),
    [queueEvents, activeQueueFilter]
  );
  const selectedEvent = filteredEvents.find((row) => row?.event_id === selectedEventId) ||
    filteredEvents[0] ||
    {};
  const selectedRouter = embedded ? (runRouter || {}) : selectedEvent;

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
	                Tracks announcement filings against the saved thesis map and shows how each filing changes the stock's thesis trajectory.
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
              <button type="button" className="gantt-lab-inline-retry" onClick={() => navigateTo('/gantt-lab')}>
                Open Timeline Lab
              </button>
            </div>
          </div>

          <div className="announcement-router-console">
            <aside className="announcement-router-side-rail">
	              <div className="announcement-router-side-head">
	                <span>Filing Queue</span>
                <strong>{filteredEvents.length}</strong>
              </div>
              <RouterKpiStrip
                filters={queueFilters}
                activeFilter={activeQueueFilter}
                onSelect={(key) => {
                  setActiveQueueFilter(key);
                  setSelectedEventId('');
                }}
              />
            </aside>

            <div className="announcement-router-main-stage">
              <div className="announcement-router-workspace">
                <RouterQueue
                  events={filteredEvents}
                  selectedEventId={selectedEvent?.event_id || selectedEventId}
                  onSelect={setSelectedEventId}
                />
                <DecisionPanel router={selectedRouter} emptyTitle="No routed announcements yet" />
              </div>

              <RouterTimeline
                events={queueEvents}
                selectedEventId={selectedEvent?.event_id || selectedEventId}
                onSelect={setSelectedEventId}
              />
            </div>
          </div>

          {error && <div className="run-meta-note run-meta-note-error">Announcement router monitor error: {error}</div>}
        </section>
      </div>
    </main>
  );
}
