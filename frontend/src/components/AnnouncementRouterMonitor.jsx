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

const ROUTER_DRIVER_LABELS = {
  administrative: 'Administrative',
  asset_project: 'Project delivery',
  capital_financing: 'Funding',
  capital_management: 'Capital management',
  clinical_regulatory: 'Clinical / regulatory',
  commercial_customer: 'Commercial agreement',
  development_timeline: 'Timeline',
  drilling_exploration: 'Exploration results',
  earnings_guidance: 'Guidance',
  governance_management: 'Governance',
  legal: 'Legal',
  market_backdrop: 'Market backdrop',
  operations: 'Operations',
  permitting: 'Permitting',
  production_operations: 'Production',
  regulatory_legal: 'Regulatory / legal',
  resource: 'Resource / reserve',
  strategy_mna: 'Strategy / M&A',
};

function labelRouterDriver(value) {
  const key = String(value || '').trim().toLowerCase();
  if (!key) return '';
  return ROUTER_DRIVER_LABELS[key] || titleizeKey(key);
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

function fmtSignedDelta(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n === 0) return '0';
  const text = Math.abs(n) % 1 === 0 ? Math.abs(n).toFixed(0) : Math.abs(n).toFixed(1);
  return `${n > 0 ? '+' : '-'}${text}`;
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

function labelCompanyThesisPath(value) {
  const path = String(value || '').trim().toLowerCase();
  if (path === 'bull') return 'Bull';
  if (path === 'bear') return 'Bear';
  return 'Base';
}

function labelCompanyThesisPathShort(value) {
  return labelCompanyThesisPath(value);
}

function labelScenarioAction(value) {
  const action = String(value || '').trim().toLowerCase();
  const labels = {
    ignore: 'No maintenance',
    watch: 'Monitor only',
    annotate_run: 'Attach to thesis log',
    run_delta_only: 'Update thesis note',
    rerun_stage1: 'Refresh evidence pack',
    full_rerun: 'Rebuild council run',
    urgent_human_review: 'Human review now',
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

const COMPANY_THESIS_PATHS = new Set(['bull', 'base', 'bear']);
const ROUTER_MONITOR_LIMIT = 5000;
const COMPANY_SUMMARY_EVENT_LIMIT = 5;
const COMPANY_SUMMARY_BASIS_LIMIT = 3;

function defaultReviewNote(router, status) {
  const title = routerTitle(router);
  const trajectory = routerOutcomeLabel(router);
  if (status === 'reviewed') return `Handled ${title}. Router verdict accepted: ${trajectory}.`;
  if (status === 'dismissed') return `Cleared ${title} as no further action.`;
  return '';
}

function isGeneratedReviewNote(note, router) {
  const text = String(note || '').trim();
  const title = routerTitle(router);
  if (!text || !title || !text.endsWith(`: ${title}`)) return false;
  return text.split(':').length === 2;
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

function routerImpactVerdict(router) {
  const display = routerDisplay(router);
  return String(display.impact_verdict || router?.impact_verdict || '').trim().toLowerCase();
}

function routerFilingType(router) {
  const display = routerDisplay(router);
  return String(display.filing_type || router?.filing_type || '').trim().toLowerCase();
}

function routerThesisRelationship(router) {
  const display = routerDisplay(router);
  return String(display.thesis_relationship || router?.thesis_relationship || '').trim().toLowerCase();
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
    material_unmapped: 'Material filing outside thesis map',
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
  if (k === 'base') return 'base';
  return 'neutral';
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
  if (item.status) parts.push(conditionStatusLabel(item.status));
  if (item.reason) parts.push(String(item.reason));
  const missing = Array.isArray(item.missing_for_full_match)
    ? item.missing_for_full_match.filter(Boolean).join('; ')
    : '';
  if (missing) parts.push(`Missing: ${missing}`);
  if (item.evidence_quote) parts.push(`Quote: "${String(item.evidence_quote)}"`);
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
  if (summary && !isBoilerplateFilingSummary(summary, router) && !isMachineRouterSummary(summary)) return summary;
  const semantic = String(router?.semantic_summary || '').trim();
  if (semantic && !isBoilerplateFilingSummary(semantic, router) && !isMachineRouterSummary(semantic)) return semantic;
  return '';
}

function isMachineRouterSummary(value) {
  const text = String(value || '').trim().toLowerCase();
  return Boolean(
    text.includes(' classified as ') ||
    text.includes('; effect:') ||
    text.includes('; drivers:')
  );
}

function isBoilerplateFilingSummary(value, router) {
  const text = String(value || '').trim().toLowerCase();
  const title = routerTitle(router).toLowerCase();
  const ticker = String(router?.ticker || '').trim().toLowerCase();
  if (!text) return false;
  if (ticker && text.startsWith(`${ticker} filed`) && title && text.includes(title)) return true;
  return text.includes('filed a ') && text.includes(' update touching ') && title && text.endsWith(`${title}.`);
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
    .map((item) => labelRouterDriver(item))
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
  if (klass) return labelRouterDriver(klass);
  return routerTopicSummary(router);
}

function routerDisplay(router) {
  return router?.display && typeof router.display === 'object' ? router.display : {};
}

function reviewLabelFromStatus(status) {
  const key = String(status || '').trim().toLowerCase();
  if (key === 'reviewed') return 'Reviewed';
  if (key === 'dismissed') return 'Cleared';
  if (key === 'open') return 'Needs thesis decision';
  return '';
}

function applyReviewOverlayToEvent(row, review) {
  if (!row || !review || row.event_id !== review.event_id) return row;
  const status = String(review.review_status || '').trim().toLowerCase();
  const label = reviewLabelFromStatus(status);
  const currentDisplay = routerDisplay(row);
  return {
    ...row,
    review_overlay: { ...review, review_status: status },
    display: {
      ...currentDisplay,
      queue_bucket: currentDisplay.queue_bucket,
      queue_label: currentDisplay.queue_label,
      review_status: status || currentDisplay.review_status,
      review_label: label || currentDisplay.review_label,
      review_owner: review.review_owner || currentDisplay.review_owner,
      is_user_action_required: status === 'open',
    },
  };
}

function routerVerdictCopy(router) {
  const display = routerDisplay(router);
  if (display.primary_reason) return String(display.primary_reason);
  const state = routerTrajectoryState(router);
  const directHits = routerDirectHitCount(router);
  const transition = String(router?.path_transition || '').trim();
  const labels = {
    thesis_strengthened: 'The filing improves the company thesis path. Check the mapped conditions and evidence before updating the run narrative.',
    thesis_weakened: 'The filing weakens the company thesis path. This deserves human review before the thesis stays unchanged.',
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
  if (router?.semantic_summary && !isMachineRouterSummary(router.semantic_summary)) return String(router.semantic_summary);
  if (router?.price_time_effect) return String(router.price_time_effect);
  return routerReason(router);
}

function routerOutcomeLabel(router) {
  if (!hasRouterDecision(router)) return 'Not assessed';
  const display = routerDisplay(router);
  if (display.trajectory_label) return String(display.trajectory_label);
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
  const displayBucket = String(routerDisplay(router)?.queue_bucket || '').trim();
  if (displayBucket) return displayBucket;
  const state = routerTrajectoryState(router);
  if (['needs_classification', 'material_unmapped', 'thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'open_review';
  if (routerVerificationItems(router).length) return 'open_review';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'positive_movement';
  if (state === 'market_backdrop_only' || state === 'no_thesis_change') return 'cleared';
  if (state === 'administrative_filing') return 'administrative';
  return 'all';
}

function routerCaseBucket(router) {
  const verdict = routerImpactVerdict(router);
  const filingType = routerFilingType(router);
  const relationship = routerThesisRelationship(router);
  const score = routerTrajectoryScore(router);
  const validationType = String(score?.validation_type || '').trim().toLowerCase();
  const eventDelta = Number(score?.event_delta);
  const directHits = Number(router?.matched_conditions_count || 0) +
    Number(router?.triggered_watchlist_count || 0) +
    Number(router?.triggered_verification_count || 0);
  const unvalidatedPositive = verdict === 'positive' &&
    !directHits &&
    (!validationType || ['none', 'related_unmapped', 'material_unmapped'].includes(validationType)) &&
    (!Number.isFinite(eventDelta) || Math.abs(eventDelta) <= 0.0001) &&
    !score?.mapped_condition;
  if (filingType === 'administrative') return 'no_thesis_impact';
  if (unvalidatedPositive) return 'needs_assessment';
  if (verdict === 'positive') return 'thesis_improved';
  if (verdict === 'negative') return 'thesis_weakened';
  if (relationship === 'related_unmapped') return 'needs_assessment';
  if (['mixed', 'uncertain', 'unclear'].includes(verdict)) return 'needs_assessment';
  if (verdict === 'neutral') return 'no_thesis_impact';

  const state = routerTrajectoryState(router);
  if (['thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'thesis_weakened';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'thesis_improved';
  if (state === 'needs_classification') return 'needs_assessment';
  if (state === 'administrative_filing') return 'no_thesis_impact';
  if (['no_thesis_change', 'market_backdrop_only', 'material_unmapped'].includes(state)) return 'no_thesis_impact';
  return 'no_thesis_impact';
}

function routerReviewBucket(router) {
  const status = String(routerPresentation(router).reviewStatus || '').trim().toLowerCase();
  if (status === 'open') return 'needs_decision';
  if (status === 'reviewed') return 'reviewed';
  if (status === 'dismissed') return 'dismissed';
  if (status === 'auto_cleared') return 'auto_cleared';
  if (status === 'tracking') return 'tracking';
  return 'unknown';
}

function routerCompanyThesisPath(router) {
  const score = routerTrajectoryScore(router);
  const cumulativePath = String(score?.cumulative_position_band || '').trim().toLowerCase();
  if (COMPANY_THESIS_PATHS.has(cumulativePath)) return cumulativePath;
  const current = String(router?.current_path || '').trim().toLowerCase();
  const baseline = String(router?.baseline_path || '').trim().toLowerCase();
  if (COMPANY_THESIS_PATHS.has(current)) return current;
  if (COMPANY_THESIS_PATHS.has(baseline)) return baseline;
  return 'base';
}

function routerEventTimeMs(row) {
  return parseIsoDateOrNull(row?.saved_at_utc)?.getTime() || parseIsoDateOrNull(row?.received_at_utc)?.getTime() || 0;
}

function routerEventKey(row) {
  return String(row?.event_id || `${row?.ticker || ''}-${row?.saved_at_utc || ''}-${routerTitle(row)}`).trim();
}

function mergeRouterEventRows(currentRows, incomingRows) {
  const byKey = new Map();
  [...(Array.isArray(currentRows) ? currentRows : []), ...(Array.isArray(incomingRows) ? incomingRows : [])].forEach((row) => {
    const key = routerEventKey(row);
    if (key) byKey.set(key, row);
  });
  return [...byKey.values()];
}

function buildCompanyPathMap(events) {
  const rows = Array.isArray(events) ? events : [];
  const latestByTicker = new Map();
  rows.forEach((row) => {
    const ticker = String(row?.ticker || 'n/a').trim().toUpperCase() || 'n/a';
    const current = latestByTicker.get(ticker);
    if (!current || routerEventTimeMs(row) > routerEventTimeMs(current)) {
      latestByTicker.set(ticker, row);
    }
  });
  const pathByTicker = new Map();
  latestByTicker.forEach((row, ticker) => {
    pathByTicker.set(ticker, routerCompanyThesisPath(row));
  });
  return pathByTicker;
}

function companyPathForEvent(row, pathByTicker) {
  const ticker = String(row?.ticker || 'n/a').trim().toUpperCase() || 'n/a';
  return pathByTicker?.get(ticker) || routerCompanyThesisPath(row);
}

function routerTrajectoryScore(router) {
  return router?.trajectory_score && typeof router.trajectory_score === 'object' ? router.trajectory_score : {};
}

function scoreValidationType(score) {
  return String(score?.validation_type || '').trim().toLowerCase();
}

function hasValidatedTrajectoryScore(score) {
  const eventDelta = Number(score?.event_delta);
  return Number.isFinite(eventDelta) && eventDelta !== 0;
}

function isProvisionalTrajectoryScore(score) {
  const validationType = scoreValidationType(score);
  const eventDelta = Number(score?.event_delta);
  const unvalidatedDelta = Number(score?.unvalidated_event_delta);
  if (!Number.isFinite(unvalidatedDelta) || unvalidatedDelta === 0) return false;
  if (validationType === 'related_unmapped' || validationType === 'material_unmapped') return true;
  return !Number.isFinite(eventDelta) || eventDelta === 0;
}

function hasTrajectoryScore(score) {
  return hasValidatedTrajectoryScore(score) || isProvisionalTrajectoryScore(score);
}

function trajectoryScoreDisplayDelta(score) {
  return Number(score?.event_delta);
}

function trajectoryScoreTone(score) {
  const direction = String(score?.direction || '').trim().toLowerCase();
  if (direction === 'positive') return 'positive';
  if (direction === 'negative') return 'urgent';
  if (direction === 'mixed') return 'warn';
  if (isProvisionalTrajectoryScore(score)) return 'warn';
  return 'neutral';
}

function trajectoryScoreTrackLabel(score) {
  return isProvisionalTrajectoryScore(score) ? 'Secondary Analysis' : 'Primary Thesis Analysis';
}

function trajectoryScoreDirectionLabel(score) {
  const direction = String(score?.direction || '').trim().toLowerCase();
  if (direction === 'positive') return 'Positive contribution';
  if (direction === 'negative') return 'Negative contribution';
  if (direction === 'mixed') return 'Mixed contribution';
  return 'No contribution';
}

function safeTrajectoryPositionLabel(score, key = 'position_label') {
  const raw = String(score?.[key] || '').trim();
  if (!raw) return '';
  if (isProvisionalTrajectoryScore(score)) {
    if (/bull case/i.test(raw)) return 'Bull-leaning, unvalidated';
    if (/bear case/i.test(raw)) return 'Bear-leaning, unvalidated';
  }
  return raw
    .replace(/\bBull evidence zone\b/g, 'Bull zone')
    .replace(/\bBear evidence zone\b/g, 'Bear zone')
    .replace(/\bBase evidence zone\b/g, 'Base zone')
    .replace(/\bBull case\b/g, 'Bull zone')
    .replace(/\bBear case\b/g, 'Bear zone')
    .replace(/\bBase case\b/g, 'Base zone');
}

function trajectoryScoreReason(score) {
  const raw = String(score?.reason || '').trim();
  if (!raw) return 'Directional evidence was scored against the saved scenario path.';
  if (!isProvisionalTrajectoryScore(score)) {
    return raw
      .replace(/^Bull-leaning\b/i, 'Positive')
      .replace(/^Bear-leaning\b/i, 'Negative');
  }
  const detail = raw.includes(':') ? raw.slice(raw.indexOf(':') + 1).trim() : raw;
  return `Outside the saved thesis map: ${detail}`
    .replace(/\ba outside saved conditions\b/i, 'outside the saved thesis map')
    .replace(/^Outside the saved thesis map:\s*Related filing outside the saved thesis map;\s*/i, 'Outside the saved thesis map; ');
}

function scoreBandLabel(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '';
  if (num <= -4) return 'Bear';
  if (num <= -2) return 'Bear-leaning';
  if (num < 2) return 'Base';
  if (num < 4) return 'Bull-leaning';
  return 'Bull';
}

function secondaryAnalysisPathLabel(score) {
  const baselineScore = Number(score?.baseline_score);
  const delta = Number(score?.event_delta);
  if (!Number.isFinite(baselineScore) || !Number.isFinite(delta)) return '';
  return scoreBandLabel(baselineScore + delta);
}

function rawSecondaryDelta(score) {
  const delta = Number(score?.raw_secondary_delta ?? score?.unvalidated_event_delta);
  return Number.isFinite(delta) && delta !== 0 ? delta : null;
}

function routerDirectHitCount(router) {
  return routerDetailItems(router, 'matched_condition_details', 'matched_conditions').length +
    routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist').length +
    routerVerificationItems(router).length;
}

function checkedNotTriggeredWatchlist(router) {
  const rows = Array.isArray(router?.watchlist_condition_checks) ? router.watchlist_condition_checks : [];
  return rows.filter((item) => String(item?.status || '').trim().toLowerCase() === 'checked_not_triggered');
}

function routerConflictCount(router) {
  return Array.isArray(router?.conflicts_with_run) ? router.conflicts_with_run.length : 0;
}

function routerIsQuietCleared(router) {
  const state = routerTrajectoryState(router);
  return (
    ['no_thesis_change', 'administrative_filing', 'market_backdrop_only'].includes(state) &&
    !routerDirectHitCount(router) &&
    !routerConflictCount(router) &&
    !checkedNotTriggeredWatchlist(router).length
  );
}

function routerPriorityTone(router) {
  const verdict = routerImpactVerdict(router);
  const state = routerTrajectoryState(router);
  const action = routerActionKey(router);
  const status = String(router?.status || '').trim().toLowerCase();
  const materiality = routerMateriality(router);
  const isMaterial = ['medium', 'high', 'critical'].includes(materiality);
  const isAdministrative = routerFilingType(router) === 'administrative' || state === 'administrative_filing';
  if (status === 'error') return 'urgent';
  if (verdict === 'negative') return 'urgent';
  if (verdict === 'positive') return 'positive';
  if (['mixed', 'uncertain', 'unclear'].includes(verdict)) return 'warn';
  if (['thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'urgent';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'positive';
  if (isAdministrative) return 'neutral';
  if (isMaterial || ['needs_classification', 'material_unmapped'].includes(state)) return 'warn';
  if (verdict === 'neutral') return 'neutral';
  if (['market_backdrop_only', 'no_thesis_change', 'administrative_filing'].includes(state)) return 'neutral';
  if (action === 'urgent_human_review' || action === 'full_rerun') return 'urgent';
  if (action === 'rerun_stage1' || action === 'run_delta_only' || routerConflictCount(router)) return 'alarm';
  if (routerDirectHitCount(router) || action === 'annotate_run') return 'warn';
  if (action === 'watch') return 'neutral';
  return String(routerDisplay(router)?.tone || '').trim() || 'neutral';
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
  const displayEvidence = String(routerDisplay(router)?.evidence_label || '').trim();
  if (displayEvidence) return displayEvidence;
  const state = routerTrajectoryState(router);
  if (routerConflictCount(router)) return 'Saved thesis conflict';
  if (routerDetailItems(router, 'matched_condition_details', 'matched_conditions').length) return 'Thesis condition matched';
  if (routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist').length) return 'Watchlist condition matched';
  if (routerVerificationItems(router).length) return 'Verification item matched';
  if (state === 'material_unmapped') return 'No saved condition match';
  if (state === 'needs_classification') return 'Classification unresolved';
  if (state === 'market_backdrop_only') return 'Market backdrop only';
  if (Array.isArray(router?.market_context_conditions) && router.market_context_conditions.length) return 'Market checked';
  if (!router?.source_url && String(router?.source_type || '').toLowerCase() !== 'exchange_filing') return 'Source not resolved';
  return 'No condition match';
}

function routerPresentation(router) {
  const tone = routerPriorityTone(router);
  const action = routerActionKey(router);
  const display = routerDisplay(router);
  const reviewOverlay = router?.review_overlay && typeof router.review_overlay === 'object' ? router.review_overlay : {};
  const trajectoryLabel = display.trajectory_label || routerOutcomeLabel(router);
  return {
    tone,
    action,
    bucket: routerActionBucket(router),
    trajectoryState: routerTrajectoryState(router),
    trajectoryLabel,
    reviewLabel: display.review_label || (routerActionBucket(router) === 'open_review' ? 'Needs thesis decision' : 'Auto-cleared'),
    reviewStatus: display.review_status || '',
    reviewNote: reviewOverlay.review_note || '',
    queueLabel: display.queue_label || titleizeKey(routerActionBucket(router)),
    actionLabel: display.system_action_label || labelScenarioAction(action),
    outcomeLabel: trajectoryLabel,
    evidenceLabel: routerEvidenceState(router),
    primaryReason: display.primary_reason || routerVerdictCopy(router),
    needsAttention: Boolean(display.is_user_action_required) || routerNeedsAttention(router),
  };
}

function pathExplanation(router) {
  const path = String(router?.baseline_path || router?.current_path || '').trim();
  if (!path) return '';
  return `${labelCompanyThesisPath(path)} is the company-level saved view from the latest council run before this announcement. Announcement verdicts are assessed separately.`;
}

function conditionTone(status, group = '') {
  const normalized = String(status || '').trim().toLowerCase();
  const normalizedGroup = String(group || '').trim().toLowerCase();
  if (normalized === 'matched' && ['failure', 'red_flag'].includes(normalizedGroup)) return 'contradict';
  if (normalized === 'matched') return 'confirm';
  if (normalized === 'partial_match') return 'unclear';
  if (normalized === 'checked_not_triggered') return 'muted';
  if (normalized === 'contradicted') return 'contradict';
  if (normalized === 'unclear') return 'unclear';
  return 'muted';
}

function conditionStatusLabel(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'not_matched') return 'Not matched';
  if (normalized === 'matched') return 'Matched';
  if (normalized === 'partial_match') return 'Partially matched';
  if (normalized === 'checked_not_triggered') return 'Checked, not triggered';
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
    { total: 0, matched: 0, partial_match: 0, checked_not_triggered: 0, not_matched: 0, contradicted: 0, unclear: 0, unknown: 0 }
  );
}

function compactStatusSummary(items, empty = 'none checked') {
  const counts = countStatuses(items);
  if (!counts.total) return empty;
  const parts = [
    counts.matched && `${counts.matched} matched`,
    counts.partial_match && `${counts.partial_match} partial`,
    counts.checked_not_triggered && `${counts.checked_not_triggered} not triggered`,
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
  const checkedWatchlist = checkedNotTriggeredWatchlist(router);
  if (directHits) return 'Mapped evidence was found';
  if (checkedWatchlist.length) return 'Watchlist risk checked, not triggered';
  if (state === 'needs_classification') return 'Classification is unresolved';
  if (state === 'material_unmapped') return 'Material filing is not covered by the thesis map';
  if (state === 'administrative_filing') return 'Procedural filing with no mapped thesis impact';
  if (state === 'market_backdrop_only') return 'Market context only';
  if (state === 'no_thesis_change') return 'No mapped thesis condition changed';
  if (['thesis_weakened', 'timeline_delayed', 'risk_increased'].includes(state)) return 'Company thesis path is under pressure';
  if (['thesis_strengthened', 'timeline_accelerated', 'risk_reduced'].includes(state)) return 'Company thesis path improved';
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
  const checkedWatchlist = checkedNotTriggeredWatchlist(router);
  if (checkedWatchlist.length) {
    const first = checkedWatchlist[0] || {};
    const label = routerItemLabel(first) || 'a saved watchlist item';
    const reason = String(first.reason || '').trim();
    return [
      `The router compared the filing against "${label}" and did not treat it as triggered.`,
      reason || 'The filing touched the risk area, but did not establish the watched condition.',
    ].filter(Boolean).join(' ');
  }
  if (state === 'needs_classification') {
    return router?.classification_reason || 'The filing was captured, but the classifier could not map it to a known filing type or saved thesis driver. It should be labelled by a human rather than treated as urgent thesis damage.';
  }
  if (state === 'material_unmapped') {
    return 'The filing appears material, but none of the saved thesis conditions matched it. That points to a thesis-map gap, not a clean no-impact result.';
  }
  if (state === 'administrative_filing') {
    return 'The filing is procedural or administrative, and no saved thesis or watchlist condition matched it.';
  }
  if (state === 'market_backdrop_only' || (marketCount && !directHits)) {
    return 'The announcement itself did not match a saved thesis condition. Market facts were checked as backdrop only and are not the filing verdict.';
  }
  if (state === 'no_thesis_change') {
    return 'The filing was checked against the saved announcement and watchlist conditions; none changed the company thesis path.';
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

function PathTransition({ router, companyPath = '' }) {
  const transition = splitScenarioTransition(router?.path_transition);
  const baseline = transition?.from || String(router?.baseline_path || router?.current_path || '').trim().toLowerCase();
  const current = transition?.to || String(router?.current_path || router?.baseline_path || '').trim().toLowerCase();
  const changed = Boolean(transition && transition.from !== transition.to);
  const normalizedCompanyPath = String(companyPath || '').trim().toLowerCase();
  const displayPath = COMPANY_THESIS_PATHS.has(normalizedCompanyPath)
    ? normalizedCompanyPath
    : COMPANY_THESIS_PATHS.has(current)
      ? current
      : COMPANY_THESIS_PATHS.has(baseline)
        ? baseline
        : 'base';
  const pathCopy = COMPANY_THESIS_PATHS.has(normalizedCompanyPath)
    ? 'Current path'
    : changed
      ? `Moved from ${labelCompanyThesisPathShort(baseline)} after this filing`
      : 'Current saved path';
  return (
    <div className="announcement-router-path-bar">
      <div className="announcement-router-path-main">
        <span className="announcement-router-path-label">Thesis path</span>
        <div className="announcement-router-path-stack" aria-label="Thesis path">
          {['bull', 'base', 'bear'].map((name) => (
            <span
              key={name}
              className={`announcement-router-path-pill tone-${name} ${displayPath === name ? 'is-on' : ''}`}
            >
              {labelCompanyThesisPathShort(name)}
            </span>
          ))}
        </div>
        <span className="announcement-router-path-arrow">{pathCopy}</span>
      </div>
      <div className="announcement-router-path-effect">
        <span>Announcement effect</span>
        <strong>{routerOutcomeLabel(router)}</strong>
      </div>
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

function MarketPathProjection({ router, actualPrices = [], routerEvents = [] }) {
  const [showPriceHistory, setShowPriceHistory] = useState(true);
  const [showRouterEvents, setShowRouterEvents] = useState(true);
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
  const chartStartDate = projection?.baseline_started_at_utc || projection?.as_of_utc || router?.saved_at_utc || '';

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
      <div className="lab-chart-toolbar is-compact" aria-label="Market path overlays">
        <span>Overlays</span>
        <button
          type="button"
          className={showPriceHistory ? 'is-active' : ''}
          onClick={() => setShowPriceHistory((value) => !value)}
        >
          Price history
        </button>
        <button
          type="button"
          className={showRouterEvents ? 'is-active' : ''}
          onClick={() => setShowRouterEvents((value) => !value)}
        >
          Router events
        </button>
      </div>
      {hasChart && (
        <ScenarioTimelineUnit
          data={chartData}
          currency={currency}
          timelineBars={timelineBars}
          orientation="vertical"
          actualPrices={actualPrices}
          routerEvents={routerEvents}
          showPriceHistory={showPriceHistory}
          showRouterEvents={showRouterEvents}
          startDate={chartStartDate}
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

function TrajectoryScorePanel({ router }) {
  const score = routerTrajectoryScore(router);
  if (!hasTrajectoryScore(score)) return null;
  const tickerSignal = Number(score?.cumulative_delta);
  const eventLabel = safeTrajectoryPositionLabel(score, 'position_label') || 'Position not assessed';
  const tickerSignalLabel = safeTrajectoryPositionLabel(score, 'cumulative_position_label') || eventLabel;
  const isProvisional = isProvisionalTrajectoryScore(score);
  const eventDelta = trajectoryScoreDisplayDelta(score);
  const secondaryPathLabel = secondaryAnalysisPathLabel(score);
  const rawDelta = rawSecondaryDelta(score);
  return (
    <div className="announcement-router-score-panel" data-tone={trajectoryScoreTone(score)}>
      <span>{trajectoryScoreTrackLabel(score)}</span>
      <strong>
        {trajectoryScoreDirectionLabel(score)}
        <em>{fmtSignedDelta(eventDelta)}</em>
      </strong>
      <p>{trajectoryScoreReason(score)}</p>
      <div>
        <b>
          {isProvisional
            ? `Reduced weight: ${secondaryPathLabel || eventLabel}`
            : 'Full weight'}
        </b>
        {isProvisional && rawDelta !== null && (
          <b>Raw model assessment: {fmtSignedDelta(rawDelta)}</b>
        )}
        {Number.isFinite(tickerSignal) && (
          <b>Router Score: {tickerSignalLabel || 'Position not assessed'} ({fmtSignedDelta(tickerSignal)})</b>
        )}
      </div>
    </div>
  );
}

function ReviewWorkflow({ router, onReviewAction, reviewBusy = '' }) {
  const [isAddingNote, setIsAddingNote] = useState(false);
  const [note, setNote] = useState('');
  const vm = routerPresentation(router);
  const isTerminalReview = ['reviewed', 'dismissed'].includes(vm.reviewStatus);
  const visibleNote = isGeneratedReviewNote(vm.reviewNote, router) ? '' : vm.reviewNote;

  if (!onReviewAction || !router?.event_id) return null;
  if (isTerminalReview) return null;
  if (vm.reviewStatus === 'auto_cleared') {
    if (!visibleNote) return null;
    return (
      <div className="announcement-router-review-workflow">
        <div className="announcement-router-review-state">
          <span>Note</span>
          <p>{visibleNote}</p>
        </div>
      </div>
    );
  }

  const submitSimple = (status) => {
    onReviewAction(router, status, {
      review_note: defaultReviewNote(router, status),
      review_owner: 'analyst',
    });
  };

  const submitNote = () => {
    onReviewAction(router, 'open', {
      review_note: note.trim(),
      review_owner: 'analyst',
    });
    setIsAddingNote(false);
  };

  return (
    <div className="announcement-router-review-workflow">
      {!!visibleNote && (
        <div className="announcement-router-review-state">
          <span>Note</span>
          <p>{visibleNote}</p>
        </div>
      )}

      {isAddingNote && (
        <div className="announcement-router-follow-up-card">
          <div className="announcement-router-follow-up-fields">
            <label className="is-wide">
              <span>Analyst note</span>
              <textarea
                value={note}
                onChange={(event) => setNote(event.target.value)}
                placeholder="Optional context for the next review step"
                rows={3}
              />
            </label>
          </div>
        </div>
      )}

      <div className="announcement-router-review-actions" aria-label="Review actions">
        {isAddingNote && (
          <button
            type="button"
            className="announcement-router-review-primary"
            disabled={Boolean(reviewBusy) || !note.trim()}
            onClick={submitNote}
          >
            {reviewBusy ? 'Saving...' : 'Save note'}
          </button>
        )}
        <button
          type="button"
          disabled={Boolean(reviewBusy)}
          onClick={() => submitSimple('reviewed')}
        >
          {reviewBusy === 'reviewed' ? 'Saving...' : 'Mark handled'}
        </button>
        <button
          type="button"
          disabled={Boolean(reviewBusy)}
          onClick={() => submitSimple('dismissed')}
        >
          {reviewBusy === 'dismissed' ? 'Saving...' : 'Clear as no change'}
        </button>
        <button
          type="button"
          disabled={Boolean(reviewBusy)}
          onClick={() => setIsAddingNote((value) => !value)}
        >
          {isAddingNote ? 'Hide note' : 'Add note'}
        </button>
      </div>
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

function RouterKpiStrip({ title, filters, activeFilter, onSelect, ariaLabel }) {
  return (
    <section className="announcement-router-filter-group">
      {title && <h4>{title}</h4>}
      <div className="announcement-router-kpi-strip" role="tablist" aria-label={ariaLabel || title || 'Filing queue filters'}>
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
    </section>
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
        <h4>Announcement Queue</h4>
        <span>{events.length} shown</span>
      </div>
      <div className="announcement-router-queue-list">
        {events.map((row) => {
          const vm = routerPresentation(row);
          const score = routerTrajectoryScore(row);
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
                    {hasTrajectoryScore(score) && (
                      <em className="announcement-router-queue-delta" data-tone={trajectoryScoreTone(score)}>
	                        {fmtSignedDelta(trajectoryScoreDisplayDelta(score))}
                      </em>
                    )}
	                </span>
	                <span className="announcement-router-queue-title">{routerTitle(row)}</span>
	                <span className="announcement-router-queue-meta">
                      {hasTrajectoryScore(score) && <span>{trajectoryScoreTrackLabel(score)}</span>}
		                  <span>{routerMaterialityLabel(row)}</span>
		                  <span>{routerDriverLabel(row)}</span>
		                  {vm.reviewStatus && vm.reviewStatus !== 'auto_cleared' && <span>{vm.reviewLabel}</span>}
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

function buildCompanyQueueGroups(events, companyPathByTicker) {
  const byTicker = new Map();
  (Array.isArray(events) ? events : []).forEach((row) => {
    const ticker = String(row?.ticker || 'n/a').trim().toUpperCase() || 'n/a';
    if (!byTicker.has(ticker)) byTicker.set(ticker, []);
    byTicker.get(ticker).push(row);
  });
  return [...byTicker.entries()]
    .map(([ticker, rows]) => {
      const sortedRows = [...rows].sort((a, b) => routerEventTimeMs(b) - routerEventTimeMs(a));
      const latestEvent = sortedRows[0] || {};
      const pathBasis = sortedRows
        .filter((row) => {
          const delta = Number(routerTrajectoryScore(row)?.event_delta);
          return Number.isFinite(delta) && delta !== 0;
        })
        .sort((a, b) => Math.abs(Number(routerTrajectoryScore(b)?.event_delta || 0)) - Math.abs(Number(routerTrajectoryScore(a)?.event_delta || 0)));
      return {
        ticker,
        path: companyPathByTicker?.get(ticker) || routerCompanyThesisPath(latestEvent),
        latestEvent,
        pathBasis,
        events: sortedRows,
      };
    })
    .sort((a, b) => a.ticker.localeCompare(b.ticker));
}

function CompanyPathQueue({ companies, selectedEventId, onSelect, onLoadAll, expandedTickers, busyTicker }) {
  const announcementCount = companies.reduce((sum, company) => sum + company.events.length, 0);
  return (
    <article className="announcement-router-queue announcement-router-company-queue">
      <div className="announcement-router-queue-head">
        <h4>Company Thesis</h4>
        <span>{companies.length} companies · {announcementCount} filings</span>
      </div>
      <div className="announcement-router-queue-list">
        {companies.map((company) => {
          const latestVm = routerPresentation(company.latestEvent);
          const isBusy = busyTicker === company.ticker;
          const isExpanded = expandedTickers?.has(company.ticker);
          const visibleBasis = isExpanded
            ? company.pathBasis
            : company.pathBasis.slice(0, COMPANY_SUMMARY_BASIS_LIMIT);
          const visibleEvents = isExpanded
            ? company.events
            : company.events.slice(0, COMPANY_SUMMARY_EVENT_LIMIT);
          const hiddenCount = Math.max(0, company.events.length - visibleEvents.length);
          return (
            <section className="announcement-router-company-group" key={company.ticker}>
              <div className="announcement-router-company-head">
                <div>
                  <strong>{company.ticker}</strong>
                  <span>
                    {visibleEvents.length} of {company.events.length} filing{company.events.length === 1 ? '' : 's'} shown
                  </span>
                </div>
                <div className="announcement-router-company-actions">
                  {hiddenCount > 0 ? (
                    <button
                      type="button"
                      className="announcement-router-company-load-all"
                      disabled={isBusy}
                      onClick={() => onLoadAll?.(company.ticker)}
                    >
                      {isBusy ? 'Loading...' : `Show all ${company.events.length} announcements`}
                    </button>
                  ) : (
                    <span className="announcement-router-company-loaded">All shown</span>
                  )}
                  <b className={`announcement-router-company-path tone-${company.path}`}>
                    {labelCompanyThesisPath(company.path)}
                  </b>
                </div>
              </div>
              <div className="announcement-router-company-latest">
                <span>Latest filing</span>
                <strong>{latestVm.trajectoryLabel}</strong>
                <em>{routerTitle(company.latestEvent)}</em>
              </div>
              {!!visibleBasis.length && (
                <div className="announcement-router-company-basis">
                  <span>Path basis</span>
                  {visibleBasis.map((row) => {
                    const score = routerTrajectoryScore(row);
                    return (
                      <button
                        type="button"
                        key={`basis-${row.event_id || `${row.ticker}-${row.saved_at_utc}-${routerTitle(row)}`}`}
                        onClick={() => onSelect(row.event_id || '')}
                      >
                        <em data-tone={trajectoryScoreTone(score)}>{fmtSignedDelta(trajectoryScoreDisplayDelta(score))}</em>
                        <strong>{routerTitle(row)}</strong>
                        <small>{row.saved_at_utc ? fmtRelativeSince(row.saved_at_utc) : 'n/a'}</small>
                      </button>
                    );
                  })}
                </div>
              )}
              <div className="announcement-router-company-events">
                {visibleEvents.map((row) => {
                  const vm = routerPresentation(row);
                  const score = routerTrajectoryScore(row);
                  const selected = selectedEventId === row.event_id;
                  return (
                    <button
                      type="button"
                      className={`announcement-router-company-event ${selected ? 'is-selected' : ''}`}
                      data-tone={vm.tone}
                      key={row.event_id || `${row.ticker}-${row.saved_at_utc}-${routerTitle(row)}`}
                      onClick={() => onSelect(row.event_id || '')}
                    >
                      <span className="announcement-router-company-event-dot" />
                      <span className="announcement-router-company-event-body">
                        <span>
                          <b>{vm.trajectoryLabel}</b>
                          {hasTrajectoryScore(score) && (
                            <em data-tone={trajectoryScoreTone(score)}>{fmtSignedDelta(trajectoryScoreDisplayDelta(score))}</em>
                          )}
                        </span>
                        <strong>{routerTitle(row)}</strong>
                        <small>{row.saved_at_utc ? fmtRelativeSince(row.saved_at_utc) : 'n/a'} · {routerMaterialityLabel(row)}</small>
                      </span>
                    </button>
                  );
                })}
              </div>
              {hiddenCount > 0 && (
                <button
                  type="button"
                  className="announcement-router-company-more"
                  disabled={isBusy}
                  onClick={() => onLoadAll?.(company.ticker)}
                >
                  {isBusy ? 'Loading...' : `${hiddenCount} older announcement${hiddenCount === 1 ? '' : 's'} hidden`}
                </button>
              )}
            </section>
          );
        })}
        {!companies.length && <div className="watch-empty">No companies in this thesis path.</div>}
      </div>
    </article>
  );
}

function eventMatchesCaseFilter(row, filterKey) {
  if (filterKey === 'all') return true;
  return routerCaseBucket(row) === filterKey;
}

function eventMatchesReviewFilter(row, filterKey) {
  if (filterKey === 'all') return true;
  return routerReviewBucket(row) === filterKey;
}

function eventMatchesScenarioFilter(row, filterKey, companyPathByTicker) {
  if (filterKey === 'all') return true;
  return companyPathForEvent(row, companyPathByTicker) === filterKey;
}

function eventMatchesFilters(row, { caseFilter = 'all', reviewFilter = 'all', scenarioFilter = 'all', companyPathByTicker = null } = {}) {
  return (
    eventMatchesCaseFilter(row, caseFilter) &&
    eventMatchesReviewFilter(row, reviewFilter) &&
    eventMatchesScenarioFilter(row, scenarioFilter, companyPathByTicker)
  );
}

function countEventsMatching(events, filters) {
  return events.filter((row) => eventMatchesFilters(row, filters)).length;
}

function countEventsByCase(events, key, activeReviewFilter, activeScenarioFilter, companyPathByTicker) {
  if (key === 'all') {
    return countEventsMatching(events, { reviewFilter: activeReviewFilter, scenarioFilter: activeScenarioFilter, companyPathByTicker });
  }
  return countEventsMatching(events, { caseFilter: key, reviewFilter: activeReviewFilter, scenarioFilter: activeScenarioFilter, companyPathByTicker });
}

function countEventsByReview(events, key, activeCaseFilter, activeScenarioFilter, companyPathByTicker) {
  if (key === 'all') {
    return countEventsMatching(events, { caseFilter: activeCaseFilter, scenarioFilter: activeScenarioFilter, companyPathByTicker });
  }
  return countEventsMatching(events, { caseFilter: activeCaseFilter, reviewFilter: key, scenarioFilter: activeScenarioFilter, companyPathByTicker });
}

function countCompaniesByScenario(companyPathByTicker, key) {
  const paths = [...(companyPathByTicker?.values() || [])];
  if (key === 'all') return paths.length;
  return paths.filter((path) => path === key).length;
}

function activeFilterSummary(activeFilterGroup, caseFilter, reviewFilter, scenarioFilter) {
  if (activeFilterGroup === 'case' && caseFilter === 'all') return 'All announcements';
  if (activeFilterGroup === 'review' && reviewFilter === 'all') return 'All review states';
  if (activeFilterGroup === 'scenario' && scenarioFilter === 'all') return 'All company theses';
  const active = [
    activeFilterGroup === 'case' && caseFilter !== 'all' && `Case: ${titleizeKey(caseFilter)}`,
    activeFilterGroup === 'review' && reviewFilter !== 'all' && `Review: ${titleizeKey(reviewFilter)}`,
    activeFilterGroup === 'scenario' && scenarioFilter !== 'all' && `Path: ${labelCompanyThesisPathShort(scenarioFilter)}`,
  ].filter(Boolean);
  return active.length ? active.join(' | ') : 'All filings';
}

function resetFilters(setCaseFilter, setReviewFilter, setScenarioFilter, setActiveFilterGroup, setSelectedEventId) {
  setCaseFilter('all');
  setReviewFilter('all');
  setScenarioFilter('all');
  setActiveFilterGroup('');
  setSelectedEventId('');
}

function hasActiveFilters(activeFilterGroup, caseFilter, reviewFilter, scenarioFilter) {
  return Boolean(
    (activeFilterGroup === 'case' && caseFilter !== 'all') ||
    (activeFilterGroup === 'review' && reviewFilter !== 'all') ||
    (activeFilterGroup === 'scenario' && scenarioFilter !== 'all')
  );
}

function DecisionPanel({
  router,
  emptyTitle = 'No announcement decision attached',
  emptyCopy = '',
  onReviewAction = null,
  reviewBusy = '',
  priceHistory = [],
  routerEvents = [],
  companyPath = '',
}) {
  const decisionAvailable = hasRouterDecision(router);
  const matchedConditions = routerDetailItems(router, 'matched_condition_details', 'matched_conditions');
  const watchHits = routerDetailItems(router, 'triggered_watchlist_details', 'triggered_watchlist');
  const verificationHits = routerVerificationItems(router);
  const findings = Array.isArray(router?.key_findings) ? router.key_findings : [];
  const conflicts = Array.isArray(router?.conflicts_with_run) ? router.conflicts_with_run : [];
  const marketConditions = Array.isArray(router?.market_context_conditions) ? router.market_context_conditions : [];
  const announcementChecks = Array.isArray(router?.announcement_condition_checks) ? router.announcement_condition_checks : [];
  const watchlistChecks = Array.isArray(router?.watchlist_condition_checks) ? router.watchlist_condition_checks : [];
  const checkedWatchlistRows = checkedNotTriggeredWatchlist(router);
  const verificationChecks = Array.isArray(router?.verification_condition_checks) ? router.verification_condition_checks : [];
  const directHits = matchedConditions.length + watchHits.length + verificationHits.length;
  const vm = routerPresentation(router);
  const classificationConfidence = router?.classification_confidence ?? router?.semantic_confidence ?? router?.parser_confidence;
  const filingSummary = routerFilingSummary(router);
  const quietCleared = routerIsQuietCleared(router);
  const whyCount = directHits
    ? `${directHits} mapped condition${directHits === 1 ? '' : 's'}`
    : checkedWatchlistRows.length
      ? `${checkedWatchlistRows.length} watchlist checked`
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
        <div className="announcement-router-decision-summary">
          <div className="announcement-router-decision-verdict">
            <span>Trajectory</span>
            <strong>{vm.trajectoryLabel}</strong>
            <p>{vm.primaryReason}</p>
          </div>
        </div>
        {filingSummary && <p className="announcement-router-filing-summary">{filingSummary}</p>}
        <TrajectoryScorePanel router={router} />
        <div className="announcement-router-hero-actions">
          {router?.source_url && (
            <a className="announcement-router-primary-link" href={router.source_url} target="_blank" rel="noreferrer">Open filing</a>
          )}
          <span className="announcement-router-hero-metric">{classificationConfidence != null && classificationConfidence !== '' ? `Classification confidence ${fmtPct(classificationConfidence)}` : 'Classification confidence n/a'}</span>
          <span className="announcement-router-hero-tag">{router?.source_type ? titleizeKey(router.source_type) : 'Source unresolved'}</span>
        </div>
        <ReviewWorkflow
          key={router?.event_id || routerTitle(router)}
          router={router}
          onReviewAction={onReviewAction}
          reviewBusy={reviewBusy}
        />
      </div>
      <PathTransition router={router} companyPath={companyPath} />
      <MarketPathProjection router={router} actualPrices={priceHistory} routerEvents={routerEvents} />

      <RouterSection title="Why This Verdict" count={whyCount} muted={quietCleared}>
        <div className={`announcement-router-impact-callout ${directHits ? 'has-hit' : 'no-hit'}`}>
          <strong>{routerWhyHeadline(router, directHits)}</strong>
          <span>{routerWhyCopy(router, directHits)}</span>
        </div>
        <ConfidenceBreakdown router={router} />
        <EvidenceRows title="Matched Thesis Conditions" items={matchedConditions} />
        <EvidenceRows title="Watchlist Matches" items={watchHits} />
        <ConditionChecks title="Watchlist Checked, Not Triggered" items={checkedWatchlistRows} />
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
  const [activeFilterGroup, setActiveFilterGroup] = useState('');
  const [activeCaseFilter, setActiveCaseFilter] = useState('all');
  const [activeReviewFilter, setActiveReviewFilter] = useState('all');
  const [activeScenarioFilter, setActiveScenarioFilter] = useState('all');
  const [reviewBusy, setReviewBusy] = useState('');
  const [companyFetchBusy, setCompanyFetchBusy] = useState('');
  const [expandedCompanyTickers, setExpandedCompanyTickers] = useState(() => new Set());
  const [selectedPriceHistory, setSelectedPriceHistory] = useState([]);

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
        const payload = await api.getAnnouncementRouterOverview(ROUTER_MONITOR_LIMIT, effectiveTicker);
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
      return routerEventTimeMs(b) - routerEventTimeMs(a);
    }),
    [recentEvents]
  );
  const companyPathByTicker = useMemo(
    () => buildCompanyPathMap(queueEvents),
    [queueEvents]
  );
  const caseFilters = useMemo(() => {
    const count = (key) => countEventsByCase(queueEvents, key, 'all', 'all', companyPathByTicker);
    return [
      { key: 'all', label: 'All case types', tone: 'neutral', count: count('all') },
      { key: 'thesis_improved', label: 'Thesis improved', tone: 'positive', count: count('thesis_improved') },
      { key: 'thesis_weakened', label: 'Thesis weakened', tone: 'urgent', count: count('thesis_weakened') },
      { key: 'needs_assessment', label: 'Needs assessment', tone: 'warn', count: count('needs_assessment') },
      { key: 'no_thesis_impact', label: 'No thesis impact', tone: 'neutral', count: count('no_thesis_impact') },
    ];
  }, [queueEvents, companyPathByTicker]);
  const reviewFilters = useMemo(() => {
    const count = (key) => countEventsByReview(queueEvents, key, 'all', 'all', companyPathByTicker);
    return [
      { key: 'all', label: 'All review states', tone: 'neutral', count: count('all') },
      { key: 'needs_decision', label: 'Needs decision', tone: 'warn', count: count('needs_decision') },
      { key: 'reviewed', label: 'Reviewed', tone: 'neutral', count: count('reviewed') },
      { key: 'dismissed', label: 'Cleared', tone: 'neutral', count: count('dismissed') },
      { key: 'auto_cleared', label: 'Auto-cleared', tone: 'neutral', count: count('auto_cleared') },
      { key: 'tracking', label: 'Tracking', tone: 'info', count: count('tracking') },
    ];
  }, [queueEvents, companyPathByTicker]);
  const scenarioFilters = useMemo(() => {
    const count = (key) => countCompaniesByScenario(companyPathByTicker, key);
    return [
      { key: 'all', label: 'All', tone: 'neutral', count: count('all') },
      { key: 'bull', label: 'Bull', tone: 'positive', count: count('bull') },
      { key: 'base', label: 'Base', tone: 'warn', count: count('base') },
      { key: 'bear', label: 'Bear', tone: 'urgent', count: count('bear') },
    ];
  }, [companyPathByTicker]);
  const filteredEvents = useMemo(
    () => queueEvents.filter((row) => eventMatchesFilters(row, {
      caseFilter: activeFilterGroup === 'case' ? activeCaseFilter : 'all',
      reviewFilter: activeFilterGroup === 'review' ? activeReviewFilter : 'all',
      scenarioFilter: activeFilterGroup === 'scenario' ? activeScenarioFilter : 'all',
      companyPathByTicker,
    })),
    [queueEvents, activeFilterGroup, activeCaseFilter, activeReviewFilter, activeScenarioFilter, companyPathByTicker]
  );
  const filteredCompanyGroups = useMemo(
    () => buildCompanyQueueGroups(filteredEvents, companyPathByTicker),
    [filteredEvents, companyPathByTicker]
  );
  const selectedEvent = useMemo(
    () => filteredEvents.find((row) => row?.event_id === selectedEventId) || filteredEvents[0] || {},
    [filteredEvents, selectedEventId]
  );
  const selectedRouter = useMemo(
    () => (embedded ? (runRouter || {}) : selectedEvent),
    [embedded, runRouter, selectedEvent]
  );
  const selectedRouterTicker = String(selectedRouter?.ticker || effectiveTicker || '').trim().toUpperCase();
  const selectedRouterEvents = useMemo(() => {
    if (!selectedRouterTicker) return [];
    const rows = queueEvents.filter((row) => String(row?.ticker || '').trim().toUpperCase() === selectedRouterTicker);
    if (selectedRouter?.event_id && !rows.some((row) => row?.event_id === selectedRouter.event_id)) {
      rows.push(selectedRouter);
    }
    return rows;
  }, [queueEvents, selectedRouter, selectedRouterTicker]);

  useEffect(() => {
    if (!selectedRouterTicker) {
      setSelectedPriceHistory([]);
      return undefined;
    }
    let cancelled = false;
    const loadPriceHistory = async () => {
      try {
        const payload = await api.getSecurityPriceHistory(selectedRouterTicker);
        if (!cancelled) setSelectedPriceHistory(Array.isArray(payload?.points) ? payload.points : []);
      } catch {
        if (!cancelled) setSelectedPriceHistory([]);
      }
    };
    loadPriceHistory();
    return () => {
      cancelled = true;
    };
  }, [selectedRouterTicker]);

  const openFullMonitor = useCallback(() => {
    if (onOpenFullMonitor) {
      onOpenFullMonitor();
      return;
    }
    openAnnouncementRouter();
  }, [onOpenFullMonitor]);

  const loadAllCompanyAnnouncements = useCallback(async (ticker) => {
    const cleanTicker = String(ticker || '').trim().toUpperCase();
    if (!cleanTicker) return;
    setCompanyFetchBusy(cleanTicker);
    try {
      const payload = await api.listAnnouncementRouterEvents(ROUTER_MONITOR_LIMIT, cleanTicker);
      const rows = Array.isArray(payload?.events) ? payload.events : [];
      setOverview((current) => ({
        ...(current || {}),
        recent_events: mergeRouterEventRows(current?.recent_events, rows),
      }));
      setExpandedCompanyTickers((current) => {
        const next = new Set(current);
        next.add(cleanTicker);
        return next;
      });
    } catch (err) {
      setError(err?.message || `Failed to load announcements for ${cleanTicker}`);
    } finally {
      setCompanyFetchBusy('');
    }
  }, []);

  const updateReview = useCallback(async (row, status, options = {}) => {
    const eventId = String(row?.event_id || '').trim();
    const reviewStatus = String(status || '').trim().toLowerCase();
    if (!eventId || !reviewStatus) return;
    setReviewBusy(reviewStatus);
    try {
      const payload = await api.updateAnnouncementRouterReview(eventId, {
        review_status: reviewStatus,
        review_note: options.review_note || `Router event ${reviewStatus} from announcement monitor.`,
        reviewed_by: 'analyst',
        review_owner: options.review_owner || 'analyst',
      });
      const review = payload?.review || {};
      setOverview((current) => {
        if (!current || !Array.isArray(current.recent_events)) return current;
        return {
          ...current,
          recent_events: current.recent_events.map((item) => applyReviewOverlayToEvent(item, review)),
        };
      });
    } catch (err) {
      setError(err?.message || 'Failed to update router review');
    } finally {
      setReviewBusy('');
    }
  }, []);

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
            priceHistory={selectedPriceHistory}
            routerEvents={selectedRouterEvents}
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
	                Tracks new filings against the saved thesis map and shows which ones need a human thesis decision.
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
	                <span>{activeFilterSummary(activeFilterGroup, activeCaseFilter, activeReviewFilter, activeScenarioFilter)}</span>
                <strong>{filteredEvents.length}</strong>
              </div>
              <RouterKpiStrip
                title="Announcements"
                filters={caseFilters}
                activeFilter={activeFilterGroup === 'case' ? activeCaseFilter : ''}
                onSelect={(key) => {
                  setActiveCaseFilter(key);
                  setActiveReviewFilter('all');
                  setActiveScenarioFilter('all');
                  setActiveFilterGroup('case');
                  setSelectedEventId('');
                }}
              />
              <RouterKpiStrip
                title="Company Thesis"
                filters={scenarioFilters}
                activeFilter={activeFilterGroup === 'scenario' ? activeScenarioFilter : ''}
                onSelect={(key) => {
                  setActiveCaseFilter('all');
                  setActiveReviewFilter('all');
                  setActiveScenarioFilter(key);
                  setActiveFilterGroup('scenario');
                  setSelectedEventId('');
                }}
              />
              <RouterKpiStrip
                title="Review State"
                filters={reviewFilters}
                activeFilter={activeFilterGroup === 'review' ? activeReviewFilter : ''}
                onSelect={(key) => {
                  setActiveCaseFilter('all');
                  setActiveReviewFilter(key);
                  setActiveScenarioFilter('all');
                  setActiveFilterGroup('review');
                  setSelectedEventId('');
                }}
              />
              {hasActiveFilters(activeFilterGroup, activeCaseFilter, activeReviewFilter, activeScenarioFilter) && (
                <button
                  type="button"
                  className="announcement-router-reset-filters"
                  onClick={() => resetFilters(setActiveCaseFilter, setActiveReviewFilter, setActiveScenarioFilter, setActiveFilterGroup, setSelectedEventId)}
                >
                  Clear filters
                </button>
              )}
            </aside>

            <div className="announcement-router-main-stage">
              <div className="announcement-router-workspace">
                {activeFilterGroup === 'scenario' ? (
                  <CompanyPathQueue
                    companies={filteredCompanyGroups}
                    selectedEventId={selectedEvent?.event_id || selectedEventId}
                    onSelect={setSelectedEventId}
                    onLoadAll={loadAllCompanyAnnouncements}
                    expandedTickers={expandedCompanyTickers}
                    busyTicker={companyFetchBusy}
                  />
                ) : (
                  <RouterQueue
                    events={filteredEvents}
                    selectedEventId={selectedEvent?.event_id || selectedEventId}
                    onSelect={setSelectedEventId}
                  />
                )}
                <DecisionPanel
                  router={selectedRouter}
                  emptyTitle="No routed announcements yet"
                  onReviewAction={updateReview}
                  reviewBusy={selectedRouter?.event_id ? reviewBusy : ''}
                  priceHistory={selectedPriceHistory}
                  routerEvents={selectedRouterEvents}
                  companyPath={companyPathByTicker.get(selectedRouterTicker)}
                />
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
