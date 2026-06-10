import { useCallback, useEffect, useMemo, useState } from 'react';
import { mapStage3ToGanttModel } from '../utils/stage3GanttMapper';
import { api } from '../api';
import AnnouncementRouterMonitor from './AnnouncementRouterMonitor';
import ScenarioTimelineUnit from './ScenarioTimelineUnit';
import './GanttIntelligenceLab.css';

function backToCouncilChat() {
  if (typeof window === 'undefined') return;
  const current = window.location.pathname;
  if (current === '/') return;
  window.history.pushState({}, '', '/');
  window.dispatchEvent(new Event('app:navigate'));
}

function navigateTo(pathname) {
  if (typeof window === 'undefined') return;
  const current = window.location.pathname;
  if (current === pathname) return;
  window.history.pushState({}, '', pathname);
  window.dispatchEvent(new Event('app:navigate'));
}

function fmtNum(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  return n.toFixed(digits);
}

function fmtScore(value, digits = 1) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  return `${n.toFixed(digits)}`;
}

function fmtScorePct(value, digits = 1) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  return `${n.toFixed(digits)}%`;
}

function fmtPct(value) {
  if (value == null || value === '') return 'n/a';
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  const pct = n <= 1 ? n * 100 : n;
  return `${Math.round(pct)}%`;
}

function fmtMoney(value, currency = 'AUD') {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  const code = String(currency || '').toUpperCase();
  if (code === 'GBP') return `£${n.toFixed(2)}`;
  if (code === 'GBP_PENCE' || code === 'GBX' || code === 'GBPENCE') {
    return `${n >= 10 ? n.toFixed(0) : n.toFixed(2)}p`;
  }
  const symbol = code === 'AUD' ? 'A$' : code === 'USD' ? 'US$' : '$';
  return `${symbol}${n.toFixed(2)}`;
}

function normalizeCurrencyCode(value) {
  const raw = String(value || '').trim().toUpperCase();
  if (!raw) return '';
  if (raw === 'GBX' || raw === 'GBP_PENCE' || raw === 'GBPENCE' || raw === 'PENCE') return 'GBP_PENCE';
  if (raw === 'GBP' || raw === 'AUD' || raw === 'USD' || raw === 'CAD' || raw === 'EUR') return raw;
  return raw;
}

function titleizeKey(key) {
  return String(key || '')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function fmtRankMetric(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  return n.toFixed(digits);
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

function fmtRunTimestamp(value) {
  const dt = parseIsoDateOrNull(value);
  if (!dt) return '';
  try {
    return new Intl.DateTimeFormat(undefined, {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(dt);
  } catch {
    return dt.toISOString().replace('T', ' ').slice(0, 16);
  }
}

function shortRunId(runId) {
  const raw = String(runId || '').trim();
  if (!raw) return '';
  const base = raw.replace(/\.json$/i, '');
  const tail = base.match(/(\d{6})$/);
  if (tail) return tail[1];
  return base.slice(-8);
}

function buildRunOptionLabel(run) {
  const label = String(run?.label || run?.file || run?.id || 'Run').trim();
  const ts = fmtRunTimestamp(run?.analysis_date || run?.updated_at || '');
  const suffix = shortRunId(run?.id || run?.file || '');
  const parts = [label];
  if (ts) parts.push(ts);
  if (suffix) parts.push(`#${suffix}`);
  return parts.join(' · ');
}

function computeFreshnessFallback(stage3, updatedAt) {
  const analysisRaw = String(stage3?.analysis_date || '').trim();
  const marketRaw = String(stage3?.market_data_provenance?.prepass_as_of_utc || '').trim();
  const updatedRaw = String(updatedAt || '').trim();

  const analysisDt = parseIsoDateOrNull(analysisRaw);
  const marketDt = parseIsoDateOrNull(marketRaw);
  const updatedDt = parseIsoDateOrNull(updatedRaw);

  const baselineDt = analysisDt || marketDt || updatedDt;
  if (!baselineDt) {
    return {
      analysis_as_of_utc: analysisRaw,
      market_as_of_utc: marketRaw,
      baseline_as_of_utc: '',
      baseline_source: '',
      age_days: null,
      status: 'unknown',
      recommended_action: 'review_soon',
      reason: 'No reliable as-of timestamp found in this artifact.',
    };
  }

  const now = new Date();
  const ageDays = Math.max(0, Math.floor((now.getTime() - baselineDt.getTime()) / 86400000));
  let status = 'watch';
  let recommendedAction = 'review_soon';
  if (ageDays <= 7) {
    status = 'fresh';
    recommendedAction = 'reuse';
  } else if (ageDays > 21) {
    status = 'stale';
    recommendedAction = 'full_rerun_recommended';
  }

  let baselineSource = 'analysis_date';
  if (!analysisDt && marketDt) baselineSource = 'market_data_provenance.prepass_as_of_utc';
  if (!analysisDt && !marketDt) baselineSource = 'artifact_updated_at';

  return {
    analysis_as_of_utc: analysisRaw,
    market_as_of_utc: marketRaw,
    baseline_as_of_utc: baselineDt.toISOString(),
    baseline_source: baselineSource,
    age_days: ageDays,
    status,
    recommended_action: recommendedAction,
    reason: `baseline from ${baselineSource}; age=${ageDays} day(s)`,
  };
}

const BREAKDOWN_LABELS = {
  jurisdiction: 'Jurisdiction',
  infrastructure: 'Infrastructure',
  management: 'Management',
  development_stage: 'Development Stage',
  funding: 'Funding',
  certainty: 'Certainty',
  certainty_12m: '12M Execution Certainty',
  esg: 'ESG',
  clinical_ethical: 'Clinical & Ethical Standards',
  clinical_ethical_standards: 'Clinical & Ethical Standards',
  regulatory_environment: 'Regulatory Environment',
  scientific_manufacturing: 'Scientific & Manufacturing Capability',
  pipeline_maturity: 'Pipeline Maturity',
  cash_runway_funding: 'Cash Runway / Funding',

  npv_vs_market_cap: 'NPV vs Market Cap',
  rnpv_vs_market_cap: 'rNPV vs Market Cap',
  ev_resource: 'EV / Resource',
  ev_per_resource_oz: 'EV / Resource oz',
  ev_per_risk_adj_peak_sales: 'EV / Risk-Adjusted Peak Sales',
  ev_vs_peak_sales: 'EV / Peak Sales',
  exploration_upside: 'Exploration Upside',
  pipeline_platform_potential: 'Pipeline Optionality',
  pipeline_optionality: 'Pipeline Optionality',
  market_positioning_moat: 'Competitive Position',
  cost_competitiveness: 'Cost Competitiveness',
  ma_strategic: 'M&A Strategic Value',
  ma_strategic_value: 'M&A Strategic Value',
};

function labelForBreakdownKey(key) {
  return BREAKDOWN_LABELS[String(key || '').trim()] || titleizeKey(key);
}

function breakdownValue(v) {
  if (v == null) return null;
  if (typeof v === 'number') return v;
  if (typeof v === 'object') {
    if (Number.isFinite(Number(v.score))) return Number(v.score);
    if (Number.isFinite(Number(v.value))) return Number(v.value);
    if (Number.isFinite(Number(v.total))) return Number(v.total);
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

const FIELD_PATH_LABELS = {
  'market_data.current_price': 'Current Share Price',
  'market_data.market_cap_m': 'Market Capitalization',
  'market_data.shares_outstanding_m': 'Shares Outstanding',
  'market_data.shares_outstanding_post_split': 'Shares Outstanding (Post Split)',
  'financials.pro_forma_cash': 'Pro Forma Cash',
  'financials.pro_forma_cash_post_avenue_loan': 'Pro Forma Cash (Post Avenue Loan)',
  'pipeline[0].cmc_readiness_status': 'Lead Asset CMC Readiness',
  'pipeline[0].acquisition_royalty_obligations': 'Lead Asset Royalty Obligations',
  'pipeline[0].regulatory_status.bla_submission_date': 'Lead Asset BLA Submission Date',
  'regulatory.priority_review_voucher_eligibility': 'Priority Review Voucher Eligibility',
  'price_targets.scenario_targets.24m.base': '24M Base Price Target',
};

function labelForFieldPath(path) {
  const p = String(path || '').trim();
  if (!p) return 'Unknown field';
  if (FIELD_PATH_LABELS[p]) return FIELD_PATH_LABELS[p];
  return p
    .replace(/\[(\d+)\]/g, ' #$1')
    .split('.')
    .map((part) => titleizeKey(part))
    .join(' > ');
}

function renderInlineMarkdown(text, keyPrefix = 'inline') {
  const src = String(text || '');
  if (!src) return null;
  const parts = src.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).filter(Boolean);
  return parts.map((part, idx) => {
    if (part.startsWith('**') && part.endsWith('**')) {
      return <strong key={`${keyPrefix}-b-${idx}`}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith('`') && part.endsWith('`')) {
      return <code key={`${keyPrefix}-c-${idx}`}>{part.slice(1, -1)}</code>;
    }
    return <span key={`${keyPrefix}-t-${idx}`}>{part}</span>;
  });
}

function renderMarkdownBlocks(markdown) {
  const lines = String(markdown || '').replace(/\r\n/g, '\n').split('\n');
  const blocks = [];
  let paragraph = [];
  let listItems = [];
  let listType = null;

  const flushParagraph = () => {
    if (!paragraph.length) return;
    const text = paragraph.join(' ').trim();
    if (text) blocks.push({ type: 'p', text });
    paragraph = [];
  };

  const flushList = () => {
    if (!listItems.length) return;
    blocks.push({ type: listType || 'ul', items: [...listItems] });
    listItems = [];
    listType = null;
  };

  for (const rawLine of lines) {
    const line = String(rawLine || '');
    const trimmed = line.trim();

    const heading = trimmed.match(/^(#{1,4})\s+(.+)$/);
    if (heading) {
      flushParagraph();
      flushList();
      blocks.push({ type: `h${heading[1].length}`, text: heading[2].trim() });
      continue;
    }

    const bullet = trimmed.match(/^[-*]\s+(.+)$/);
    if (bullet) {
      flushParagraph();
      if (listType && listType !== 'ul') flushList();
      listType = 'ul';
      listItems.push(bullet[1].trim());
      continue;
    }

    const numbered = trimmed.match(/^\d+\.\s+(.+)$/);
    if (numbered) {
      flushParagraph();
      if (listType && listType !== 'ol') flushList();
      listType = 'ol';
      listItems.push(numbered[1].trim());
      continue;
    }

    if (!trimmed) {
      flushParagraph();
      flushList();
      continue;
    }

    if (listType) flushList();
    paragraph.push(trimmed);
  }

  flushParagraph();
  flushList();

  if (!blocks.length) {
    return <p className="memo-empty">No analyst memo available for this run.</p>;
  }

  return blocks.map((block, idx) => {
    if (block.type === 'h1') return <h1 key={`md-${idx}`}>{renderInlineMarkdown(block.text, `h1-${idx}`)}</h1>;
    if (block.type === 'h2') return <h2 key={`md-${idx}`}>{renderInlineMarkdown(block.text, `h2-${idx}`)}</h2>;
    if (block.type === 'h3') return <h3 key={`md-${idx}`}>{renderInlineMarkdown(block.text, `h3-${idx}`)}</h3>;
    if (block.type === 'h4') return <h4 key={`md-${idx}`}>{renderInlineMarkdown(block.text, `h4-${idx}`)}</h4>;
    if (block.type === 'ul') {
      return (
        <ul key={`md-${idx}`}>
          {block.items.map((item, i) => (
            <li key={`md-${idx}-li-${i}`}>{renderInlineMarkdown(item, `uli-${idx}-${i}`)}</li>
          ))}
        </ul>
      );
    }
    if (block.type === 'ol') {
      return (
        <ol key={`md-${idx}`}>
          {block.items.map((item, i) => (
            <li key={`md-${idx}-li-${i}`}>{renderInlineMarkdown(item, `oli-${idx}-${i}`)}</li>
          ))}
        </ol>
      );
    }
    return <p key={`md-${idx}`}>{renderInlineMarkdown(block.text, `p-${idx}`)}</p>;
  });
}

function periodToQuarterIndex(period) {
  const text = String(period || '').trim().toUpperCase();
  if (!text) return null;

  let match = text.match(/\bQ([1-4])\s*[-/]\s*Q([1-4])\s*(20\d{2})\b/);
  if (match) {
    const q1 = Number(match[1]);
    const q2 = Number(match[2]);
    const year = Number(match[3]);
    return (year * 4) + Math.max(q1, q2);
  }

  match = text.match(/\bQ([1-4])\s*(20\d{2})\b/);
  if (match) {
    const quarter = Number(match[1]);
    const year = Number(match[2]);
    return (year * 4) + quarter;
  }

  match = text.match(/\bH([12])\s*(20\d{2})\b/);
  if (match) {
    const half = Number(match[1]);
    const year = Number(match[2]);
    const quarter = half === 1 ? 2 : 4;
    return (year * 4) + quarter;
  }

  match = text.match(/\b(20\d{2})\b/);
  if (match) {
    const year = Number(match[1]);
    return (year * 4) + 4;
  }

  return null;
}

function currentQuarterIndex() {
  const now = new Date();
  const quarter = Math.floor(now.getMonth() / 3) + 1;
  return (now.getFullYear() * 4) + quarter;
}

function statusIndicatesPast(status) {
  const low = String(status || '').trim().toLowerCase();
  if (!low) return false;
  return [
    'achieved',
    'completed',
    'done',
    'delivered',
    'closed',
    'finished',
    'met',
    'launched',
    'commissioned',
    'first gold',
  ].some((token) => low.includes(token));
}

function statusIndicatesFuture(status) {
  const low = String(status || '').trim().toLowerCase();
  if (!low) return false;
  return [
    'planned',
    'at_risk',
    'at risk',
    'pending',
    'upcoming',
    'target',
    'on track',
    'on_track',
    'current',
    'in progress',
    'in_progress',
    'speculative',
    'proposed',
  ].some((token) => low.includes(token));
}

function isPastTimelineLike({ status, targetPeriod }) {
  if (statusIndicatesPast(status)) return true;
  if (statusIndicatesFuture(status)) return false;
  const q = periodToQuarterIndex(targetPeriod);
  if (q == null) return false;
  return q < currentQuarterIndex();
}

function capPastTimelineLikeItems(items, maxPast = 1) {
  if (!Array.isArray(items)) return [];
  if (!items.length) return [];
  if (maxPast < 0) return [];

  const indexed = items.map((item, idx) => {
    const targetPeriod = item?.target_period || item?.targetPeriod || item?.period || item?.date || '';
    const status = item?.status || item?.state || item?.current_status || '';
    return {
      idx,
      item,
      isPast: isPastTimelineLike({ status, targetPeriod }),
      qIndex: periodToQuarterIndex(targetPeriod),
    };
  });

  const pastRows = indexed.filter((row) => row.isPast);
  if (pastRows.length <= maxPast) return indexed.map((row) => row.item);

  const keepPastIdx = new Set(
    [...pastRows]
      .sort((a, b) => {
        const aq = a.qIndex == null ? -1 : a.qIndex;
        const bq = b.qIndex == null ? -1 : b.qIndex;
        if (bq !== aq) return bq - aq;
        return b.idx - a.idx;
      })
      .slice(0, maxPast)
      .map((row) => row.idx)
  );

  return indexed
    .filter((row) => !row.isPast || keepPastIdx.has(row.idx))
    .map((row) => row.item);
}

function extractPeriodFromText(text) {
  const raw = String(text || '');
  const match = raw.match(/\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2})\b/i);
  return match ? String(match[1] || '').trim() : '';
}

function normalizeTimelineRows(rawTimeline) {
  const rows = Array.isArray(rawTimeline) ? rawTimeline : [];
  const periodPattern = /\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2})\b/i;

  const normalized = rows
    .map((entry, idx) => {
      if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
        const milestone = String(
          entry.milestone || entry.event || entry.name || entry.goal || entry.title || ''
        ).trim();
        const targetPeriod = String(
          entry.target_period || entry.targetPeriod || entry.period || entry.when || entry.date || ''
        ).trim();
        const status = String(entry.status || entry.current_status || entry.state || 'unspecified').trim();
        const confidence = toNumberOrNull(entry.confidence_pct ?? entry.certainty_pct);
        if (!milestone && !targetPeriod) return null;
        return {
          milestone: milestone || `Milestone ${idx + 1}`,
          target_period: targetPeriod,
          status: status || 'unspecified',
          confidence_pct: confidence,
          primary_risk: String(entry.primary_risk || entry.risk || '').trim(),
        };
      }

      if (typeof entry === 'string') {
        const text = entry.trim();
        if (!text) return null;
        let milestone = text;
        let targetPeriod = '';

        // Common chairman form: "Q1-Q2 2026: Milestone name"
        const colonSplit = text.match(/^([^:]{2,40}):\s*(.+)$/);
        if (colonSplit) {
          const lhs = colonSplit[1].trim();
          const rhs = colonSplit[2].trim();
          if (periodPattern.test(lhs)) {
            targetPeriod = lhs;
            milestone = rhs || text;
          }
        }

        // Fallback: period appears somewhere in the line.
        if (!targetPeriod) {
          const m = text.match(periodPattern);
          if (m) {
            targetPeriod = m[1].trim();
            const stripped = text.replace(m[0], '').replace(/^[:\-\s]+/, '').trim();
            milestone = stripped || text;
          }
        }

        return {
          milestone: milestone || `Milestone ${idx + 1}`,
          target_period: targetPeriod,
          status: 'unspecified',
          confidence_pct: null,
          primary_risk: '',
        };
      }

      return null;
    })
    .filter(Boolean);

  return capPastTimelineLikeItems(normalized, 1);
}

function normalizeCatalysts(rawCatalysts) {
  const rows = Array.isArray(rawCatalysts) ? rawCatalysts : [];
  const normalized = rows
    .map((item) => {
      if (item && typeof item === 'object' && !Array.isArray(item)) {
        const text = String(
          item.name || item.title || item.milestone || item.catalyst || item.event || ''
        ).trim();
        const targetPeriod = String(
          item.target_period || item.targetPeriod || item.period || item.when || item.date || ''
        ).trim() || extractPeriodFromText(text);
        const status = String(item.status || item.current_status || item.state || '').trim();
        if (!text) return null;
        return { text, target_period: targetPeriod, status };
      }

      if (typeof item === 'string') {
        const text = item.trim();
        if (!text) return null;
        return {
          text,
          target_period: extractPeriodFromText(text),
          status: text,
        };
      }
      return null;
    })
    .filter(Boolean);

  const filtered = capPastTimelineLikeItems(normalized, 1);
  return filtered.map((row) => row.text).slice(0, 8);
}

function toMonthOffsetFromNow(targetPeriod) {
  if (!targetPeriod || typeof targetPeriod !== 'string') return null;
  const now = new Date();
  const text = targetPeriod.trim().toUpperCase();

  let month = null;
  let year = null;

  const qRange = text.match(/\bQ([1-4])\s*[-/]\s*Q([1-4])\s*(20\d{2})\b/);
  if (qRange) {
    const q1 = Number(qRange[1]);
    const q2 = Number(qRange[2]);
    year = Number(qRange[3]);
    const m1 = q1 * 3 - 1;
    const m2 = q2 * 3 - 1;
    month = Math.round((m1 + m2) / 2);
  } else {
    const q = text.match(/\bQ([1-4])\s*(20\d{2})\b/);
    if (q) {
      month = Number(q[1]) * 3 - 1;
      year = Number(q[2]);
    } else {
      const h = text.match(/\bH([12])\s*(20\d{2})\b/);
      if (h) {
        month = h[1] === '1' ? 5 : 11;
        year = Number(h[2]);
      } else {
        const y = text.match(/\b(20\d{2})\b/);
        if (y) {
          month = 6;
          year = Number(y[1]);
        }
      }
    }
  }

  if (!Number.isFinite(month) || !Number.isFinite(year)) return null;
  const target = new Date(year, month, 1);
  const diffMonths =
    (target.getFullYear() - now.getFullYear()) * 12 +
    (target.getMonth() - now.getMonth());
  return Math.max(0, Math.min(24, diffMonths));
}

function scoreTone(score) {
  if (score >= 80) return 'bull';
  if (score >= 60) return 'base';
  return 'bear';
}

function statusTone(status) {
  const s = String(status || '').toLowerCase();
  if (s.includes('met') || s.includes('likely') || s.includes('planned') || s.includes('imminent')) return 'bull';
  if (s.includes('risk')) return 'bear';
  return 'base';
}

function scenarioTone(name) {
  const k = String(name || '').toLowerCase();
  if (k === 'bull') return 'bull';
  if (k === 'bear') return 'bear';
  return 'base';
}

function normalizeThesisCondition(item, scenario, kind, index) {
  const kindLabel = kind === 'failure' ? 'Failure' : 'Required';
  if (typeof item === 'string') {
    const text = item.trim();
    if (!text) return null;
    return {
      key: `${scenario}-${kind}-${index}-${text.slice(0, 48)}`,
      chip: kindLabel,
      chipTone: kind === 'failure' ? 'bear' : scenarioTone(scenario),
      id: '',
      text,
      meta: [],
    };
  }
  if (!item || typeof item !== 'object') return null;

  const text = String(
    item.condition
      || item.title
      || item.text
      || item.label
      || item.condition_id
      || '',
  ).trim();
  if (!text) return null;

  const currentStatus = String(item.current_status || item.status || '').trim();
  const id = String(item.condition_id || item.watch_id || '').trim();
  const meta = [
    item.by ? `by ${item.by}` : '',
    item.trigger_window || item.target_window || item.window || '',
    item.duration || '',
    item.source_to_monitor || '',
  ]
    .map((part) => String(part || '').trim())
    .filter(Boolean);

  return {
    key: id || `${scenario}-${kind}-${index}-${text.slice(0, 48)}`,
    chip: currentStatus || kindLabel,
    chipTone: currentStatus ? statusTone(currentStatus) : (kind === 'failure' ? 'bear' : scenarioTone(scenario)),
    id,
    text,
    meta,
  };
}

function normalizeProb(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  if (n <= 1) return n * 100;
  return n;
}

function weightedTargetForHorizon(targets = {}, probs = {}) {
  let weightedSum = 0;
  let probSum = 0;
  for (const key of ['bear', 'base', 'bull']) {
    const target = toNumberOrNull(targets?.[key]);
    const prob = normalizeProb(probs?.[key]);
    if (target == null || prob == null || prob <= 0) continue;
    weightedSum += target * prob;
    probSum += prob;
  }
  if (probSum > 0) return weightedSum / probSum;
  return toNumberOrNull(targets?.base);
}

function targetScaleFactorForCurrent(currentPrice, targetValues = []) {
  const current = toNumberOrNull(currentPrice);
  if (current == null || current <= 0) return 1;
  const med = median(
    (targetValues || [])
      .map((v) => toNumberOrNull(v))
      .filter((v) => v != null && v > 0)
  );
  if (med == null || med <= 0) return 1;

  // Handles GBp-vs-GBP and cents-vs-dollars extraction mismatches without
  // changing genuinely comparable targets.
  if (current >= 20 && med <= current / 20) return 100;
  if (current <= 20 && med >= current * 20) return 0.01;
  return 1;
}

function scalePrice(value, factor = 1) {
  const n = toNumberOrNull(value);
  if (n == null) return null;
  return n * factor;
}

function scaleTargets(targets = {}, factor = 1) {
  return {
    bear: scalePrice(targets?.bear, factor),
    base: scalePrice(targets?.base, factor),
    bull: scalePrice(targets?.bull, factor),
  };
}

function inferPriceDisplayCurrency(currency, currentPrice, targetScaleFactor = 1) {
  const code = normalizeCurrencyCode(currency);
  if (code === 'GBP' && (targetScaleFactor === 100 || Number(currentPrice) >= 20)) {
    return 'GBP_PENCE';
  }
  return code || 'AUD';
}

const PRICE_REBASE_STORAGE_KEY = 'llm-council:gantt-price-rebase:v1';

function loadStoredPriceRebases() {
  if (typeof window === 'undefined') return {};
  try {
    const raw = window.localStorage.getItem(PRICE_REBASE_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function normalizePriceRebase(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const type = String(raw.type || '').trim();
  if (!['share_consolidation', 'share_split'].includes(type)) return null;
  const oldUnits = Number(raw.old_units);
  const newUnits = Number(raw.new_units);
  if (!Number.isFinite(oldUnits) || oldUnits <= 0 || !Number.isFinite(newUnits) || newUnits <= 0) return null;
  const priceFactor = oldUnits / newUnits;
  if (!Number.isFinite(priceFactor) || priceFactor <= 0) return null;
  return {
    type,
    old_units: oldUnits,
    new_units: newUnits,
    price_factor: priceFactor,
    created_at: raw.created_at || new Date().toISOString(),
  };
}

function rebasePrice(value, factor) {
  const n = toNumberOrNull(value);
  if (n == null) return null;
  return n * factor;
}

function rebaseTargets(targets = {}, factor = 1) {
  return {
    bear: rebasePrice(targets?.bear, factor),
    base: rebasePrice(targets?.base, factor),
    bull: rebasePrice(targets?.bull, factor),
  };
}

function formatRebaseRatio(rebase) {
  if (!rebase) return '';
  return `${fmtNum(rebase.old_units, 0)}:${fmtNum(rebase.new_units, 0)}`;
}

function toNumberOrNull(value) {
  if (value == null) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    const cleaned = value.replace(/[^0-9.-]/g, '').trim();
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function median(values) {
  const list = (values || [])
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  if (!list.length) return null;
  const mid = Math.floor(list.length / 2);
  if (list.length % 2 === 1) return list[mid];
  return (list[mid - 1] + list[mid]) / 2;
}

function extractCurrentPriceCandidatesFromText(text) {
  const src = String(text || '');
  if (!src) return [];
  const trigger = /\b(current\s+(?:share\s+)?price|share\s+price)\b/i;
  const valuePattern = /(?:A\$|AU\$|AUD\s*)\s*([0-9]+(?:\.[0-9]+)?)/gi;
  const rangePattern = /(?:A\$|AU\$|AUD\s*)\s*([0-9]+(?:\.[0-9]+)?)\s*[-–]\s*([0-9]+(?:\.[0-9]+)?)/gi;
  const out = [];

  const pushIfValid = (raw) => {
    const n = toNumberOrNull(raw);
    if (n == null) return;
    if (n <= 0 || n >= 1000) return;
    out.push(n);
  };

  for (const rawLine of src.split(/\r?\n/)) {
    const line = String(rawLine || '').trim();
    if (!line || !trigger.test(line)) continue;

    let match;
    rangePattern.lastIndex = 0;
    while ((match = rangePattern.exec(line)) !== null) {
      pushIfValid(match[1]);
      pushIfValid(match[2]);
    }

    valuePattern.lastIndex = 0;
    while ((match = valuePattern.exec(line)) !== null) {
      pushIfValid(match[1]);
    }
  }

  return out;
}

function inferCurrentPrice(stage3, payload) {
  const candidates = [
    stage3?.price_targets?.current_price,
    stage3?.market_data?.current_price,
    stage3?.market_data_provenance?.prepass_current_price,
    payload?.market_facts?.normalized_facts?.current_price,
    payload?.input_audit?.market_facts?.normalized_facts?.current_price,
    payload?.input_audit?.market_details?.normalized_facts?.current_price,
    payload?.stage3_result?.structured_data?.price_targets?.current_price,
    payload?.stage3_result?.structured_data?.market_data?.current_price,
    payload?.stage3_result_primary?.structured_data?.price_targets?.current_price,
    payload?.stage3_result_primary?.structured_data?.market_data?.current_price,
  ]
    .map((v) => toNumberOrNull(v))
    .filter((v) => v != null && v > 0 && v < 1000);

  if (candidates.length) return candidates[0];

  const textSources = [
    payload?.chairman_memo_markdown,
    payload?.analyst_memo_markdown,
    payload?.chairman_document?.content_markdown,
    payload?.chairman_document?.content,
    payload?.analyst_document?.content_markdown,
    payload?.analyst_document?.content,
    payload?.stage3_result?.response,
    payload?.stage3_result_primary?.response,
  ];
  const textCandidates = [];
  textSources.forEach((txt) => {
    textCandidates.push(...extractCurrentPriceCandidatesFromText(txt));
  });
  return median(textCandidates);
}

function canonicalRunId(rawId) {
  const id = String(rawId || '').trim();
  if (!id) return '';
  if (id.endsWith('.checkpoint.json')) {
    return id.replace(/\.stage[^.]*\.checkpoint\.json$/i, '.json');
  }
  if (id.includes('.normalized_preview_') && id.endsWith('.json')) {
    return `${id.split('.normalized_preview_')[0]}.json`;
  }
  if (id.endsWith('.json') && id.includes('.stage')) {
    return `${id.split('.stage')[0]}.json`;
  }
  return id;
}

function dedupeRuns(runs) {
  const list = Array.isArray(runs) ? runs : [];
  const byCanonical = new Map();
  list.forEach((run) => {
    if (!run || typeof run !== 'object') return;
    const id = String(run.id || run.file || '').trim();
    if (!id) return;
    const canonical = canonicalRunId(id);
    const isCanonical = id === canonical;
    const ts = Date.parse(String(run.updated_at || run.analysis_date || '')) || 0;
    const score = isCanonical ? 2 : 1;
    const prev = byCanonical.get(canonical);
    if (!prev || score > prev.score || (score === prev.score && ts > prev.ts)) {
      // Keep the real artifact id for API fetches; use canonical only for dedupe grouping.
      byCanonical.set(canonical, { run: { ...run }, score, ts });
    }
  });

  return Array.from(byCanonical.values())
    .map((x) => x.run)
    .sort((a, b) => {
      const ta = Date.parse(String(a.updated_at || a.analysis_date || '')) || 0;
      const tb = Date.parse(String(b.updated_at || b.analysis_date || '')) || 0;
      return tb - ta;
    });
}

function normalizeVerificationQueue(stage3) {
  const rawQueue = Array.isArray(stage3?.verification_queue) ? stage3.verification_queue : [];
  const rawFields = Array.isArray(stage3?.verification_required_fields) ? stage3.verification_required_fields : [];
  const source = rawQueue.length ? rawQueue : rawFields;
  const out = [];

  source.forEach((item, idx) => {
    if (item == null) return;
    if (typeof item === 'string') {
      const field = item.trim();
      if (!field) return;
      out.push({
        field,
        priority: 'medium',
        reason: 'High-impact uncertain field from chairman synthesis.',
        required_source: 'Primary filing / latest company update',
        _k: `${field}-${idx}`,
      });
      return;
    }
    if (typeof item === 'object') {
      const field = String(item.field || item.field_path || '').trim();
      if (!field) return;
      const p = String(item.priority || 'medium').toLowerCase();
      out.push({
        field,
        priority: ['high', 'medium', 'low'].includes(p) ? p : 'medium',
        reason: String(item.reason || 'High-impact uncertain field from chairman synthesis.'),
        required_source: String(item.required_source || 'Primary filing / latest company update'),
        _k: `${field}-${idx}`,
      });
    }
  });

  return out;
}

function normalizeWatchlist(stage3, thesisMap) {
  const raw = stage3?.monitoring_watchlist;
  const isObject = raw && typeof raw === 'object' && !Array.isArray(raw);
  const redFlags = isObject && Array.isArray(raw.red_flags) ? raw.red_flags : [];
  const confirmatory = isObject && Array.isArray(raw.confirmatory_signals) ? raw.confirmatory_signals : [];

  const normalizeWatchItems = (items, kind) => items.map((item, idx) => {
    if (typeof item === 'string') {
      const txt = item.trim();
      if (!txt) return null;
      return {
        watch_id: `${kind}_${idx}`,
        condition: txt,
        source_to_monitor: 'Company filings and milestone updates',
        trigger_window: '',
        duration: '',
        severity: kind === 'red' ? 'high' : 'medium',
      };
    }
    if (item && typeof item === 'object') {
      const txt = String(item.condition || item.title || item.watch_id || '').trim();
      if (!txt) return null;
      return {
        watch_id: String(item.watch_id || `${kind}_${idx}`),
        condition: txt,
        source_to_monitor: String(item.source_to_monitor || 'Company filings and milestone updates'),
        trigger_window: String(item.trigger_window || ''),
        duration: String(item.duration || ''),
        severity: String(item.severity || (kind === 'red' ? 'high' : 'medium')),
      };
    }
    return null;
  }).filter(Boolean);

  if (redFlags.length || confirmatory.length) {
    return {
      red_flags: normalizeWatchItems(redFlags, 'red'),
      confirmatory_signals: normalizeWatchItems(confirmatory, 'confirm'),
    };
  }

  const out = { red_flags: [], confirmatory_signals: [] };
  const asText = (cond) => {
    if (typeof cond === 'string') return cond.trim();
    if (cond && typeof cond === 'object') return String(cond.condition || cond.condition_id || '').trim();
    return '';
  };

  const bear = thesisMap?.bear && typeof thesisMap.bear === 'object' ? thesisMap.bear : {};
  const base = thesisMap?.base && typeof thesisMap.base === 'object' ? thesisMap.base : {};
  const bull = thesisMap?.bull && typeof thesisMap.bull === 'object' ? thesisMap.bull : {};

  [...(bear.required_conditions || []), ...(bear.failure_conditions || [])].slice(0, 6).forEach((c, i) => {
    const txt = asText(c);
    if (!txt) return;
    out.red_flags.push({
      watch_id: String(c?.condition_id || `bear_${i}`),
      condition: txt,
      source_to_monitor: String(c?.source_to_monitor || 'Company filings and milestone updates'),
      trigger_window: String(c?.trigger_window || ''),
      duration: String(c?.duration || ''),
      severity: String(c?.severity || 'high'),
    });
  });

  [...(base.required_conditions || []), ...(bull.required_conditions || [])].slice(0, 6).forEach((c, i) => {
    const txt = asText(c);
    if (!txt) return;
    out.confirmatory_signals.push({
      watch_id: String(c?.condition_id || `confirm_${i}`),
      condition: txt,
      source_to_monitor: String(c?.source_to_monitor || 'Company filings and milestone updates'),
    });
  });

  return out;
}

export default function GanttIntelligenceLab({ monitorOnly = false }) {
  const [locationSearch, setLocationSearch] = useState(() => (
    typeof window === 'undefined' ? '' : window.location.search || ''
  ));
  const [datasetId, setDatasetId] = useState('');
  const [pendingDatasetId, setPendingDatasetId] = useState('');
  const [timelineOrientation, setTimelineOrientation] = useState('vertical');
  const [remoteRuns, setRemoteRuns] = useState([]);
  const [remoteDataById, setRemoteDataById] = useState({});
  const [deltaByRunId, setDeltaByRunId] = useState({});
  const [deltaLoadingByRunId, setDeltaLoadingByRunId] = useState({});
  const [remoteLoading, setRemoteLoading] = useState(false);
  const [remoteError, setRemoteError] = useState('');
  const [deletingRunId, setDeletingRunId] = useState('');
  const [runsReloadToken, setRunsReloadToken] = useState(0);
  const [priceRebaseByRunId, setPriceRebaseByRunId] = useState(loadStoredPriceRebases);
  const [priceAdjustOpen, setPriceAdjustOpen] = useState(false);
  const [priceAdjustError, setPriceAdjustError] = useState('');
  const [priceAdjustForm, setPriceAdjustForm] = useState({
    type: 'share_consolidation',
    oldUnits: '10',
    newUnits: '1',
  });
  const [showPriceHistoryOverlay, setShowPriceHistoryOverlay] = useState(true);
  const [showRouterEventOverlay, setShowRouterEventOverlay] = useState(true);
  const [securityHistoryPayload, setSecurityHistoryPayload] = useState(null);
  const [routerOverlayEvents, setRouterOverlayEvents] = useState([]);
  const [marketPathError, setMarketPathError] = useState('');
  const preferredRunIdFromUrl = useMemo(() => {
    try {
      const params = new URLSearchParams(locationSearch || '');
      return String(
        params.get('run_id')
          || params.get('runId')
          || params.get('run')
          || ''
      ).trim();
    } catch {
      return '';
    }
  }, [locationSearch]);
  const preferredTickerFromUrl = useMemo(() => {
    try {
      const params = new URLSearchParams(locationSearch || '');
      return String(params.get('ticker') || '').trim().toUpperCase();
    } catch {
      return '';
    }
  }, [locationSearch]);
  const preferredTickerAliases = useMemo(() => {
    const raw = String(preferredTickerFromUrl || '').trim().toUpperCase();
    if (!raw) return [];
    const out = [raw];
    if (raw.includes(':')) {
      const suffix = raw.split(':').pop()?.trim().toUpperCase() || '';
      if (suffix && !out.includes(suffix)) out.push(suffix);
    }
    return out;
  }, [preferredTickerFromUrl]);
  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    const sync = () => setLocationSearch(window.location.search || '');
    window.addEventListener('popstate', sync);
    window.addEventListener('app:navigate', sync);
    return () => {
      window.removeEventListener('popstate', sync);
      window.removeEventListener('app:navigate', sync);
    };
  }, []);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;

    const root = document.getElementById('root');
    const prevRootOverflow = root?.style.overflow ?? '';
    const prevBodyOverflow = document.body.style.overflow;
    const prevHtmlOverflow = document.documentElement.style.overflow;

    if (root) root.style.overflow = 'auto';
    document.body.style.overflow = 'auto';
    document.documentElement.style.overflow = 'auto';

    return () => {
      if (root) root.style.overflow = prevRootOverflow;
      document.body.style.overflow = prevBodyOverflow;
      document.documentElement.style.overflow = prevHtmlOverflow;
    };
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(PRICE_REBASE_STORAGE_KEY, JSON.stringify(priceRebaseByRunId || {}));
    } catch {
      // Local display preference only; ignore storage failures.
    }
  }, [priceRebaseByRunId]);

  useEffect(() => {
    let cancelled = false;
    const loadRuns = async () => {
      try {
        setRemoteLoading(true);
        setRemoteError('');
        const payload = await api.listGanttRuns(50);
        let runs = dedupeRuns(payload?.runs);

        if (preferredTickerAliases.length) {
          runs = [...runs].sort((a, b) => {
            const aTicker = String(a?.ticker || '').trim().toUpperCase();
            const bTicker = String(b?.ticker || '').trim().toUpperCase();
            const aMatch = preferredTickerAliases.includes(aTicker) ? 1 : 0;
            const bMatch = preferredTickerAliases.includes(bTicker) ? 1 : 0;
            if (aMatch !== bMatch) return bMatch - aMatch;
            const ta = Date.parse(String(a?.updated_at || a?.analysis_date || '')) || 0;
            const tb = Date.parse(String(b?.updated_at || b?.analysis_date || '')) || 0;
            return tb - ta;
          });
        }

        if (!cancelled) {
          setRemoteRuns(runs);
        }
      } catch (err) {
        if (cancelled) return;
        setRemoteError(err?.message || 'Failed to load run list');
      } finally {
        if (!cancelled) {
          setRemoteLoading(false);
        }
      }
    };
    loadRuns();
    return () => {
      cancelled = true;
    };
  }, [preferredTickerAliases, runsReloadToken]);

  useEffect(() => {
    let cancelled = false;
    const directRunId = String(preferredRunIdFromUrl || '').trim();
    if (!directRunId || remoteDataById[directRunId]) return undefined;

    const loadPreferredRunDirectly = async () => {
      try {
        const payload = await api.getGanttRun(directRunId);
        const structured = payload?.structured_data;
        if (!structured || typeof structured !== 'object' || cancelled) return;
        const runMeta = {
          id: directRunId,
          file: payload?.file || directRunId,
          label: payload?.label || payload?.file || directRunId,
          ticker: structured?.ticker || '',
          company_name: structured?.company_name || structured?.company || '',
          analysis_date: structured?.analysis_date || '',
          updated_at: payload?.updated_at || '',
          freshness: payload?.freshness || null,
        };
        setRemoteRuns((prev) => dedupeRuns([runMeta, ...(Array.isArray(prev) ? prev : [])]));
        setRemoteDataById((prev) => ({ ...prev, [directRunId]: payload }));
        setRemoteError('');
      } catch (err) {
        if (!cancelled) {
          setRemoteError(err?.message || 'Failed to load selected run');
        }
      }
    };
    loadPreferredRunDirectly();
    return () => {
      cancelled = true;
    };
  }, [preferredRunIdFromUrl, remoteDataById]);

  const datasets = useMemo(() => {
    const runs = remoteRuns.map((run) => ({
      id: `run:${run.id}`,
      label: run.label || run.file || run.id,
      optionLabel: buildRunOptionLabel(run),
      remoteRunId: run.id,
      data: remoteDataById[run.id] || null,
    }));
    return runs;
  }, [remoteRuns, remoteDataById]);

  const setRunIdInUrl = useCallback((runId) => {
    if (typeof window === 'undefined') return;
    const normalizedRunId = String(runId || '').trim();
    const url = new URL(window.location.href);
    const current = String(url.searchParams.get('run_id') || '').trim();
    if (normalizedRunId) {
      if (current === normalizedRunId) return;
      url.searchParams.set('run_id', normalizedRunId);
    } else {
      if (!current) return;
      url.searchParams.delete('run_id');
    }
    window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`);
    setLocationSearch(url.search || '');
  }, []);

  const requestDatasetSelection = useCallback((nextDatasetId) => {
    const normalized = String(nextDatasetId || '').trim();
    if (!normalized) return;
    const target = datasets.find((d) => d.id === normalized);
    if (!target) return;
    setRunIdInUrl(target.remoteRunId || '');
    if (target.data) {
      setDatasetId(normalized);
      setPendingDatasetId('');
      return;
    }
    setPendingDatasetId(normalized);
  }, [datasets, setRunIdInUrl]);

  useEffect(() => {
    if (!datasets.length) return;
    const preferredDatasetId = preferredRunIdFromUrl ? `run:${preferredRunIdFromUrl}` : '';
    if (preferredDatasetId && datasets.some((d) => d.id === preferredDatasetId)) {
      if (datasetId !== preferredDatasetId && pendingDatasetId !== preferredDatasetId) {
        requestDatasetSelection(preferredDatasetId);
      }
      return;
    }
    if (!datasets.some((d) => d.id === datasetId) && !pendingDatasetId) {
      requestDatasetSelection(datasets[0].id);
    }
  }, [datasets, datasetId, pendingDatasetId, preferredRunIdFromUrl, requestDatasetSelection]);

  useEffect(() => {
    const targetDatasetId = String(pendingDatasetId || datasetId || '').trim();
    const selected = datasets.find((d) => d.id === targetDatasetId);
    if (!selected || !selected.remoteRunId) {
      return;
    }
    if (selected.data) {
      if (targetDatasetId !== datasetId) {
        setDatasetId(targetDatasetId);
      }
      if (pendingDatasetId === targetDatasetId) {
        setPendingDatasetId('');
      }
      return;
    }
    let cancelled = false;
    const loadRun = async () => {
      try {
        setRemoteLoading(true);
        setRemoteError('');
        const payload = await api.getGanttRun(selected.remoteRunId);
        const structured = payload?.structured_data;
        if (!cancelled && structured && typeof structured === 'object') {
          setRemoteDataById((prev) => ({ ...prev, [selected.remoteRunId]: payload }));
          setDatasetId(targetDatasetId);
          setPendingDatasetId((prev) => (prev === targetDatasetId ? '' : prev));
        }
      } catch (err) {
        if (!cancelled) {
          setRemoteError(err?.message || 'Failed to load run');
          setPendingDatasetId((prev) => (prev === targetDatasetId ? '' : prev));
        }
      } finally {
        if (!cancelled) {
          setRemoteLoading(false);
        }
      }
    };
    loadRun();
    return () => {
      cancelled = true;
    };
  }, [datasetId, pendingDatasetId, datasets]);

  const selected = datasets.find((d) => d.id === datasetId) || null;
  const selectedMeta = remoteRuns.find((run) => `run:${run?.id}` === datasetId) || null;
  const selectedPayload = selected?.data || null;
  const selectedRunId = selected?.remoteRunId || '';
  const activePriceRebase = normalizePriceRebase(priceRebaseByRunId[selectedRunId]);
  const priceRebaseFactor = activePriceRebase?.price_factor || 1;
  const visibleDatasetId = pendingDatasetId || datasetId || '';
  const selectionPending = Boolean(pendingDatasetId && pendingDatasetId !== datasetId);
  const deltaCheck = selectedPayload?.delta_check || (selectedRunId ? deltaByRunId[selectedRunId] : null) || null;
  const stage3 = useMemo(() => selectedPayload?.structured_data || selectedPayload || {}, [selectedPayload]);
  const freshness = useMemo(() => {
    const raw = selectedPayload?.freshness;
    if (raw && typeof raw === 'object') {
      const actionRaw = String(raw.recommended_action || '').trim().toLowerCase();
      return {
        ...raw,
        recommended_action: actionRaw === 'reuse_with_caution' ? 'review_soon' : raw.recommended_action,
      };
    }
    return computeFreshnessFallback(stage3, selectedPayload?.updated_at);
  }, [selectedPayload?.freshness, selectedPayload?.updated_at, stage3]);
  const mapped = useMemo(() => mapStage3ToGanttModel(stage3), [stage3]);
  const analystMemoMarkdown = selectionPending
    ? ''
    : String(
      selectedPayload?.analyst_memo_markdown
        || selectedPayload?.analyst_document?.content_markdown
        || ''
    );
  const memoStartsWithH1 = (() => {
    const lines = analystMemoMarkdown.replace(/\r\n/g, '\n').split('\n');
    const firstNonEmpty = lines.find((line) => String(line || '').trim()) || '';
    return /^#\s+/.test(String(firstNonEmpty).trim());
  })();
  const fallbackCompanyName = String(selected?.data?.structured_data?.company_name || selectedMeta?.company_name || '').trim();
  const memoTitle = (mapped?.companyName || fallbackCompanyName)
    ? `Investment Analysis: ${mapped.companyName || fallbackCompanyName}`
    : 'Investment Analysis';
  const bannerCompanyName = mapped?.companyName || fallbackCompanyName || 'Loading run…';
  const bannerTicker = mapped?.ticker || String(selectedMeta?.ticker || '').trim();
  const marketPathTicker = String(bannerTicker || selectedMeta?.ticker || '').trim().toUpperCase();
  const bannerThesis = selectionPending
    ? 'Loading selected analysis run…'
    : (stage3?.investment_recommendation?.summary || mapped.thesis || 'No thesis summary provided.');
  const selectedRunRouter = useMemo(
    () => selectedPayload?.scenario_router || {},
    [selectedPayload?.scenario_router]
  );
  const selectedRunRouterHasDecision = Boolean(
    selectedRunRouter
    && typeof selectedRunRouter === 'object'
    && (selectedRunRouter.action || selectedRunRouter.current_path || selectedRunRouter.announcement_title)
  );
  const goToScenarioRouter = useCallback(() => {
    navigateTo('/announcement-router');
  }, []);

  const currency = normalizeCurrencyCode(
    stage3?.market_data_provenance?.prepass_currency ||
    stage3?.market_data?.currency ||
    stage3?.market_facts?.normalized_facts?.currency ||
    'AUD'
  );
  const qScore = Number(stage3?.quality_score?.total);
  const vScore = Number(stage3?.value_score?.total);

  const targets12 = stage3?.price_targets?.scenario_targets?.['12m'] || stage3?.price_targets?.scenarios || {};
  const targets24 = stage3?.price_targets?.scenario_targets?.['24m'] || {};
  const p12 = stage3?.price_targets?.scenario_probabilities?.['12m'] || {};
  const p24 = stage3?.price_targets?.scenario_probabilities?.['24m'] || {};
  const currentSpotPrice = useMemo(
    () => inferCurrentPrice(stage3, selectedPayload),
    [stage3, selectedPayload]
  );
  const rawDisplayTargets12 = {
    bear: targets12.bear,
    base: toNumberOrNull(targets12.base) ?? toNumberOrNull(stage3?.price_targets?.target_12m),
    bull: targets12.bull,
  };
  const rawDisplayTargets24 = {
    bear: toNumberOrNull(targets24.bear) ?? toNumberOrNull(targets12.bear),
    base: toNumberOrNull(targets24.base) ?? toNumberOrNull(stage3?.price_targets?.target_24m),
    bull: toNumberOrNull(targets24.bull) ?? toNumberOrNull(targets12.bull),
  };
  const targetScaleFactor = targetScaleFactorForCurrent(currentSpotPrice, [
    ...Object.values(rawDisplayTargets12),
    ...Object.values(rawDisplayTargets24),
  ]);
  const scaledTargets12 = scaleTargets(rawDisplayTargets12, targetScaleFactor);
  const scaledTargets24 = scaleTargets(rawDisplayTargets24, targetScaleFactor);
  const weighted12 = weightedTargetForHorizon(scaledTargets12, p12);
  const weighted24 = weightedTargetForHorizon(scaledTargets24, p24);
  const priceDisplayCurrency = inferPriceDisplayCurrency(currency, currentSpotPrice, targetScaleFactor);
  const displayCurrentSpotPrice = rebasePrice(currentSpotPrice, priceRebaseFactor);
  const displayTargets12 = rebaseTargets(scaledTargets12, priceRebaseFactor);
  const displayTargets24 = rebaseTargets(scaledTargets24, priceRebaseFactor);
  const displayWeighted12 = rebasePrice(weighted12, priceRebaseFactor);
  const displayWeighted24 = rebasePrice(weighted24, priceRebaseFactor);
  const chartStartDate = String(
    selectedPayload?.updated_at
      || selectedMeta?.updated_at
      || selectedMeta?.created_at
      || stage3?.analysis_date
      || ''
  ).trim();

  const timeline = useMemo(
    () => normalizeTimelineRows(stage3?.development_timeline),
    [stage3?.development_timeline]
  );
  const thesisMap = useMemo(() => stage3?.thesis_map || {}, [stage3?.thesis_map]);
  const watch = useMemo(() => normalizeWatchlist(stage3, thesisMap), [stage3, thesisMap]);
  const verification = useMemo(() => normalizeVerificationQueue(stage3), [stage3]);
  const dissents = (stage3?.extended_analysis?.dissenting_views || []).slice(0, 6);
  const catalysts = useMemo(
    () => normalizeCatalysts(stage3?.extended_analysis?.next_major_catalysts),
    [stage3?.extended_analysis?.next_major_catalysts]
  );
  const aggregateRankings = Array.isArray(stage3?.council_metadata?.stage2_aggregate_rankings)
    ? stage3.council_metadata.stage2_aggregate_rankings
    : [];
  const judgeRankings = Array.isArray(stage3?.council_metadata?.stage2_judge_rankings)
    ? stage3.council_metadata.stage2_judge_rankings
    : [];
  const legacyTopModels = Array.isArray(stage3?.council_metadata?.top_ranked_models)
    ? stage3.council_metadata.top_ranked_models
    : [];
  const norm = stage3?.council_metadata?.normalization || {};
  const detLane = stage3?.council_metadata?.deterministic_finance_lane || {};
  const claimLedger = stage3?.council_metadata?.claim_ledger_counts || {};

  const chartPayload = {
    runDate: chartStartDate,
    current: displayCurrentSpotPrice,
    targets12: {
      bear: displayTargets12.bear || 0,
      base: displayTargets12.base || 0,
      bull: displayTargets12.bull || 0,
    },
    targets24: {
      bear: displayTargets24.bear || 0,
      base: displayTargets24.base || 0,
      bull: displayTargets24.bull || 0,
    },
    weighted12: displayWeighted12 ?? 0,
    weighted24: displayWeighted24 ?? 0,
  };
  const actualPricePoints = Array.isArray(securityHistoryPayload?.points)
    ? securityHistoryPayload.points
    : [];
  const marketPathRouterEvents = useMemo(() => {
    const rows = Array.isArray(routerOverlayEvents) ? [...routerOverlayEvents] : [];
    if (selectedRunRouterHasDecision) {
      rows.push({
        ...selectedRunRouter,
        date: selectedRunRouter.saved_at_utc || selectedRunRouter.received_at_utc || chartStartDate,
        title: selectedRunRouter.title || selectedRunRouter.announcement_title,
      });
    }
    const seen = new Set();
    return rows
      .filter((row) => row && typeof row === 'object')
      .filter((row) => {
        const key = String(row.event_id || `${row.date || row.saved_at_utc || ''}:${row.title || row.announcement_title || ''}`);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
  }, [chartStartDate, routerOverlayEvents, selectedRunRouter, selectedRunRouterHasDecision]);

  useEffect(() => {
    if (monitorOnly || !marketPathTicker) {
      setSecurityHistoryPayload(null);
      setRouterOverlayEvents([]);
      setMarketPathError('');
      return undefined;
    }
    let cancelled = false;
    const loadMarketPathContext = async () => {
      try {
        setMarketPathError('');
        const [history, routerEventsPayload] = await Promise.all([
          api.getSecurityPriceHistory(marketPathTicker),
          api.listAnnouncementRouterEvents(120, marketPathTicker),
        ]);
        if (cancelled) return;
        setSecurityHistoryPayload(history || null);
        setRouterOverlayEvents(Array.isArray(routerEventsPayload?.events) ? routerEventsPayload.events : []);
      } catch (err) {
        if (cancelled) return;
        setSecurityHistoryPayload(null);
        setRouterOverlayEvents([]);
        setMarketPathError(err?.message || 'Could not load market path overlays');
      }
    };
    loadMarketPathContext();
    return () => {
      cancelled = true;
    };
  }, [marketPathTicker, monitorOnly]);

  const freshnessStatus = String(freshness?.status || '').toLowerCase();
  const freshnessTone =
    freshnessStatus === 'fresh'
      ? 'bull'
      : freshnessStatus === 'stale'
        ? 'bear'
        : 'base';
  const freshnessAgeDays = Number(freshness?.age_days);
  const deltaFreshnessStatus = String(deltaCheck?.freshness_status || '').toLowerCase();
  const deltaCheckedAt = deltaCheck?.checked_at_utc;
  const deltaMaterialCount = Number(deltaCheck?.material_sources_count);
  const deltaTone =
    deltaFreshnessStatus === 'fresh'
      ? 'bull'
      : deltaFreshnessStatus === 'stale'
        ? 'bear'
        : 'base';

  const runDeltaCheckNow = async () => {
    if (!selectedRunId) return;
    try {
      setDeltaLoadingByRunId((prev) => ({ ...prev, [selectedRunId]: true }));
      setRemoteError('');
      const payload = await api.runDeltaCheck(selectedRunId, {
        force: false,
        max_sources: 12,
        lookback_days: 14,
      });
      if (payload && typeof payload === 'object') {
        setDeltaByRunId((prev) => ({ ...prev, [selectedRunId]: payload }));
      }
    } catch (err) {
      setRemoteError(err?.message || 'Failed to run delta-check');
    } finally {
      setDeltaLoadingByRunId((prev) => ({ ...prev, [selectedRunId]: false }));
    }
  };

  const timelineBars = timeline
    .map((row) => ({
      ...row,
      offset: toMonthOffsetFromNow(row.target_period),
    }))
    .sort((a, b) => {
      if (a.offset == null) return 1;
      if (b.offset == null) return -1;
      return a.offset - b.offset;
    });
  const scoreCards = [
    {
      label: 'Quality',
      value: qScore,
      tone: scoreTone(qScore),
      kind: 'score',
    },
    {
      label: 'Value',
      value: vScore,
      tone: scoreTone(vScore),
      kind: 'score',
    },
  ];
  const qualityBreakdown = Object.entries(stage3?.quality_score?.breakdown || {}).map(([k, v]) => ({
    key: k,
    label: labelForBreakdownKey(k),
    value: breakdownValue(v),
  }));
  const valueBreakdown = Object.entries(stage3?.value_score?.breakdown || {}).map(([k, v]) => ({
    key: k,
    label: labelForBreakdownKey(k),
    value: breakdownValue(v),
  }));
  const topVerification = verification.slice(0, 5);
  const retryLoadRuns = () => {
    setRunsReloadToken((prev) => prev + 1);
  };
  const openPriceAdjust = () => {
    const existing = activePriceRebase;
    setPriceAdjustError('');
    setPriceAdjustForm({
      type: existing?.type || 'share_consolidation',
      oldUnits: existing ? String(existing.old_units) : '10',
      newUnits: existing ? String(existing.new_units) : '1',
    });
    setPriceAdjustOpen(true);
  };
  const applyPriceAdjust = () => {
    if (!selectedRunId) return;
    const oldUnits = Number(priceAdjustForm.oldUnits);
    const newUnits = Number(priceAdjustForm.newUnits);
    const type = String(priceAdjustForm.type || '').trim();
    const normalized = normalizePriceRebase({
      type,
      old_units: oldUnits,
      new_units: newUnits,
      created_at: new Date().toISOString(),
    });
    if (!normalized) {
      setPriceAdjustError('Enter a valid positive ratio.');
      return;
    }
    setPriceRebaseByRunId((prev) => ({
      ...(prev || {}),
      [selectedRunId]: normalized,
    }));
    setPriceAdjustOpen(false);
  };
  const clearPriceAdjust = () => {
    if (!selectedRunId) return;
    setPriceRebaseByRunId((prev) => {
      const next = { ...(prev || {}) };
      delete next[selectedRunId];
      return next;
    });
    setPriceAdjustOpen(false);
    setPriceAdjustError('');
  };
  const deleteSelectedRun = async () => {
    const targetRunId = String(selectedRunId || '').trim();
    if (!targetRunId || deletingRunId) return;
    const targetLabel = String(selected?.label || targetRunId).trim();
    const ok = typeof window === 'undefined'
      ? true
      : window.confirm(`Delete run?\n\n${targetLabel}\n\nThis removes the run artifact and its sidecars.`);
    if (!ok) return;
    try {
      setDeletingRunId(targetRunId);
      setRemoteError('');
      await api.deleteGanttRun(targetRunId);
      setRemoteRuns((prev) => (Array.isArray(prev) ? prev.filter((run) => run?.id !== targetRunId) : []));
      setRemoteDataById((prev) => {
        const next = { ...(prev || {}) };
        delete next[targetRunId];
        return next;
      });
      setDeltaByRunId((prev) => {
        const next = { ...(prev || {}) };
        delete next[targetRunId];
        return next;
      });
      if (typeof window !== 'undefined') {
        const url = new URL(window.location.href);
        if (String(url.searchParams.get('run_id') || '').trim() === targetRunId) {
          url.searchParams.delete('run_id');
          window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`);
        }
      }
      setDatasetId((prev) => (prev === `run:${targetRunId}` ? '' : prev));
      setRunsReloadToken((prev) => prev + 1);
    } catch (err) {
      setRemoteError(err?.message || 'Failed to delete run');
    } finally {
      setDeletingRunId('');
    }
  };

  if (!datasets.length && !monitorOnly) {
    return (
      <div className="gantt-lab-root">
        <div className="gantt-lab-shell">
          <div className="gantt-lab-content">
            <div className="gantt-empty-state">
              <div className="gantt-empty-title">
                {preferredTickerFromUrl ? `No analysis runs loaded for ${preferredTickerFromUrl}` : 'No analysis runs loaded'}
              </div>
              <div className="gantt-empty-copy">
                Run an analysis from Council first, then inspect the timeline view here.
              </div>
              <div className="gantt-lab-controls">
                {!remoteLoading && (
                  <button type="button" className="gantt-lab-inline-retry" onClick={retryLoadRuns}>
                    Retry Fetch
                  </button>
                )}
                <button type="button" className="gantt-lab-back" onClick={backToCouncilChat}>
                  Back To Council
                </button>
              </div>
            </div>
            <div className="gantt-lab-status">
              <span>
                {remoteLoading ? 'Loading run artifacts...' : `No Stage 3 run artifacts found. ${remoteError ? `(${remoteError})` : ''}`}
              </span>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="gantt-lab-root">
      <div className="gantt-lab-shell">
      <div className="gantt-lab-content">
      {!monitorOnly && (
      <div className="gantt-lab-banner">
        <div>
          <div className="banner-name">
            {bannerCompanyName} {bannerTicker ? `(${bannerTicker})` : ''}
          </div>
          <div className="banner-thesis">
            {bannerThesis}
          </div>
        </div>
        <div className="banner-reco">
          <span className={`tone-${scoreTone(Number(stage3?.quality_score?.total ?? stage3?.quality_score))}`}>
            {stage3?.investment_recommendation?.rating || 'N/A'}
          </span>
          <strong>{stage3?.investment_recommendation?.conviction || 'N/A'}</strong>
        </div>
      </div>
      )}
      {!monitorOnly && (
      <div className="score-grid">
        {scoreCards.map((card) => (
          <div key={card.label} className={`score-card tone-${card.tone}`}>
            <label>{card.label}</label>
            <div className="score-value">
              {Number.isFinite(card.value)
                ? (card.kind === 'score' ? fmtScore(card.value) : fmtPct(card.value))
                : 'n/a'}
            </div>
            {card.help && <div className="score-help">{card.help}</div>}
          </div>
        ))}
        <div className="score-card tone-base">
          <label>24M Prob-Weighted</label>
          <div className="score-value">{fmtMoney(displayWeighted24, priceDisplayCurrency)}</div>
        </div>
        <div className="stats-heading-wrap score-card tone-base" tabIndex={0}>
          <label>Stats</label>
          <div className="stats-card-copy">Hover to reveal decomposition</div>
          <div className="stats-hover-reveal">
            <div className="stats-hover-columns">
              <div className="stats-hover-col">
                <h4>Quality Breakdown</h4>
                {qualityBreakdown.map((row) => (
                  <div className="stats-hover-row" key={`stats-q-${row.key}`}>
                    <span>{row.label}</span>
                    <strong>{fmtScorePct(row.value, 1)}</strong>
                  </div>
                ))}
                {!qualityBreakdown.length && <div className="cond-empty">n/a</div>}
              </div>
              <div className="stats-hover-col">
                <h4>Value Breakdown</h4>
                {valueBreakdown.map((row) => (
                  <div className="stats-hover-row" key={`stats-v-${row.key}`}>
                    <span>{row.label}</span>
                    <strong>{fmtScorePct(row.value, 1)}</strong>
                  </div>
                ))}
                {!valueBreakdown.length && <div className="cond-empty">n/a</div>}
              </div>
            </div>
          </div>
        </div>
      </div>
      )}

      {monitorOnly && (
      <AnnouncementRouterMonitor />
      )}

      {!monitorOnly && (
      <div className="lab-chart-toolbar" aria-label="Scenario chart overlays">
        <span>Chart overlays</span>
        <button
          type="button"
          className={showPriceHistoryOverlay ? 'is-active' : ''}
          onClick={() => setShowPriceHistoryOverlay((value) => !value)}
        >
          Price history
        </button>
        <button
          type="button"
          className={showRouterEventOverlay ? 'is-active' : ''}
          onClick={() => setShowRouterEventOverlay((value) => !value)}
        >
          Router events
        </button>
        {securityHistoryPayload?.latest?.date && (
          <em>Latest Alpha Edge price: {fmtMoney(securityHistoryPayload.latest.price, priceDisplayCurrency)} on {securityHistoryPayload.latest.date}</em>
        )}
        {marketPathError && <em>{marketPathError}</em>}
      </div>
      )}

      {!monitorOnly && (
      <ScenarioTimelineUnit
        data={chartPayload}
        currency={priceDisplayCurrency}
        timelineBars={timelineBars}
        orientation={timelineOrientation}
        actualPrices={actualPricePoints}
        routerEvents={marketPathRouterEvents}
        showPriceHistory={showPriceHistoryOverlay}
        showRouterEvents={showRouterEventOverlay}
        startDate={chartStartDate}
      />
      )}

      {!monitorOnly && (
      <section className="lab-panel analyst-snapshot-panel">
        <h3>Analyst Snapshot</h3>
        <div className="analyst-snapshot-grid">
          <article className="snapshot-card">
            <h4>Thesis & Recommendation</h4>
            <p className="snapshot-thesis">
              {stage3?.investment_recommendation?.summary || mapped.thesis || 'No thesis summary provided.'}
            </p>
            <div className="snapshot-meta-row">
              <span>Rating</span>
              <strong>{stage3?.investment_recommendation?.rating || stage3?.investment_verdict?.rating || 'N/A'}</strong>
            </div>
            <div className="snapshot-meta-row">
              <span>Conviction</span>
              <strong>{stage3?.investment_recommendation?.conviction || stage3?.investment_verdict?.conviction || 'N/A'}</strong>
            </div>
            <div className="snapshot-meta-row">
              <span>Current Price</span>
              <strong>{fmtMoney(displayCurrentSpotPrice, priceDisplayCurrency)}</strong>
            </div>
            {activePriceRebase && (
              <div className="snapshot-adjust-note">
                Adjusted {formatRebaseRatio(activePriceRebase)}
              </div>
            )}
            <h4>Price Scenarios</h4>
            <div className="snapshot-horizon">
              <div className="snapshot-horizon-title">12M</div>
              <div className="snapshot-scenario-row"><span>Bear</span><span>{fmtMoney(displayTargets12.bear, priceDisplayCurrency)}</span><span>{fmtPct(normalizeProb(p12.bear))}</span></div>
              <div className="snapshot-scenario-row"><span>Base</span><span>{fmtMoney(displayTargets12.base, priceDisplayCurrency)}</span><span>{fmtPct(normalizeProb(p12.base))}</span></div>
              <div className="snapshot-scenario-row"><span>Bull</span><span>{fmtMoney(displayTargets12.bull, priceDisplayCurrency)}</span><span>{fmtPct(normalizeProb(p12.bull))}</span></div>
              <div className="snapshot-scenario-row snapshot-scenario-row-em"><span>Prob-weighted</span><strong>{fmtMoney(displayWeighted12, priceDisplayCurrency)}</strong><span /></div>
            </div>
            <div className="snapshot-horizon">
              <div className="snapshot-horizon-title">24M</div>
              <div className="snapshot-scenario-row"><span>Bear</span><span>{fmtMoney(displayTargets24.bear, priceDisplayCurrency)}</span><span>{fmtPct(normalizeProb(p24.bear))}</span></div>
              <div className="snapshot-scenario-row"><span>Base</span><span>{fmtMoney(displayTargets24.base, priceDisplayCurrency)}</span><span>{fmtPct(normalizeProb(p24.base))}</span></div>
              <div className="snapshot-scenario-row"><span>Bull</span><span>{fmtMoney(displayTargets24.bull, priceDisplayCurrency)}</span><span>{fmtPct(normalizeProb(p24.bull))}</span></div>
              <div className="snapshot-scenario-row snapshot-scenario-row-em"><span>Prob-weighted</span><strong>{fmtMoney(displayWeighted24, priceDisplayCurrency)}</strong><span /></div>
            </div>
          </article>

          <article className="snapshot-card">
            <h4>Verification Priorities</h4>
            <ul className="snapshot-list">
              {topVerification.map((row) => (
                <li key={`snap-v-${row._k || row.field}`}>
                  <div>{labelForFieldPath(row.field)}</div>
                  <div className="snapshot-sub">{String(row.priority || 'medium').toUpperCase()} · {row.required_source || 'Primary source required'}</div>
                </li>
              ))}
              {!topVerification.length && <li className="snapshot-sub">No verification items.</li>}
            </ul>
          </article>
        </div>
      </section>
      )}

      {!monitorOnly && (
      <AnnouncementRouterMonitor
        embedded
        runRouter={selectedRunRouterHasDecision ? selectedRunRouter : null}
        selectedRunId={selectedRunId}
        selectedTicker={bannerTicker}
        onOpenFullMonitor={goToScenarioRouter}
      />
      )}

      {!monitorOnly && (
      <details className="debug-panels">
        <summary>Debug Panels (expand)</summary>
        <div className="lab-layout">
        <section className="lab-panel">
          <h3>Run Metadata + Freshness</h3>
          <div className="run-meta-grid">
            <div className="run-meta-row">
              <span>Run</span>
              <strong>{selected?.label || selectedRunId || 'n/a'}</strong>
            </div>
            <div className="run-meta-row">
              <span>Freshness</span>
              <strong className={`tone-${freshnessTone}`}>
                {freshnessStatus || 'n/a'} {Number.isFinite(freshnessAgeDays) ? `· ${freshnessAgeDays}d` : ''}
              </strong>
            </div>
            <div className="run-meta-row">
              <span>Delta Check</span>
              <strong className={`tone-${deltaTone}`}>
                {deltaCheck
                  ? `${deltaFreshnessStatus || 'watch'} · ${Number.isFinite(deltaMaterialCount) ? `${deltaMaterialCount} material` : 'n/a'}`
                  : 'not run'}
              </strong>
            </div>
            <div className="run-meta-row">
              <span>Last Delta</span>
              <strong>{deltaCheck ? fmtRelativeSince(deltaCheckedAt) : 'n/a'}</strong>
            </div>
            <div className="run-meta-row">
              <span>Artifact Load</span>
              <strong>
                {remoteLoading ? 'loading' : remoteError ? 'error' : 'ok'}
              </strong>
            </div>
          </div>
          {freshness?.reason && <div className="run-meta-note">{freshness.reason}</div>}
          {remoteError && <div className="run-meta-note run-meta-note-error">Run artifacts error: {remoteError}</div>}
          <div className="run-meta-actions">
            <button
              type="button"
              className="delta-check-btn"
              onClick={runDeltaCheckNow}
              disabled={!selectedRunId || Boolean(deltaLoadingByRunId[selectedRunId])}
            >
              {deltaLoadingByRunId[selectedRunId] ? 'Checking…' : 'Run Delta Check'}
            </button>
            {!remoteLoading && (
              <button type="button" className="gantt-lab-inline-retry" onClick={retryLoadRuns}>
                Retry Fetch
              </button>
            )}
          </div>
        </section>

        <section className="lab-panel lab-panel-wide thesis-map-panel">
          <div className="panel-title-row">
            <h3>Thesis Map Condition Engine</h3>
            <span>Bull / base / bear monitor</span>
          </div>
          <div className="thesis-grid">
            {['bull', 'base', 'bear'].map((name) => {
              const rawBlock = thesisMap?.[name];
              const block = rawBlock && typeof rawBlock === 'object' && !Array.isArray(rawBlock) ? rawBlock : {};
              const conditions = (Array.isArray(block.required_conditions) ? block.required_conditions : [])
                .map((condition, idx) => normalizeThesisCondition(condition, name, 'required', idx))
                .filter(Boolean);
              const failures = (Array.isArray(block.failure_conditions) ? block.failure_conditions : [])
                .map((condition, idx) => normalizeThesisCondition(condition, name, 'failure', idx))
                .filter(Boolean);
              const rawTarget12 = toNumberOrNull(block.target_12m) ?? toNumberOrNull(targets12?.[name]);
              const rawTarget24 = toNumberOrNull(block.target_24m) ?? toNumberOrNull(targets24?.[name]);
              const target12 = rebasePrice(scalePrice(rawTarget12, targetScaleFactor), priceRebaseFactor);
              const target24 = rebasePrice(scalePrice(rawTarget24, targetScaleFactor), priceRebaseFactor);
              const prob = toNumberOrNull(block.probability_24m_pct ?? block.probability_pct);
              const logicRequired = String(block?.condition_logic?.required_conditions || '').trim();
              const logicFailure = String(block?.condition_logic?.failure_conditions || '').trim();
              const summary = String(block.summary || '').trim();
              const positioning = String(block.current_positioning || '').trim();
              const why = String(block.why_current_positioning || '').trim();
              return (
                <article key={name} className={`thesis-card tone-${scenarioTone(name)}`}>
                  <header>
                    <strong>{name.toUpperCase()}</strong>
                    {(target12 != null || target24 != null) && (
                      <span>
                        {target12 != null ? `12M ${fmtMoney(target12, priceDisplayCurrency)}` : '12M -'}
                        {' · '}
                        {target24 != null ? `24M ${fmtMoney(target24, priceDisplayCurrency)}` : '24M -'}
                      </span>
                    )}
                    {prob != null && <span>Prob {fmtPct(prob)}</span>}
                  </header>
                  {summary && <div className="logic-line">Summary: {summary}</div>}
                  {(positioning || why) && (
                    <div className="logic-line">
                      Positioning: {positioning || 'n/a'}
                      {why ? ` · ${why}` : ''}
                    </div>
                  )}
                  {(logicRequired || logicFailure) && (
                    <div className="logic-line">
                      Logic: required {logicRequired || 'n/a'} · failure {logicFailure || 'n/a'}
                    </div>
                  )}
                  {conditions.length > 0 && <div className="cond-section-label">Required conditions</div>}
                  <div className="cond-list">
                    {conditions.map((c) => (
                      <div key={c.key} className="cond-item">
                        <div className="cond-top">
                          <span className={`chip tone-${c.chipTone}`}>{c.chip}</span>
                          {c.id && <span>{c.id}</span>}
                        </div>
                        <div className="cond-main">{c.text}</div>
                        {c.meta.length > 0 && <div className="cond-sub">{c.meta.join(' · ')}</div>}
                      </div>
                    ))}
                    {!conditions.length && <div className="cond-empty">No required conditions provided.</div>}
                  </div>
                  {failures.length > 0 && <div className="cond-section-label cond-section-label-failure">Failure conditions</div>}
                  <div className="cond-list">
                    {failures.map((c) => (
                      <div key={c.key} className="cond-item">
                        <div className="cond-top">
                          <span className={`chip tone-${c.chipTone}`}>{c.chip}</span>
                          {c.id && <span>{c.id}</span>}
                        </div>
                        <div className="cond-main">{c.text}</div>
                        {c.meta.length > 0 && <div className="cond-sub">{c.meta.join(' · ')}</div>}
                      </div>
                    ))}
                    {!failures.length && <div className="cond-empty">No failure conditions provided.</div>}
                  </div>
                </article>
              );
            })}
          </div>
        </section>

        <section className="lab-panel lab-panel-wide watchlist-panel">
          <div className="panel-title-row">
            <h3>Monitoring Watchlist</h3>
            <span>{(watch?.red_flags || []).length} red flags · {(watch?.confirmatory_signals || []).length} confirmatory</span>
          </div>
          <div className="watch-columns">
            <div className="watch-column">
              <h4>Red Flags</h4>
              <div className="watch-list">
                {(watch?.red_flags || []).map((row) => (
                  <div className="watch-item watch-red" key={row.watch_id || row.condition}>
                    <div className="watch-title">{row.condition}</div>
                    <div className="watch-meta">
                      {row.trigger_window || 'No window'} · {row.duration || 'No duration'} · {String(row.severity || 'n/a').toUpperCase()}
                    </div>
                  </div>
                ))}
                {!(watch?.red_flags || []).length && <div className="watch-empty">No red flags.</div>}
              </div>
            </div>
            <div className="watch-column">
              <h4>Confirmatory Signals</h4>
              <div className="watch-list">
                {(watch?.confirmatory_signals || []).map((row) => (
                  <div className="watch-item watch-green" key={row.watch_id || row.condition}>
                    <div className="watch-title">{row.condition}</div>
                    <div className="watch-meta">{row.source_to_monitor || 'Company filings and milestone updates'}</div>
                  </div>
                ))}
                {!(watch?.confirmatory_signals || []).length && <div className="watch-empty">No confirmatory signals.</div>}
              </div>
            </div>
          </div>
        </section>

        <section className="lab-panel lab-panel-wide verification-panel">
          <div className="panel-title-row">
            <h3>Verification Queue</h3>
            <span>{verification.length} unresolved fields</span>
          </div>
          <div className="verify-list">
            {verification.map((v) => (
              <div className="verify-item" key={v._k || v.field}>
                <div className="verify-title">
                  <strong>{labelForFieldPath(v.field)}</strong>
                  <span className={`chip tone-${v.priority === 'high' ? 'bear' : v.priority === 'medium' ? 'base' : 'bull'}`}>
                    {String(v.priority || 'n/a').toUpperCase()}
                  </span>
                </div>
                <div className="verify-path">{v.field}</div>
                <div className="verify-meta">{v.reason}</div>
                <div className="verify-meta">Need: {v.required_source}</div>
              </div>
            ))}
            {!verification.length && <div className="watch-empty">No verification fields.</div>}
          </div>
        </section>

        <section className="lab-panel lab-panel-wide">
          <h3>Council Telemetry + Provenance</h3>
          <div className="telemetry-grid">
            <div className="telemetry-wide">
              <h4>Full Stage 2 Ranking</h4>
              {aggregateRankings.length ? (
                <div className="ranking-table">
                  {aggregateRankings.map((row, idx) => (
                    <div className="ranking-row" key={`${row.model}-${idx}`}>
                      <span className="ranking-pos">{idx + 1}</span>
                      <span className="ranking-model-name">{row.model || 'n/a'}</span>
                      <span>avg {fmtRankMetric(row.average_rank)}</span>
                      <span>{row.rankings_count ?? 0} votes</span>
                      <span>{row.first_place_votes ?? 0} firsts</span>
                      <span>borda {row.borda_score ?? 'n/a'}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <ul>
                  {legacyTopModels.map((m) => (
                    <li key={m}>{m}</li>
                  ))}
                  {!legacyTopModels.length && <li>n/a</li>}
                </ul>
              )}
            </div>
            <div className="telemetry-wide">
              <h4>Judge Ballots</h4>
              {judgeRankings.length ? (
                <div className="judge-ballots">
                  {judgeRankings.map((ballot, idx) => (
                    <details key={`${ballot.judge_model || 'judge'}-${idx}`}>
                      <summary>{ballot.judge_model || 'Unknown judge'}</summary>
                      <ol>
                        {(ballot.ranking || []).map((entry) => (
                          <li key={`${ballot.judge_model}-${entry.rank}-${entry.model || entry.label}`}>
                            {entry.model || entry.label || 'n/a'}
                          </li>
                        ))}
                      </ol>
                    </details>
                  ))}
                </div>
              ) : (
                <div className="cond-empty">No judge ballots available.</div>
              )}
            </div>
            <div>
              <h4>Normalizer</h4>
              <ul>
                <li>Style: {norm?.chairman_output_style || 'n/a'}</li>
                <li>Model: {norm?.jsonifier_model || 'n/a'}</li>
                <li>Forced: {String(norm?.jsonifier_forced)}</li>
                <li>Used: {String(norm?.jsonifier_used)}</li>
                <li>Response chars: {norm?.jsonifier_response_length || 'n/a'}</li>
              </ul>
            </div>
            <div>
              <h4>Claim Ledger</h4>
              <ul>
                <li>Raw claims: {claimLedger?.raw_claims ?? 'n/a'}</li>
                <li>Resolved fields: {claimLedger?.resolved_fields ?? 'n/a'}</li>
                <li>Conflicts: {claimLedger?.conflicts ?? 'n/a'}</li>
              </ul>
            </div>
            <div>
              <h4>Deterministic Lane</h4>
              <ul>
                <li>Status: {detLane?.status || 'n/a'}</li>
                <li>Stage: {detLane?.project_stage || 'n/a'}</li>
                <li>Missing: {(detLane?.missing_critical_fields || []).join(', ') || 'none'}</li>
              </ul>
            </div>
          </div>
        </section>

        <section className="lab-panel">
          <h3>Catalysts + Dissent</h3>
          <h4>Next Catalysts</h4>
          <ul className="simple-list">
            {catalysts.map((c, i) => (
              <li key={`${i}-${c}`}>{c}</li>
            ))}
            {!catalysts.length && <li>n/a</li>}
          </ul>
          <h4>Dissenting Views</h4>
          <ul className="simple-list">
            {dissents.map((d, i) => (
              <li key={`${i}-${d}`}>{d}</li>
            ))}
            {!dissents.length && <li>n/a</li>}
          </ul>
        </section>

        <section className="lab-panel">
          <h3>Raw JSON Explorer</h3>
          <details open>
            <summary>price_targets</summary>
            <pre>{JSON.stringify(stage3?.price_targets || {}, null, 2)}</pre>
          </details>
          <details>
            <summary>thesis_map</summary>
            <pre>{JSON.stringify(stage3?.thesis_map || {}, null, 2)}</pre>
          </details>
          <details>
            <summary>monitoring_watchlist</summary>
            <pre>{JSON.stringify(stage3?.monitoring_watchlist || {}, null, 2)}</pre>
          </details>
          <details>
            <summary>full structured_data</summary>
            <pre>{JSON.stringify(stage3 || {}, null, 2)}</pre>
          </details>
        </section>
      </div>
      </details>
      )}
      {!monitorOnly && (
      <div className="gantt-lab-toolbar gantt-lab-toolbar-bottom">
        <div className="gantt-lab-controls">
          <select value={visibleDatasetId} onChange={(e) => requestDatasetSelection(e.target.value)} className="gantt-lab-select">
            {datasets.map((ds) => (
              <option key={ds.id} value={ds.id}>
                {ds.optionLabel || ds.label}
              </option>
            ))}
          </select>
          <button
            type="button"
            className="gantt-lab-delete"
            onClick={deleteSelectedRun}
            disabled={!selectedRunId || Boolean(deletingRunId)}
            title={selectedRunId ? 'Delete selected run artifact family' : 'No run selected'}
          >
            {deletingRunId && deletingRunId === selectedRunId ? 'Deleting…' : 'Delete Run'}
          </button>
          <button
            type="button"
            className="gantt-lab-adjust"
            onClick={openPriceAdjust}
            disabled={!selectedRunId}
            title={selectedRunId ? 'Adjust displayed per-share prices' : 'No run selected'}
          >
            Adjust
          </button>
          {activePriceRebase && (
            <span className="price-adjust-pill">
              {formatRebaseRatio(activePriceRebase)}
            </span>
          )}
          <button
            type="button"
            className={`gantt-lab-toggle ${timelineOrientation === 'horizontal' ? 'is-horizontal' : 'is-vertical'}`}
            onClick={() => setTimelineOrientation((prev) => (prev === 'vertical' ? 'horizontal' : 'vertical'))}
            aria-label={`Switch to ${timelineOrientation === 'vertical' ? 'horizontal' : 'vertical'} chart/catalyst layout`}
          >
            {timelineOrientation === 'vertical' ? 'Layout: Vertical' : 'Layout: Horizontal'}
          </button>
          <button type="button" className="gantt-lab-back" onClick={backToCouncilChat}>
            Back To Council
          </button>
        </div>
      </div>
      )}
      </div>
      {!monitorOnly && (
      <aside className="gantt-memo-pane">
        {!memoStartsWithH1 && <h1 className="memo-h1">{memoTitle}</h1>}
        {selectionPending
          ? <p className="memo-empty">Loading selected run…</p>
          : renderMarkdownBlocks(analystMemoMarkdown)}
      </aside>
      )}
      {priceAdjustOpen && !monitorOnly && (
        <div
          className="price-adjust-overlay"
          role="presentation"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) setPriceAdjustOpen(false);
          }}
        >
          <div className="price-adjust-modal" role="dialog" aria-modal="true" aria-label="Adjust price">
            <div className="price-adjust-head">
              <h3>Adjust Price</h3>
              <button
                type="button"
                className="price-adjust-close"
                onClick={() => setPriceAdjustOpen(false)}
                aria-label="Close adjust price dialog"
              >
                x
              </button>
            </div>
            <div className="price-adjust-type-row">
              <button
                type="button"
                className={`price-adjust-type ${priceAdjustForm.type === 'share_consolidation' ? 'is-active' : ''}`}
                onClick={() => {
                  setPriceAdjustError('');
                  setPriceAdjustForm({ type: 'share_consolidation', oldUnits: '10', newUnits: '1' });
                }}
              >
                Consolidation
              </button>
              <button
                type="button"
                className={`price-adjust-type ${priceAdjustForm.type === 'share_split' ? 'is-active' : ''}`}
                onClick={() => {
                  setPriceAdjustError('');
                  setPriceAdjustForm({ type: 'share_split', oldUnits: '1', newUnits: '10' });
                }}
              >
                Split
              </button>
            </div>
            <label className="price-adjust-label" htmlFor="price-adjust-old">Ratio</label>
            <div className="price-adjust-ratio-row">
              <input
                id="price-adjust-old"
                className="price-adjust-ratio-input"
                inputMode="decimal"
                value={priceAdjustForm.oldUnits}
                onChange={(event) => setPriceAdjustForm((prev) => ({ ...prev, oldUnits: event.target.value }))}
              />
              <span>:</span>
              <input
                className="price-adjust-ratio-input"
                inputMode="decimal"
                value={priceAdjustForm.newUnits}
                onChange={(event) => setPriceAdjustForm((prev) => ({ ...prev, newUnits: event.target.value }))}
              />
            </div>
            <div className="price-adjust-example">10:1 means 10 old shares became 1 new share.</div>
            {priceAdjustError && <div className="price-adjust-error">{priceAdjustError}</div>}
            <div className="price-adjust-actions">
              {activePriceRebase && (
                <button type="button" className="price-adjust-secondary" onClick={clearPriceAdjust}>
                  Clear
                </button>
              )}
              <button type="button" className="price-adjust-secondary" onClick={() => setPriceAdjustOpen(false)}>
                Cancel
              </button>
              <button type="button" className="price-adjust-primary" onClick={applyPriceAdjust}>
                Apply
              </button>
            </div>
          </div>
        </div>
      )}
      </div>
    </div>
  );
}
