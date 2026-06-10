const CHART_METRICS = {
  width: 820,
  height: 374,
  margin: { top: 12, right: 24, bottom: 58, left: 58 },
};

const chartX = (months, domainStart = 0, domainEnd = 24) => {
  const { width, margin } = CHART_METRICS;
  const plotW = width - margin.left - margin.right;
  const span = Math.max(1, domainEnd - domainStart);
  return margin.left + ((months - domainStart) / span) * plotW;
};

const LANE_AXIS_STYLE = {
  '--lane-axis-left': `${(CHART_METRICS.margin.left / CHART_METRICS.width) * 100}%`,
  '--lane-axis-right': `${(CHART_METRICS.margin.right / CHART_METRICS.width) * 100}%`,
};

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

function parseDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function shortDate(value) {
  const parsed = parseDate(value);
  if (!parsed) return 'n/a';
  return parsed.toISOString().slice(0, 10);
}

function addMonths(dateValue, months) {
  const parsed = parseDate(dateValue);
  if (!parsed) return null;
  return new Date(Date.UTC(
    parsed.getUTCFullYear(),
    parsed.getUTCMonth() + months,
    1
  ));
}

function monthAxisLabel(dateValue, months) {
  const date = addMonths(dateValue, months);
  if (!date) return '';
  return date.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' });
}

function horizonAxisLabel(months) {
  if (months === 0) return 'Run';
  if (months % 6 === 0) return `+${months}M`;
  return '';
}

function monthOffsetFromDate(dateValue, startDate) {
  const date = parseDate(dateValue);
  const start = parseDate(startDate);
  if (!date || !start) return null;
  return (date.getTime() - start.getTime()) / (1000 * 60 * 60 * 24 * 30.4375);
}

function normalizePricePoints(points, startDate, runPrice = null) {
  const rows = (Array.isArray(points) ? points : [])
    .map((point) => {
      const price = Number(point?.price);
      const m = monthOffsetFromDate(point?.date || point?.observed_at, startDate);
      if (!Number.isFinite(price) || price <= 0 || m == null || m < 0 || m > 24) return null;
      return {
        ...point,
        date: shortDate(point?.date || point?.observed_at),
        m,
        price,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.m - b.m);
  const anchorPrice = Number(runPrice);
  if (Number.isFinite(anchorPrice) && anchorPrice > 0 && (!rows.length || rows[0].m > 0.05)) {
    rows.unshift({
      date: shortDate(startDate),
      m: 0,
      price: anchorPrice,
      source: 'run baseline',
    });
  }
  return rows;
}

function smoothedPricePoints(points) {
  if (!Array.isArray(points) || points.length <= 2) return points || [];
  const bucketSizeMonths = 0.25;
  const buckets = new Map();
  points.forEach((point) => {
    const key = Math.floor(point.m / bucketSizeMonths);
    const bucket = buckets.get(key) || [];
    bucket.push(point);
    buckets.set(key, bucket);
  });
  const bucketed = Array.from(buckets.values()).map((bucket) => {
    const last = bucket[bucket.length - 1];
    const sum = bucket.reduce((acc, point) => ({
      m: acc.m + point.m,
      price: acc.price + point.price,
    }), { m: 0, price: 0 });
    return {
      ...last,
      m: sum.m / bucket.length,
      price: sum.price / bucket.length,
    };
  });
  return bucketed.map((point, idx) => {
    if (idx === 0 || idx === bucketed.length - 1) return point;
    const prev = bucketed[idx - 1];
    const next = bucketed[idx + 1];
    return {
      ...point,
      price: (prev.price + point.price + next.price) / 3,
    };
  });
}

function todayPriceMarker(latestPoint, startDate) {
  if (!latestPoint) return null;
  const todayM = monthOffsetFromDate(new Date().toISOString(), startDate);
  if (todayM == null || todayM < 0 || todayM > 24) return latestPoint;
  return {
    ...latestPoint,
    m: Math.max(latestPoint.m, todayM),
    markerDate: shortDate(new Date().toISOString()),
  };
}

function smoothSvgPath(points, x, y) {
  if (!Array.isArray(points) || !points.length) return '';
  const coords = points.map((point) => ({ x: x(point.m), y: y(point.price) }));
  if (coords.length === 1) return `M ${coords[0].x} ${coords[0].y}`;
  return coords.reduce((path, point, idx) => {
    if (idx === 0) return `M ${point.x} ${point.y}`;
    const p0 = coords[idx - 2] || coords[idx - 1];
    const p1 = coords[idx - 1];
    const p2 = point;
    const p3 = coords[idx + 1] || p2;
    const cp1x = p1.x + (p2.x - p0.x) / 6;
    const cp1y = p1.y + (p2.y - p0.y) / 6;
    const cp2x = p2.x - (p3.x - p1.x) / 6;
    const cp2y = p2.y - (p3.y - p1.y) / 6;
    return `${path} C ${cp1x} ${cp1y} ${cp2x} ${cp2y} ${p2.x} ${p2.y}`;
  }, '');
}

function markerTone(event) {
  const raw = String(event?.tone || event?.trajectory_state || event?.impact_level || '').trim().toLowerCase();
  if (raw.includes('strength') || raw.includes('accelerat') || raw.includes('reduced') || raw.includes('positive')) return 'positive';
  if (raw.includes('weaken') || raw.includes('delay') || raw.includes('increased') || raw.includes('negative') || raw.includes('bear')) return 'negative';
  return 'neutral';
}

function normalizeRouterEvents(events, startDate) {
  return (Array.isArray(events) ? events : [])
    .map((event) => {
      const eventDate = event?.date || event?.saved_at_utc || event?.received_at_utc || event?.occurred_at;
      const m = monthOffsetFromDate(eventDate, startDate);
      if (m == null || m < 0 || m > 24) return null;
      const tone = markerTone(event);
      if (tone !== 'positive' && tone !== 'negative') return null;
      return {
        ...event,
        date: shortDate(eventDate),
        m,
        tone,
        title: String(event?.title || event?.announcement_title || 'Routed announcement').trim(),
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.m - b.m);
}

function nearestPrice(points, month) {
  if (!points.length) return null;
  return points.reduce((best, point) => (
    !best || Math.abs(point.m - month) < Math.abs(best.m - month) ? point : best
  ), null);
}

function statusTone(status) {
  const s = String(status || '').toLowerCase();
  if (s.includes('met') || s.includes('likely') || s.includes('planned') || s.includes('imminent')) return 'bull';
  if (s.includes('risk')) return 'bear';
  return 'base';
}

function statusLabel(status) {
  const s = String(status || '').trim().toLowerCase();
  if (s === 'at_risk' || s === 'at risk') return 'At risk';
  if (s === 'achieved' || s === 'completed' || s === 'complete') return 'Achieved';
  if (s === 'current' || s === 'in_progress' || s === 'in progress') return 'In progress';
  if (s === 'planned') return 'Planned';
  return s ? s.replace(/_/g, ' ') : 'n/a';
}

export default function ScenarioTimelineUnit({
  data,
  currency,
  timelineBars,
  orientation = 'vertical',
  actualPrices = [],
  routerEvents = [],
  showPriceHistory = false,
  showRouterEvents = false,
  startDate = '',
}) {
  const { width, height, margin } = CHART_METRICS;
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;

  const runStart = parseDate(startDate || data?.runDate || data?.asOfDate) || new Date();
  const rawPricePoints = showPriceHistory ? normalizePricePoints(actualPrices, runStart, data?.current) : [];
  const latestRawPricePoint = rawPricePoints.length ? rawPricePoints[rawPricePoints.length - 1] : null;
  const currentPricePoint = todayPriceMarker(latestRawPricePoint, runStart);
  const pricePoints = smoothedPricePoints(
    currentPricePoint && latestRawPricePoint && currentPricePoint.m > latestRawPricePoint.m
      ? [...rawPricePoints, currentPricePoint]
      : rawPricePoints
  );
  const routeMarkers = showRouterEvents ? normalizeRouterEvents(routerEvents, runStart) : [];
  const domainStart = 0;
  const domainEnd = 24;
  const x = (m) => chartX(m, domainStart, domainEnd);

  const now = runStart;
  const yearSegments = (() => {
    const startYear = now.getFullYear();
    const startMonth = now.getMonth();
    const boundaries = [0];
    const firstYearRollover = 12 - startMonth;
    if (firstYearRollover > 0 && firstYearRollover < 24) boundaries.push(firstYearRollover);
    for (let next = firstYearRollover + 12; next < 24; next += 12) boundaries.push(next);
    boundaries.push(24);

    const segments = [];
    let year = startYear;
    for (let i = 0; i < boundaries.length - 1; i += 1) {
      const start = boundaries[i];
      const end = boundaries[i + 1];
      if (end - start >= 1.5) {
        segments.push({
          center: (start + end) / 2,
          label: String(year),
        });
      }
      year += 1;
    }
    return segments;
  })();

  const values = [
    data.current,
    data.targets12.bear,
    data.targets12.base,
    data.targets12.bull,
    data.targets24.bear,
    data.targets24.base,
    data.targets24.bull,
    data.weighted12,
    data.weighted24,
    ...rawPricePoints.map((point) => point.price),
    currentPricePoint?.price,
  ].filter((v) => Number.isFinite(v) && v > 0);
  const safeValues = values.length ? values : [1];
  const min = Math.min(...safeValues);
  const max = Math.max(...safeValues);
  const pad = Math.max((max - min) * 0.18, max * 0.08, 0.08);
  const yMin = Math.max(0, min - pad);
  const yMax = max + pad;
  const y = (v) => margin.top + ((yMax - v) / Math.max(yMax - yMin, 0.001)) * plotH;

  const hasCurrentPoint = Number.isFinite(data.current) && data.current > 0;

  const series = {
    bear: [
      ...(hasCurrentPoint ? [{ m: 0, v: data.current }] : []),
      { m: 12, v: data.targets12.bear },
      { m: 24, v: data.targets24.bear },
    ],
    base: [
      ...(hasCurrentPoint ? [{ m: 0, v: data.current }] : []),
      { m: 12, v: data.targets12.base },
      { m: 24, v: data.targets24.base },
    ],
    bull: [
      ...(hasCurrentPoint ? [{ m: 0, v: data.current }] : []),
      { m: 12, v: data.targets12.bull },
      { m: 24, v: data.targets24.bull },
    ],
  };

  const shades = {
    bear: '#e05952',
    base: '#f2b948',
    bull: '#33c08f',
  };

  const ribbonPoints = [
    ...series.bull.map((p) => `${x(p.m)},${y(p.v)}`),
    ...[...series.bear].reverse().map((p) => `${x(p.m)},${y(p.v)}`),
  ].join(' ');

  const yTicks = Array.from({ length: 6 }, (_, i) => {
    const v = yMin + ((yMax - yMin) * i) / 5;
    return Number(v.toFixed(2));
  }).filter((t) => Number.isFinite(t) && Number.isFinite(y(t)));

  const weightedPath = [
    { m: 12, v: data.weighted12 },
    { m: 24, v: data.weighted24 },
  ].filter((p) => Number.isFinite(p.v) && p.v > 0);
  const axisTicks = Array.from({ length: 9 }, (_, idx) => idx * 3);
  const actualPath = smoothSvgPath(pricePoints, x, y);
  const markerY = (event) => {
    const nearby = nearestPrice(rawPricePoints, event.m) || nearestPrice(pricePoints, event.m);
    if (nearby) return y(nearby.price);
    if (Number.isFinite(data.current) && data.current > 0) return y(data.current);
    return margin.top + plotH * 0.5;
  };
  const markerPath = (event) => {
    const px = x(event.m);
    const py = markerY(event);
    if (event.tone === 'positive') return `M ${px} ${py - 8} L ${px - 7} ${py + 7} L ${px + 7} ${py + 7} Z`;
    if (event.tone === 'negative') return `M ${px} ${py + 8} L ${px - 7} ${py - 7} L ${px + 7} ${py - 7} Z`;
    return `M ${px - 6} ${py - 6} L ${px + 6} ${py - 6} L ${px + 6} ${py + 6} L ${px - 6} ${py + 6} Z`;
  };

  return (
    <div className={`lab-chart-wrap orientation-${orientation}`}>
      <svg viewBox={`0 0 ${width} ${height}`} className="lab-chart" aria-label="24 month scenario price surface">
        <defs>
          <linearGradient id="labRibbonGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="rgba(54, 197, 138, 0.28)" />
            <stop offset="100%" stopColor="rgba(224, 89, 82, 0.18)" />
          </linearGradient>
        </defs>

        <rect x={margin.left} y={margin.top} width={plotW} height={plotH} className="lab-chart-bg" />
        {yTicks.map((t) => {
          const yCoord = y(t);
          return (
          <g key={`yt-${t}`}>
            <line x1={margin.left} y1={yCoord} x2={margin.left + plotW} y2={yCoord} className="lab-grid" />
            <text x={margin.left - 8} y={yCoord + 4} textAnchor="end" className="lab-axis-label">
              {fmtMoney(t, currency)}
            </text>
          </g>
          );
        })}

        {axisTicks.map((m) => (
          <g key={`xt-${m}`}>
            <line x1={x(m)} y1={margin.top} x2={x(m)} y2={margin.top + plotH} className="lab-grid-vert" />
            <text x={x(m)} y={margin.top + plotH + 16} textAnchor="middle" className="lab-axis-month-label">
              {monthAxisLabel(runStart, m)}
            </text>
            {horizonAxisLabel(m) && (
              <text x={x(m)} y={margin.top + plotH + 31} textAnchor="middle" className="lab-axis-label">
                {horizonAxisLabel(m)}
              </text>
            )}
          </g>
        ))}

        {yearSegments.map((segment) => (
          <text
            key={`yr-${segment.label}-${segment.center}`}
            x={x(segment.center)}
            y={margin.top + plotH + 48}
            textAnchor="middle"
            className="lab-axis-year-label"
          >
            {segment.label}
          </text>
        ))}

        <polygon points={ribbonPoints} className="lab-ribbon" />

        {pricePoints.length > 1 && (
          <path
            d={actualPath}
            fill="none"
            className="lab-actual-price-line"
          />
        )}

        {Object.entries(series).map(([key, points]) => (
          <g key={key}>
            <polyline
              points={points.map((p) => `${x(p.m)},${y(p.v)}`).join(' ')}
              fill="none"
              stroke={shades[key]}
              strokeWidth="2.7"
              strokeOpacity={key === 'base' ? 1 : 0.5}
            />
            {points.map((p) => (
              <g key={`${key}-${p.m}`}>
                <circle cx={x(p.m)} cy={y(p.v)} r="4.6" fill={shades[key]} />
                <text x={x(p.m)} y={y(p.v) - 8} textAnchor="middle" className="lab-point-label">
                  {fmtMoney(p.v, currency)}
                </text>
              </g>
            ))}
          </g>
        ))}

        {weightedPath.length > 0 && (
          <g>
            {weightedPath.length > 1 && (
              <polyline
                points={weightedPath.map((p) => `${x(p.m)},${y(p.v)}`).join(' ')}
                fill="none"
                stroke="#d8e9ff"
                strokeWidth="2.2"
                strokeDasharray="none"
                strokeOpacity="0.35"
              />
            )}
            {weightedPath.map((p) => (
              <g key={`weighted-${p.m}`}>
                <circle cx={x(p.m)} cy={y(p.v)} r="5.4" fill="#d8e9ff" />
                <circle cx={x(p.m)} cy={y(p.v)} r="2.4" fill="#0e2230" />
              </g>
            ))}
          </g>
        )}

        {currentPricePoint && (
          <g className="lab-current-price-marker">
            <circle cx={x(currentPricePoint.m)} cy={y(currentPricePoint.price)} r="6">
              <title>{`Today: ${fmtMoney(currentPricePoint.price, currency)} (latest source date ${currentPricePoint.date})`}</title>
            </circle>
          </g>
        )}

        {routeMarkers.length > 0 && (
          <g className="lab-router-markers">
            {routeMarkers.map((event, idx) => (
              <path
                key={`router-${event.date}-${event.title}-${idx}`}
                d={markerPath(event)}
                className={`tone-${event.tone}`}
              >
                <title>{`${event.date}: ${event.title}`}</title>
              </path>
            ))}
          </g>
        )}
      </svg>
      {(pricePoints.length > 0 || routeMarkers.length > 0) && (
        <div className="lab-chart-overlay-legend">
          {pricePoints.length > 0 && <span><i className="is-price" />Actual price history</span>}
          {currentPricePoint && <span><i className="is-current" />Today {fmtMoney(currentPricePoint.price, currency)}</span>}
          {routeMarkers.length > 0 && <span><i className="is-positive" />Positive catalyst</span>}
          {routeMarkers.length > 0 && <span><i className="is-negative" />Negative catalyst</span>}
        </div>
      )}
      <div className="timeline-impact-embedded" style={LANE_AXIS_STYLE}>
        <div className="timeline-lane">
          {(timelineBars || []).map((row, idx) => (
            <div key={`${row.milestone}-${idx}`} className="timeline-row">
              <div className="timeline-meta">
                <div className="timeline-name">{row.milestone}</div>
                <div className="timeline-sub">
                  {row.target_period || 'TBD'} · {statusLabel(row.status)}
                </div>
                {row.primary_risk && <div className="timeline-note">{row.primary_risk}</div>}
              </div>
              <div className="timeline-track">
                <svg
                  viewBox={`0 0 ${CHART_METRICS.width} 16`}
                  className="timeline-track-svg"
                  aria-label={`timeline track for ${row.milestone}`}
                >
                  <line
                    x1={chartX(0, domainStart, domainEnd)}
                    y1={8}
                    x2={chartX(24, domainStart, domainEnd)}
                    y2={8}
                    className="timeline-track-line"
                  />
                  <circle
                    cx={chartX(row.offset ?? 24, domainStart, domainEnd)}
                    cy={8}
                    r={6}
                    className={`timeline-track-dot tone-${statusTone(row.status)}`}
                  />
                </svg>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
