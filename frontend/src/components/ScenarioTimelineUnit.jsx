const CHART_METRICS = {
  width: 900,
  height: 420,
  margin: { top: 12, right: 30, bottom: 44, left: 62 },
};

const chartX = (months) => {
  const { width, margin } = CHART_METRICS;
  const plotW = width - margin.left - margin.right;
  return margin.left + (months / 24) * plotW;
};

const LANE_AXIS_STYLE = {
  '--lane-axis-left': `${(CHART_METRICS.margin.left / CHART_METRICS.width) * 100}%`,
  '--lane-axis-right': `${(CHART_METRICS.margin.right / CHART_METRICS.width) * 100}%`,
};

function fmtMoney(value, currency = 'AUD') {
  const n = Number(value);
  if (!Number.isFinite(n)) return 'n/a';
  const symbol = currency === 'AUD' ? 'A$' : '$';
  return `${symbol}${n.toFixed(2)}`;
}

function statusTone(status) {
  const s = String(status || '').toLowerCase();
  if (s.includes('met') || s.includes('likely') || s.includes('planned') || s.includes('imminent')) return 'bull';
  if (s.includes('risk')) return 'bear';
  return 'base';
}

export default function ScenarioTimelineUnit({ data, currency, timelineBars, orientation = 'vertical' }) {
  const { width, height, margin } = CHART_METRICS;
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;

  const x = (m) => margin.left + (m / 24) * plotW;
  const now = new Date();
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

        {[0, 6, 12, 18, 24].map((m) => (
          <g key={`xt-${m}`}>
            <line x1={x(m)} y1={margin.top} x2={x(m)} y2={margin.top + plotH} className="lab-grid-vert" />
            <text x={x(m)} y={margin.top + plotH + 18} textAnchor="middle" className="lab-axis-label">
              {m === 0 ? 'Now' : `${m}M`}
            </text>
          </g>
        ))}

        {yearSegments.map((segment) => (
          <text
            key={`yr-${segment.label}-${segment.center}`}
            x={x(segment.center)}
            y={margin.top + plotH + 34}
            textAnchor="middle"
            className="lab-axis-year-label"
          >
            {segment.label}
          </text>
        ))}

        <polygon points={ribbonPoints} className="lab-ribbon" />

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
      </svg>
      <div className="timeline-impact-embedded" style={LANE_AXIS_STYLE}>
        <div className="timeline-lane">
          {(timelineBars || []).map((row, idx) => (
            <div key={`${row.milestone}-${idx}`} className="timeline-row">
              <div className="timeline-meta">
                <div className="timeline-name">{row.milestone}</div>
                <div className="timeline-sub">
                  {row.target_period || 'TBD'} · {row.status || 'n/a'}
                </div>
              </div>
              <div className="timeline-track">
                <svg
                  viewBox={`0 0 ${CHART_METRICS.width} 16`}
                  className="timeline-track-svg"
                  aria-label={`timeline track for ${row.milestone}`}
                >
                  <line
                    x1={chartX(0)}
                    y1={8}
                    x2={chartX(24)}
                    y2={8}
                    className="timeline-track-line"
                  />
                  <circle
                    cx={chartX(row.offset ?? 24)}
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
