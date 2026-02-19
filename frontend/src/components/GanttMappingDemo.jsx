import { useMemo } from 'react';
import { ausgoldStage3Sample } from '../data/ausgoldStage3Sample';
import { mapStage3ToGanttModel } from '../utils/stage3GanttMapper';
import './GanttMappingDemo.css';

function impactClass(impact) {
  const value = String(impact || 'MED').toUpperCase();
  if (value === 'HIGH') return 'high';
  if (value === 'LOW') return 'low';
  return 'med';
}

function impactLabel(impact) {
  const value = String(impact || 'MED').toUpperCase();
  if (value === 'HIGH') return 'HIGH';
  if (value === 'LOW') return 'LOW';
  return 'MED';
}

function formatCatalystPeriod(catalyst) {
  if (catalyst?.periodLabel) return catalyst.periodLabel;
  if (!catalyst?.date) return 'TBD';
  const date = new Date(catalyst.date);
  if (Number.isNaN(date.getTime())) return 'TBD';
  return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
}

function monthDiff(fromDate, dateString) {
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return null;
  const ms = date.getTime() - fromDate.getTime();
  const approxMonthMs = 1000 * 60 * 60 * 24 * 30.4375;
  return ms / approxMonthMs;
}

function shortDriver(text) {
  if (!text) return '';
  const clean = String(text).trim();
  if (clean.length <= 68) return clean;
  return `${clean.slice(0, 67)}...`;
}

export default function GanttMappingDemo() {
  const mapped = useMemo(() => mapStage3ToGanttModel(ausgoldStage3Sample), []);
  // Demo override: simulate "today" for gantt-style progress tracking.
  // In the larger app, replace with a global date source.
  const simulatedToday = useMemo(() => new Date(2026, 2, 15), []);
  const now = simulatedToday;
  const axisStart = useMemo(
    () => new Date(now.getFullYear(), 0, 1),
    [now]
  );
  const endDate = useMemo(() => {
    const date = new Date(axisStart);
    date.setMonth(date.getMonth() + 24);
    return date;
  }, [axisStart]);

  const futureCatalysts = mapped.catalysts.filter((c) => {
    if (!c.date) return true;
    const d = new Date(c.date);
    return d > now && d <= endDate;
  });

  const chartData = useMemo(() => {
    const priceTargets = mapped.raw?.price_targets || {};
    const currentPrice = Number(priceTargets.current_price) || Number(mapped.scenario.baseCasePT) || 0;
    const targets12 = priceTargets?.scenario_targets?.['12m'] || {};
    const targets24 = priceTargets?.scenario_targets?.['24m'] || {};

    const series = {
      bear: [
        { month: 0, price: currentPrice },
        { month: 12, price: Number(targets12.bear) || Number(priceTargets?.scenarios?.bear) || 0 },
        { month: 24, price: Number(targets24.bear) || Number(targets12.bear) || 0 },
      ],
      base: [
        { month: 0, price: currentPrice },
        { month: 12, price: Number(targets12.base) || Number(priceTargets.target_12m) || 0 },
        { month: 24, price: Number(targets24.base) || Number(priceTargets.target_24m) || 0 },
      ],
      bull: [
        { month: 0, price: currentPrice },
        { month: 12, price: Number(targets12.bull) || Number(priceTargets?.scenarios?.bull) || 0 },
        { month: 24, price: Number(targets24.bull) || Number(targets12.bull) || 0 },
      ],
    };

    const values = Object.values(series).flat().map((p) => p.price).filter((v) => Number.isFinite(v) && v > 0);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const pad = Math.max((max - min) * 0.16, max * 0.08, 0.1);
    const yMin = Math.max(min - pad, 0);
    const yMax = max + pad;

    const driverMap = priceTargets?.scenario_drivers || {};
    const driverTags = [];
    for (const horizon of ['12m', '24m']) {
      const month = horizon === '12m' ? 12 : 24;
      for (const scenario of ['bear', 'base', 'bull']) {
        const drivers = Array.isArray(driverMap?.[horizon]?.[scenario]) ? driverMap[horizon][scenario] : [];
        if (!drivers.length) continue;
        const target = Number(priceTargets?.scenario_targets?.[horizon]?.[scenario]);
        driverTags.push({
          id: `${horizon}-${scenario}`,
          horizon,
          scenario,
          month,
          target: Number.isFinite(target) ? target : null,
          drivers,
        });
      }
    }

    const nonDriverCatalysts = futureCatalysts
      .filter((c) => c.source !== 'price_targets.scenario_drivers')
      .map((c) => ({
        ...c,
        month: monthDiff(axisStart, c.date),
      }))
      .filter((c) => Number.isFinite(c.month) && c.month >= 0 && c.month <= 24)
      .slice(0, 10);

    return {
      currentPrice,
      yMin,
      yMax,
      series,
      driverTags,
      nonDriverCatalysts,
    };
  }, [mapped, futureCatalysts, axisStart]);

  const chartLayout = {
    width: 920,
    height: 390,
    margin: { top: 20, right: 42, bottom: 96, left: 70 },
  };
  const plotWidth = chartLayout.width - chartLayout.margin.left - chartLayout.margin.right;
  const plotHeight = chartLayout.height - chartLayout.margin.top - chartLayout.margin.bottom;
  const xScale = (month) => chartLayout.margin.left + (month / 24) * plotWidth;
  const yScale = (price) =>
    chartLayout.margin.top +
    ((chartData.yMax - price) / Math.max(chartData.yMax - chartData.yMin, 0.0001)) * plotHeight;

  const yTicks = Array.from({ length: 6 }, (_, i) => {
    const value = chartData.yMin + ((chartData.yMax - chartData.yMin) * i) / 5;
    return Number(value.toFixed(2));
  });
  const quarterBoundaries = [0, 3, 6, 9, 12, 15, 18, 21, 24];
  const quarterSegments = useMemo(
    () =>
      Array.from({ length: 8 }, (_, idx) => ({
        start: idx * 3,
        end: (idx + 1) * 3,
        center: idx * 3 + 1.5,
        label: `Q${(idx % 4) + 1}`,
      })),
    []
  );
  const halfSegments = useMemo(
    () =>
      Array.from({ length: 4 }, (_, idx) => ({
        start: idx * 6,
        end: (idx + 1) * 6,
        center: idx * 6 + 3,
        label: idx % 2 === 0 ? 'H1' : 'H2',
      })),
    []
  );
  const yearSegments = useMemo(
    () =>
      Array.from({ length: 2 }, (_, idx) => ({
        start: idx * 12,
        end: (idx + 1) * 12,
        center: idx * 12 + 6,
        label: String(axisStart.getFullYear() + idx),
      })),
    [axisStart]
  );

  const scenarioStyle = {
    bear: { color: '#ef4444', label: 'Bear' },
    base: { color: '#f59e0b', label: 'Base' },
    bull: { color: '#10b981', label: 'Bull' },
  };
  const todayMonth = Math.min(24, Math.max(0, monthDiff(axisStart, now)));
  const todayX = xScale(todayMonth);

  return (
    <div className="gantt-demo-root">
      <div className="gantt-demo-header">
        <div>
          <div className="gantt-demo-title">
            Gantt Mapping Demo: {mapped.companyName} ({mapped.ticker})
          </div>
          <div className="gantt-demo-subtitle">
            Route: <code>/gantt-demo</code> | Source: Stage 3 structured JSON (Ausgold sample)
          </div>
        </div>
        <div className="gantt-demo-nav">
          <a href="/">Back To Council Chat</a>
        </div>
      </div>

      <div className="gantt-thesis-row">
        <label className="gantt-label">INVESTMENT THESIS</label>
        <div className="gantt-thesis-text">{mapped.thesis || 'No thesis available'}</div>
      </div>

      <div className="gantt-layout">
        <div className="gantt-card">
          <label className="gantt-label">SCENARIO ANALYSIS (12M)</label>
          <table className="scenario-table">
            <thead>
              <tr>
                <th></th>
                <th>Bear</th>
                <th>Base</th>
                <th>Bull</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>PT</td>
                <td><span className="pill pill-bear">{mapped.scenario.bearCasePT.toFixed(2)}</span></td>
                <td><span className="pill pill-base">{mapped.scenario.baseCasePT.toFixed(2)}</span></td>
                <td><span className="pill pill-bull">{mapped.scenario.bullCasePT.toFixed(2)}</span></td>
              </tr>
              <tr>
                <td>Prob%</td>
                <td>{mapped.scenario.bearProbability}</td>
                <td>{mapped.scenario.baseProbability}</td>
                <td>{mapped.scenario.bullProbability}</td>
              </tr>
            </tbody>
          </table>
          <div className="weighted-row">
            <span>Weighted PT</span>
            <strong>{mapped.scenario.weightedPT.toFixed(2)}</strong>
          </div>
        </div>

        <div className="gantt-card">
          <div className="catalyst-header-row">
            <label className="gantt-label">IMPORTANCE | KEY CATALYSTS</label>
          </div>
          <div className="catalyst-list">
            {futureCatalysts.map((c, idx) => (
              <div className="catalyst-item" key={`${c.name}-${c.date}-${idx}`}>
                <div className="catalyst-top">
                  <span className={`impact-badge impact-${impactClass(c.impact)}`}>
                    {impactLabel(c.impact)}
                  </span>
                  <div className="catalyst-name">{c.name}</div>
                  <div className="catalyst-date">{formatCatalystPeriod(c)}</div>
                </div>
                <div className="catalyst-source">{c.source}</div>
              </div>
            ))}
            {!futureCatalysts.length && (
              <div className="catalyst-item">No catalysts in 24m window.</div>
            )}
          </div>
        </div>

        <div className="gantt-card">
          <label className="gantt-label">PRICE PATH (24M)</label>
          <div className="price-chart-box">
            <svg
              className="price-chart"
              viewBox={`0 0 ${chartLayout.width} ${chartLayout.height}`}
              role="img"
              aria-label="24 month price path with scenario targets and catalysts"
            >
              <line
                x1={chartLayout.margin.left}
                y1={chartLayout.margin.top}
                x2={chartLayout.margin.left}
                y2={chartLayout.margin.top + plotHeight}
                className="axis-line"
              />
              <line
                x1={chartLayout.margin.left}
                y1={chartLayout.margin.top + plotHeight}
                x2={chartLayout.margin.left + plotWidth}
                y2={chartLayout.margin.top + plotHeight}
                className="axis-line"
              />

              <line
                x1={todayX}
                y1={chartLayout.margin.top}
                x2={todayX}
                y2={chartLayout.margin.top + plotHeight}
                className="today-line"
              />
              <text
                x={todayX + 4}
                y={chartLayout.margin.top + 12}
                className="today-label"
              >
                TODAY
              </text>

              {yTicks.map((tick) => {
                const y = yScale(tick);
                return (
                  <g key={`y-${tick}`}>
                    <line
                      x1={chartLayout.margin.left}
                      y1={y}
                      x2={chartLayout.margin.left + plotWidth}
                      y2={y}
                      className="grid-line-price"
                    />
                    <text x={chartLayout.margin.left - 8} y={y + 4} textAnchor="end" className="axis-label">
                      {tick.toFixed(2)}
                    </text>
                  </g>
                );
              })}

              {quarterBoundaries.map((tick) => {
                const x = xScale(tick);
                return (
                  <g key={`x-${tick}`}>
                    <line
                      x1={x}
                      y1={chartLayout.margin.top}
                      x2={x}
                      y2={chartLayout.margin.top + plotHeight}
                      className="grid-line-price"
                    />
                  </g>
                );
              })}

              {quarterSegments.map((segment, idx) => (
                <text
                  key={`qseg-${idx}`}
                  x={xScale(segment.center)}
                  y={chartLayout.margin.top + plotHeight + 18}
                  textAnchor="middle"
                  className="axis-quarter-label"
                >
                  {segment.label}
                </text>
              ))}

              {halfSegments.map((segment, idx) => (
                <text
                  key={`hseg-${idx}`}
                  x={xScale(segment.center)}
                  y={chartLayout.margin.top + plotHeight + 34}
                  textAnchor="middle"
                  className="axis-half-label"
                >
                  {segment.label}
                </text>
              ))}

              {yearSegments.map((segment, idx) => (
                <text
                  key={`yseg-${idx}`}
                  x={xScale(segment.center)}
                  y={chartLayout.margin.top + plotHeight + 50}
                  textAnchor="middle"
                  className="axis-year-label"
                >
                  {segment.label}
                </text>
              ))}

              {[12, 24].map((month) => (
                <line
                  key={`horizon-line-${month}`}
                  x1={xScale(month)}
                  y1={chartLayout.margin.top + plotHeight + 52}
                  x2={xScale(month)}
                  y2={chartLayout.margin.top + plotHeight}
                  className="horizon-anchor-line"
                />
              ))}

              {[12, 24].map((month) => (
                <text
                  key={`horizon-${month}`}
                  x={xScale(month)}
                  y={chartLayout.margin.top + plotHeight + 66}
                  textAnchor="middle"
                  className="axis-horizon-label"
                >
                  {month}M
                </text>
              ))}

              {chartData.nonDriverCatalysts.map((cat, idx) => {
                const x = xScale(cat.month);
                const yTop = chartLayout.margin.top + 2;
                const yBottom = chartLayout.margin.top + plotHeight - 2;
                const label = shortDriver(cat.name);
                return (
                  <g key={`cat-${idx}`}>
                    <line
                      x1={x}
                      y1={yTop}
                      x2={x}
                      y2={yBottom}
                      className="event-marker-line"
                    />
                    <text
                      x={x + 3}
                      y={yTop + 12 + idx * 11}
                      className="event-marker-label"
                    >
                      {label}
                    </text>
                  </g>
                );
              })}

              {Object.keys(scenarioStyle).map((scenario) => {
                const points = chartData.series[scenario]
                  .map((p) => `${xScale(p.month)},${yScale(p.price)}`)
                  .join(' ');
                const style = scenarioStyle[scenario];
                return (
                  <g key={scenario}>
                    <polyline points={points} fill="none" stroke={style.color} strokeWidth="2.6" />
                    {chartData.series[scenario].map((p, idx) => (
                      <g key={`${scenario}-${idx}`}>
                        <circle cx={xScale(p.month)} cy={yScale(p.price)} r="4.2" fill={style.color} />
                        <text
                          x={xScale(p.month)}
                          y={yScale(p.price) - 8}
                          textAnchor="middle"
                          className="price-point-label"
                        >
                          {p.price.toFixed(2)}
                        </text>
                      </g>
                    ))}
                  </g>
                );
              })}

              {chartData.driverTags.map((tag) => {
                const x = xScale(tag.month);
                const y = yScale(Number(tag.target) || chartData.currentPrice);
                const xLabel = tag.month >= 20 ? x - 210 : x + 10;
                const baseOffsets = { bear: 18, base: -4, bull: -26 };
                const yLabel = y + (baseOffsets[tag.scenario] || 0);
                return (
                  <g key={tag.id}>
                    <text x={xLabel} y={yLabel + 4} className="driver-tag-text">
                      {shortDriver(tag.drivers[0])}
                    </text>
                  </g>
                );
              })}
            </svg>

            <div className="scenario-legend">
              {Object.entries(scenarioStyle).map(([key, value]) => (
                <div className="legend-item" key={key}>
                  <span className="legend-swatch" style={{ backgroundColor: value.color }} />
                  <span>{value.label}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="mapping-json">
        <label className="gantt-label">SCENARIO DRIVER TAGS (FOR GANTT CONDITIONS)</label>
        <pre>{JSON.stringify(mapped.raw?.price_targets?.scenario_drivers || {}, null, 2)}</pre>
      </div>

      <div className="mapping-json">
        <label className="gantt-label">MAPPING OUTPUT JSON (FOR FRONTEND WIRING)</label>
        <pre>{JSON.stringify(mapped, null, 2)}</pre>
      </div>
    </div>
  );
}
