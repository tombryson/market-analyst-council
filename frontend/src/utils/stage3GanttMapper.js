function clampNumber(value, min, max) {
  const num = Number(value);
  if (!Number.isFinite(num)) return min;
  return Math.min(max, Math.max(min, num));
}

function normalizePeriodLabel(label) {
  if (!label || typeof label !== 'string') return null;
  const text = label.trim().replace(/\s+/g, ' ');
  if (!text) return null;
  return text;
}

function parseTargetPeriod(targetPeriod) {
  if (!targetPeriod || typeof targetPeriod !== 'string') return { date: null, periodLabel: null, explicitMonth: false };
  const text = targetPeriod.trim();
  const normalizedLabel = normalizePeriodLabel(text);
  const monthMap = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11,
  };

  let match = text.match(/\bQ([1-4])\s*[-/]?\s*(\d{4})\b/i);
  if (match) {
    const quarter = Number(match[1]);
    const year = Number(match[2]);
    const month = quarter * 3 - 1;
    return {
      date: new Date(year, month, 1).toISOString().slice(0, 10),
      periodLabel: `Q${quarter} ${year}`,
      explicitMonth: false,
    };
  }

  match = text.match(/\bH([12])\s*[-/]?\s*(\d{4})\b/i);
  if (match) {
    const half = Number(match[1]);
    const year = Number(match[2]);
    const month = half === 1 ? 5 : 11;
    return {
      date: new Date(year, month, 1).toISOString().slice(0, 10),
      periodLabel: `H${half} ${year}`,
      explicitMonth: false,
    };
  }

  match = text.match(/\bmid[-\s]?(\d{4})\b/i);
  if (match) {
    const year = Number(match[1]);
    return {
      date: new Date(year, 6, 1).toISOString().slice(0, 10),
      periodLabel: `Mid-${year}`,
      explicitMonth: false,
    };
  }

  match = text.match(/\b(\d{4})\b/);
  if (match && !/[a-z]/i.test(text.replace(match[1], ''))) {
    const year = Number(match[1]);
    return {
      date: new Date(year, 6, 1).toISOString().slice(0, 10),
      periodLabel: `${year}`,
      explicitMonth: false,
    };
  }

  // "February 2027", "Feb 2027", etc.
  match = text.match(/\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b[\s,\-/]*(\d{4})\b/i);
  if (match) {
    const monthKey = match[1].toLowerCase();
    const month = monthMap[monthKey];
    const year = Number(match[2]);
    if (Number.isFinite(month) && Number.isFinite(year)) {
      const canonicalMonth = match[1].slice(0, 1).toUpperCase() + match[1].slice(1).toLowerCase();
      return {
        date: new Date(year, month, 1).toISOString().slice(0, 10),
        periodLabel: `${canonicalMonth} ${year}`,
        explicitMonth: true,
      };
    }
  }

  if (!/\d{4}/.test(text)) {
    return { date: null, periodLabel: null, explicitMonth: false };
  }

  if (/^\d{4}$/.test(text)) {
    const year = Number(text);
    return {
      date: new Date(year, 6, 1).toISOString().slice(0, 10),
      periodLabel: `${year}`,
      explicitMonth: false,
    };
  }

  return { date: null, periodLabel: normalizedLabel, explicitMonth: false };
}

function addMonthsDateIso(baseDate, months) {
  const date = new Date(baseDate);
  date.setMonth(date.getMonth() + months);
  return date.toISOString().slice(0, 10);
}

function milestoneImpact(status, confidencePct) {
  const statusText = String(status || '').toLowerCase();
  const confidence = clampNumber(confidencePct ?? 50, 0, 100);

  if (statusText.includes('risk')) return 'HIGH';
  if (confidence >= 70) return 'HIGH';
  if (confidence >= 45) return 'MED';
  return 'LOW';
}

function toCurrencyLabel(value, currency = 'AUD') {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  const symbol = currency === 'AUD' ? 'A$' : '$';
  return `${symbol}${num.toFixed(2)}`;
}

function normalizeScenarioProbabilities(rawProbabilities) {
  const raw = rawProbabilities && typeof rawProbabilities === 'object' ? rawProbabilities : {};
  const bear = Number(raw.bear);
  const base = Number(raw.base);
  const bull = Number(raw.bull);
  if ([bear, base, bull].every((v) => Number.isFinite(v))) {
    const scale = Math.max(bear, base, bull) <= 1 ? 100 : 1;
    return {
      bear: Math.max(0, bear * scale),
      base: Math.max(0, base * scale),
      bull: Math.max(0, bull * scale),
    };
  }
  return { bear: 25, base: 50, bull: 25 };
}

function pushUniqueCatalyst(list, catalyst) {
  if (!catalyst?.name) return;
  const periodKey = catalyst.periodLabel || catalyst.date || 'unspecified';
  const key = `${catalyst.name}::${periodKey}`;
  const exists = list.some((item) => {
    const itemPeriodKey = item.periodLabel || item.date || 'unspecified';
    return `${item.name}::${itemPeriodKey}` === key;
  });
  if (!exists) list.push(catalyst);
}

export function mapStage3ToGanttModel(structuredData) {
  const data = structuredData || {};
  const priceTargets = data.price_targets || {};
  const scenarios = priceTargets.scenarios || {};
  const scenarioTargets = priceTargets.scenario_targets || {};
  const scenarioProbabilities = priceTargets.scenario_probabilities || {};
  const scenarioDrivers = priceTargets.scenario_drivers || {};
  const timeline = Array.isArray(data.development_timeline) ? data.development_timeline : [];
  const nextCatalysts = (data.extended_analysis || {}).next_major_catalysts || [];
  const currency = (data.market_data_provenance || {}).prepass_currency || 'AUD';
  const now = new Date();

  const twelveMonthTargets = scenarioTargets['12m'] || scenarios || {};
  const twentyFourMonthTargets = scenarioTargets['24m'] || {};

  const probabilities = normalizeScenarioProbabilities(scenarioProbabilities['12m']);
  const bearPT = Number(twelveMonthTargets.bear) || 0;
  const basePT = Number(twelveMonthTargets.base) || Number(priceTargets.target_12m) || 0;
  const bullPT = Number(twelveMonthTargets.bull) || 0;

  const totalProb = probabilities.bear + probabilities.base + probabilities.bull;
  const weightedPT = totalProb > 0
    ? ((bearPT * probabilities.bear) + (basePT * probabilities.base) + (bullPT * probabilities.bull)) / totalProb
    : 0;

  const catalysts = [];

  for (const item of timeline) {
    const parsedTargetPeriod = parseTargetPeriod(item.target_period);
    pushUniqueCatalyst(catalysts, {
      name: item.milestone || 'Milestone',
      date: parsedTargetPeriod.date,
      periodLabel: parsedTargetPeriod.periodLabel,
      impact: milestoneImpact(item.status, item.confidence_pct),
      source: 'development_timeline',
      status: item.status || '',
      confidencePct: item.confidence_pct ?? null,
    });
  }

  for (const horizon of ['12m', '24m']) {
    const horizonMonths = horizon === '12m' ? 12 : 24;
    const horizonDate = addMonthsDateIso(now, horizonMonths);
    const targets = horizon === '12m' ? twelveMonthTargets : twentyFourMonthTargets;
    const driversByScenario = scenarioDrivers[horizon] || {};

    for (const scenario of ['bear', 'base', 'bull']) {
      const drivers = Array.isArray(driversByScenario[scenario]) ? driversByScenario[scenario] : [];
      if (!drivers.length) continue;

      const target = Number(targets[scenario]);
      const targetLabel = toCurrencyLabel(target, currency);
      const title = `${horizon.toUpperCase()} ${scenario.toUpperCase()}${targetLabel ? ` (${targetLabel})` : ''}`;
      const impact = scenario === 'base' ? 'MED' : 'HIGH';

      pushUniqueCatalyst(catalysts, {
        name: `${title}: ${drivers[0]}`,
        date: horizonDate,
        periodLabel: horizon.toUpperCase(),
        impact,
        source: 'price_targets.scenario_drivers',
        horizon,
        scenario,
        drivers,
        target: Number.isFinite(target) ? target : null,
      });
    }
  }

  nextCatalysts.slice(0, 6).forEach((name) => {
    const parsedFromName = parseTargetPeriod(name);
    pushUniqueCatalyst(catalysts, {
      name: String(name),
      date: parsedFromName.date,
      periodLabel: parsedFromName.date ? parsedFromName.periodLabel : 'TBD',
      impact: 'MED',
      source: 'extended_analysis.next_major_catalysts',
    });
  });

  catalysts.sort((a, b) => {
    if (!a.date && !b.date) return 0;
    if (!a.date) return 1;
    if (!b.date) return -1;
    const ad = new Date(a.date).getTime();
    const bd = new Date(b.date).getTime();
    return ad - bd;
  });

  // Keep at most one historical catalyst as context; preserve forward focus.
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const indexed = catalysts.map((item, idx) => ({ idx, item }));
  const pastRows = indexed.filter(({ item }) => item?.date && new Date(item.date) < today);
  if (pastRows.length > 1) {
    const keepPastIdx = new Set(
      [...pastRows]
        .sort((a, b) => new Date(b.item.date).getTime() - new Date(a.item.date).getTime())
        .slice(0, 1)
        .map((row) => row.idx)
    );
    const filtered = indexed
      .filter(({ idx, item }) => !(item?.date && new Date(item.date) < today) || keepPastIdx.has(idx))
      .map(({ item }) => item);
    catalysts.length = 0;
    catalysts.push(...filtered);
  }

  const topReasons = (data.investment_verdict || {}).top_reasons || [];
  const headlineThesis = (data.investment_recommendation || {}).summary || topReasons[0] || '';

  return {
    companyName: data.company_name || data.company || 'Unknown Company',
    ticker: data.ticker || '',
    thesis: headlineThesis,
    scenario: {
      bearCasePT: bearPT,
      baseCasePT: basePT,
      bullCasePT: bullPT,
      bearProbability: probabilities.bear,
      baseProbability: probabilities.base,
      bullProbability: probabilities.bull,
      weightedPT,
      target24mBase: Number(twentyFourMonthTargets.base) || Number(priceTargets.target_24m) || null,
    },
    catalysts,
    raw: data,
  };
}
