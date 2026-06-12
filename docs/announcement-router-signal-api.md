# Announcement Router Signal API

This is the minimal machine contract for Trading Terminal / Alpha Edge.

The announcement router remains the source of truth for thesis-evidence scoring. Trading Terminal should not ingest router event rows, replay the router, or reimplement thesis matching. It should request the current validated signal by ticker and use that as one input into its existing allocation and momentum logic.

## Endpoint

```text
GET /api/announcement-router/signals
GET /api/announcement-router/signals?ticker=ASX:VMM
```

Legacy alias:

```text
GET /api/scenario-router/signals
```

Singular aliases are also available:

```text
GET /api/announcement-router/signal?ticker=ASX:VMM
GET /api/scenario-router/signal?ticker=ASX:VMM
```

## Response

The response is deliberately just a ticker-to-score map:

```json
{
  "ASX:VMM": 3,
  "ASX:WWI": -1.5,
  "ASX:BRK": 0
}
```

When a specific ticker is requested and the router has no stored event for it, the endpoint returns zero:

```json
{
  "ASX:XYZ": 0
}
```

## Score Definition

The exported score is the latest `cumulative_validated_delta` for that ticker from the announcement router event store.

That means:

- positive values indicate validated thesis evidence has moved in a favourable direction
- negative values indicate validated thesis evidence has moved in an unfavourable direction
- zero means no validated router movement is available

Unmapped or unvalidated directional pressure is not exported through this endpoint. It remains available in the full router events endpoint for human inspection, but it should not silently change Trading Terminal sizing or allocation.

## Trading Terminal Usage

Trading Terminal should treat this score as a small conviction input, not as a standalone buy/sell decision.

Recommended first use:

```text
router_score = signals[ticker] or 0
```

Then use `router_score` only inside existing allocation, ranking, or breakout-sensitivity logic.

Do not use this endpoint for:

- filing titles
- run IDs
- confidence breakdowns
- source links
- event-level audit trails
- explanation UI

For those, call:

```text
GET /api/announcement-router/events?ticker=ASX:VMM
```

## Ownership Boundary

LLM Council owns:

- announcement parsing
- thesis/watchlist/verification matching
- router trajectory scoring
- persisted router event artifacts
- this compact signal endpoint

Trading Terminal owns:

- portfolio allocation logic
- momentum gates
- breakout rules
- watchlist and position UX
- any decision to use, cap, ignore, or display the router score

