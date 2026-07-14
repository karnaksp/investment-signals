# T-Invest directional hypothesis study — 2026-07-14

Redacted aggregate generated locally from official T-Invest exchange one-minute
candles. No token, UID, account identifier, raw candle, price, or individual
event row was persisted.

## Scope

- Instruments: SBER, GAZP, LKOH, YDEX, T.
- Calendar range: 2026-05-30 through 2026-07-13.
- Trading sessions: 43 total; 30 train and 13 validation after chronological split.
- Completed regular-session candles: 109,853.
- Detected events: 1,041.
- Eligible detector minutes: 96,461.
- Script run ID: `e6b60b3e4fc8-d96fb416c90b`.

## Predeclared primary validation horizon

The primary validation horizon is 5 minutes. The validation split produced:

- eligible events: 314;
- decided outcomes: 310;
- outcome coverage: 98.7%;
- confirmed: 42 / 310, or 13.5%;
- contradicted: 35 / 310, or 11.3%;
- insignificant: 233 / 310, or 75.2%;
- mean net expected result: -8.80 bps;
- 95% day-bootstrap interval for mean net expected result: [-11.07, -6.27] bps;
- matched-control lift: +1.75 bps;
- 95% day-bootstrap interval for matched-control lift: [-0.50, +4.42] bps.

## Product decision

The validation set does not clear a continuation or inverse gate. The signal
family must remain experimental until an independent production cohort clears
the GA evidence gates.

This is still useful product evidence:

- automatic verdict labels are meaningful because the system can separate
  confirmed, contradicted, insignificant, and inconclusive outcomes;
- most detected events in this candle study were insignificant, so manual review
  alone would overstate value;
- inverse hypotheses must not be inferred from a single contradicted outcome or
  a weak aggregate result;
- buyer-facing claims need matched-control lift with a positive confidence
  bound, not just outcome coverage or a convenient chart.

## GA interpretation

This study validates automatic labels and evidence governance, not profitability.
A signal family can support a product claim only after production tick/L2
outcomes use actual half-spreads, exact detector/catalog/cost versions, at least
30 validation sessions, at least 300 eligible signals, and matched controls whose
95% confidence interval is above zero.

One-minute candles cannot reconstruct intraminute ordering, midpoint, spread,
order-book depth, latency, or fills. Any inverse result remains a pre-registered
shadow candidate and is never applied silently.

Method fingerprint:
`e6b60b3e4fc86c4f308181ebadc570953f0b06e84464aceab5a42dae00cd7cc2`

Input snapshot fingerprint:
`d96fb416c90be66a7056e563cfb0594466bc0b1cd56c248f3f1db17bd681edfc`
