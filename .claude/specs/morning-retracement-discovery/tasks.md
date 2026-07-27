# Morning retracement discovery tasks

### T-1: Domain path model

- **Status**: completed
- **Wired**: yes
- **Verified**: yes
- **Requirements**: US-1, US-3
- **Acceptance**: deterministic levels, first passage, conservative stop, and
  break-even simulation have unit tests.

### T-2: Causal dataset builder

- **Status**: completed
- **Wired**: yes
- **Verified**: yes
- **Requirements**: US-1, US-2
- **Dependencies**: T-1
- **Acceptance**: morning and previous-session features have sealed cutoffs and
  chronological episode identifiers.

### T-3: Model and policy research runner

- **Status**: completed
- **Wired**: yes
- **Verified**: yes
- **Requirements**: US-3, US-4
- **Dependencies**: T-2
- **Acceptance**: one command reuses candle cache and writes dataset, model
  comparison, policy frontier, selected policy, and Russian report.

### T-4: Verification and evidence run

- **Status**: completed
- **Wired**: yes
- **Verified**: yes
- **Requirements**: US-1, US-2, US-3, US-4
- **Dependencies**: T-3
- **Acceptance**: architecture gate, focused tests, full tests, and one local
  cached-data run complete; results are reported without a product claim.
