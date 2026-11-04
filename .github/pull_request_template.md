## What changed
<!-- Models / macros / tests touched, and why -->

## Test evidence
- [ ] New or changed models have `unique`/`not_null` on the grain and a description
- [ ] Business rules are covered by a unit test (boundaries, nulls)
- [ ] Cross-model integrity covered (`relationships`) or reconciliation test added
- [ ] CI is green (lint + build + tests); warnings reviewed, not ignored
- [ ] Contracts updated if a mart's columns or types changed

## Data impact
<!-- Rows affected, backfill / full-refresh needed? PII touched? -->
