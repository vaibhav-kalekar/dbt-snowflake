# Data Test Strategy

Scope: the healthcare claims dbt Core project. Approach: risk-based, automated, code-reviewed, and run on every change.

## 1. Principles
1. **Test as code.** Every check is versioned SQL/YAML, reviewed in PRs, run by CI. No manual regression packs.
2. **Shift left.** Unit and contract tests fail in seconds on a PR, before anything reaches shared data.
3. **Fail loud, never drop silently.** Invalid rows are quarantined with a reason code and counted.
4. **Risk-based depth.** Marts that feed reporting get contracts, integrity and reconciliation; staging gets grain and null checks.

## 2. Test levels

| Level | What it proves | Tool | Runs |
|---|---|---|---|
| Static | SQL style, parse errors, broken refs | SQLFluff, `dbt parse` | PR |
| Unit | Transformation logic on mocked rows (boundaries, NULLs) | dbt unit tests | PR, deploy |
| Contract | Column names and types match the agreed schema | `contract: enforced` | Build |
| Data quality | Uniqueness (grain), not-null, accepted values, ranges, regex, referential integrity | generic tests, `dbt_utils`, `dbt_expectations` | PR, deploy |
| Reconciliation | Control totals between layers; parity with the legacy system | singular test, `audit_helper` | PR, deploy, migration parallel run |
| Freshness | Sources are current before transforming | `dbt source freshness` | Scheduled |
| Regression | Changes don't break downstream | Slim CI (`state:modified+`) | PR |

## 3. Severity policy
- **error** - integrity or structure is broken (duplicate keys, orphan foreign keys, contract mismatch, reconciliation difference). Blocks merge/deploy.
- **warn** - content problems needing business triage (e.g. missing claim amount). Does not block; surfaced in the run summary and quantified in `claims_rejected`.
- Warnings are reviewed weekly; a warning that never gets fixed is either promoted to error or removed.

## 4. Entry / exit criteria
- **Entry to PR review:** lint clean, `dbt parse` passes, new models have grain tests and descriptions.
- **Exit to prod:** all errors zero; unit tests 100% pass; reconciliation control totals match; warnings reviewed.
- **Rollback:** revert the merge and redeploy (models are rebuilt from Git); for data, re-run with `--full-refresh` or restore with Snowflake Time Travel.

## 5. Test data
- Synthetic seeds with deliberate edge cases (boundary amounts, NULL amount). No production PII in dev/CI.
- PII columns are tagged in `meta` (`contains_pii: true`) to drive masking policies.
- CI isolates each PR in its own schema and drops it afterwards.

## 6. Metrics reported (from `run_results.json`)
- Test pass rate by type (unit / generic / singular)
- Warning count and trend; rejected-row rate (`claims_rejected` / total)
- Source freshness vs SLA
- CI duration and flaky-test count (a test that flips without a code change is a defect in the test)

## 7. Defect handling
1. Failing test -> classify: **data defect** (source), **logic defect** (model), or **test defect** (bad assertion).
2. Data defects go to the source owner with failing-row evidence (`store_failures` can persist them).
3. Every production escape adds a regression test before the fix is merged.

## 8. Legacy migration validation
Parallel run: load the legacy extract, compare with `audit_helper.compare_queries` on the business key, investigate
every mismatch category (missing, extra, value difference), agree tolerances with the business, and sign off per domain.
