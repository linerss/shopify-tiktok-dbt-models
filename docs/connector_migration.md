# Store-by-store connector migration

## Why migrate in the intermediate layer

A connector replacement should not force downstream marts to understand two physical schemas. Keep each connector in its own staging models, adapt both to the same explicit column contract, and union them in the intermediate layer.

The legacy staging branch can remain append-only while the replacement branch reflects the new connector's current-state behavior. Downstream models continue to consume one stable relation.

## Cutover sequence

1. Load the legacy and replacement connectors in parallel.
2. Build replacement staging adapters without changing production references.
3. Compare row counts, natural-key coverage, null rates, sums, minimum and maximum timestamps, and representative records.
4. Add one store to `shopify_migrated_stores`.
5. Rebuild and validate intermediate and downstream models for that store.
6. Monitor freshness and reconciliation results after cutover.
7. Repeat for the next store.
8. Remove the legacy branch only after every store has migrated and the rollback window has closed.

## Union pattern

```sql
{% set migrated_stores = var('shopify_migrated_stores', []) %}

select <EXPLICIT_COLUMNS>
from {{ ref('legacy_staging_model') }}
{% if migrated_stores | length > 0 %}
where store_id not in (<MIGRATED_STORES>)
{% endif %}

union all

select <EXPLICIT_COLUMNS_WITH_CASTS>
from {{ ref('replacement_staging_model') }}
{% if migrated_stores | length > 0 %}
where store_id in (<MIGRATED_STORES>)
{% else %}
where 1 = 0
{% endif %}
```

The empty-list branches are deliberate: no migrated stores means all legacy rows and zero replacement rows, without rendering invalid `IN ()` SQL. Never use `select *` in this union. Explicit columns expose schema drift and prevent a newly arrived PII field from flowing downstream unnoticed.

## Date placeholder

Use `<CUTOVER_DATE>` only when a time-bound normalization rule is unavoidable. It marks the date when a store or connector began emitting the corrected representation. Prefer normalization that works across the full history so the date can be removed entirely.

## Validation checklist per store

- Both connectors cover the expected business-date range.
- Natural keys include `store_id`.
- Numeric identifiers have no decimal or scientific-notation representation.
- Current-state intermediate models have no duplicate natural keys.
- Order, shipping, refund, statement, and transaction totals reconcile by date and currency.
- Product and shipping refunds do not overlap.
- Replacements and exchanges do not reverse revenue.
- Unknown TikTok fee keys remain visible.
- No customer PII appears in staging adapters, unions, tests, or marts.

## Rollback

Keep the legacy ingestion path available during the validation window. If a store fails reconciliation, remove it from `shopify_migrated_stores`, rebuild the affected models, investigate the replacement adapter, and repeat validation before another cutover.
