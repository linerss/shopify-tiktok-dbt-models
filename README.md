# Shopify and TikTok Shop dbt reference models

Reference dbt models for loading Shopify and TikTok Shop into a warehouse without
double counting, losing refunds, or tripping over duplicate line items.

These came out of production work across a multi-store portfolio. They are shared as
reference, not as an installable package: the sources are yours, so you will need to
point the staging models at your own raw tables.

## What is actually hard about this

Three problems show up in almost every Shopify warehouse, and this repo is mostly about them.

**The same line item arrives twice in different formats.** Connectors resend snapshots for
old orders, and a large numeric id can serialize as an integer-like string in one batch and
a decimal or scientific-notation string in the next. Deduplicate before you normalize and
the two representations look like different keys. `macros/normalize_numeric_id.sql` fixes
the representation; the intermediate layer enforces one current row per key.

**Append-only staging looks broken but is not.** Staging preserves what the connector
delivered and when. A uniqueness test there will fail forever. Enforce current-state
uniqueness downstream instead, with `row_number()` ordered by business update time and then
connector load time.

**Shipping refunds get counted twice.** Shopify can represent the same refunded shipping in
a refund-shipping row and again in an order adjustment. Pick one canonical representation
per connector and test for it, rather than passing both downstream.

## Layout

```
models/staging/       one adapter per connector, both exposing the same typed contract
models/intermediate/  deduplication, refund logic, statement reconciliation
models/marts/         TikTok Shop statement fees
macros/               id normalization and finance helpers
tests/                id format, uniqueness, and refund shipping tests
docs/                 modeling notes and the store-by-store migration guide
```

Staging models come in `legacy` and `replacement` pairs. That is the connector migration
pattern: run both connectors in parallel, adapt each to the same contract, union them in
the intermediate layer, and move one store at a time. `docs/connector_migration.md` has the
cutover sequence and what to reconcile at each step.

## Using these

1. Point `models/staging/_sources.yml` at your raw tables.
2. Replace `<STORE>` placeholders and the `shopify_migrated_stores` variable with your stores.
3. Build staging first and check the id format tests pass before going further.
4. Read `docs/modeling_notes.md`. It explains why each decision is the way it is.

Multi-store keys include a store identifier everywhere. Platform ids are not guaranteed
unique across stores, and that assumption is expensive to unwind later.

## What is not here

No customer contact data. Email, name, phone and street address are deliberately excluded;
only country and province codes are carried, because fee and refund analysis does not need
identity. No sample data, and no client data of any kind.

These models have not been run against a public dataset. They came from a working
warehouse, then were genericized. Validate against your own numbers before trusting them.

## License

MIT. See LICENSE.
