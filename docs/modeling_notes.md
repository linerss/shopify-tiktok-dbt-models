# Shopify and TikTok Shop modeling notes

## Append-only staging and current-state deduplication

Repeated snapshots in connector staging are not necessarily defects. An append-only table preserves what the connector delivered and when it delivered it. A uniqueness test on that staging table will fail by design when an old order is resent.

Enforce current-state uniqueness in the intermediate layer instead:

```sql
qualify row_number() over (
    partition by source_platform, store_id, order_id, normalized_line_item_id
    order by updated_at desc nulls last, source_loaded_at desc
) = 1
```

`store_id` is required in every multi-store partition, key, and join. Platform identifiers are not guaranteed to be globally unique across stores.

## Numeric identifier normalization

Some connectors serialize the same large identifier as an integer-like string in one batch and as a decimal or scientific-notation string in another. Normalize before constructing keys or deduplicating:

```sql
{{ normalize_numeric_id('line_item_id') }}
```

The macro first attempts numeric normalization and falls back to the original string for non-numeric identifiers. The ID-format test rejects decimal points and scientific notation after staging.

## Shopify refunds

Shopify refund payloads can describe shipping money in both a refund-shipping collection and an order adjustment whose kind is `shipping_refund`. Passing both representations downstream can double count the same refund.

This extraction uses one canonical rule:

- Keep product rows from `refund_line_item`.
- Keep shipping money from `adjustment` where `adjustment_kind = 'shipping_refund'`.
- Exclude `refund_shipping_line` from the intermediate output.

Validate the chosen representation against each connector because payload completeness differs. If a connector does not populate shipping adjustments, choose one alternative representation and add a non-overlap test rather than combining both blindly.

## TikTok Shop sales, cancellations, and returns

The order model separates two questions:

- What were the original sale economics? Use the earliest financial snapshot.
- What is the current operational status? Use the latest snapshot.

A cancellation-like field does not prove money was returned. Only configurable finalized statuses for refund and return-and-refund types reverse revenue. Replacement and exchange types preserve the original sale.

Line-level reversal rows use the connector's refund amount rather than reversing the full original line. A full refund can reverse quantity; a partial amount refund leaves quantity null because fractional quantity cannot be inferred safely. Order-level refunds with no line identifier remain visible as unallocated negative events instead of being dropped or spread across products.

Product-return payloads do not prove that shipping was refunded. The shipping model therefore publishes sales only. Add a dedicated shipping-refund source and amount before creating negative shipping events.

## TikTok statements and transactions

Connector batch metadata can reconstruct nested parent-child records, but it is transport metadata rather than a durable business identifier. Expose platform statement and transaction IDs as canonical keys.

Aggregate duplicate fee types before constructing a fee object because Snowflake `OBJECT_AGG` rejects duplicate keys. Keep one row per statement transaction so `transaction_amount` appears once. Flatten fee objects only in the fee mart, where transaction totals are intentionally omitted to prevent accidental multiplication. Never join order-level fees to every SKU unless an explicit allocation rule exists.

The fee mart preserves the raw fee type, maps unknown types to `other`, and keeps native currency. Add a reporting-currency field only after joining a dated FX source with documented rate direction and missing-rate behavior.

## Privacy boundary

The public models deliberately exclude customer contact details, personal identifiers, full street addresses, payment details, tracking identifiers, notes, messages, and arbitrary free text. Only country and province codes are retained as optional coarse geography.

Use an allowlist of selected fields rather than `select *`. New connector columns should require an intentional schema review before they enter a public model.
