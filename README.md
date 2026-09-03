# Shopify and TikTok Shop dbt models

The dbt models I use to load Shopify and TikTok Shop into a warehouse without double
counting orders, losing refunds, or tripping over duplicate line items.

These are the working models, genericized for release. The SQL is the SQL that runs.

## The three things that actually bite

**Staging is append-only and that is on purpose.** The connector resends snapshots for
old orders, so staging holds more than one row per line item by design. A uniqueness test
there fails forever. Current-state uniqueness belongs in the intermediate layer, ordered
by business update time and then load time.

**A connector can change how it serializes an id.** One of ours started sending line item
ids as decimal strings partway through, so the same line item existed under two different
string keys either side of that date. The staging models cast ids to a single
representation before anything builds a key from them. If your order counts drift and
nothing else explains it, check the format of your ids across the change window.

**Shipping refunds can be counted twice.** The same refunded shipping can appear in a
refund shipping row and again in an order adjustment. Pick one canonical representation
and test for it rather than passing both downstream.

## Layout

```
models/1_staging/          one adapter per connector, per object
models/2_intermediate/     deduplication, refund logic, statement transactions
models/3_analytics/        TikTok Shop fees
macros/                    store mappings and helpers
tests/                     id format and uniqueness tests
```

Staging models come in pairs, one per connector, feeding the same intermediate layer.
That is a connector migration in progress: downstream models never learn there are two
physical sources, and stores move across one at a time.

## Before you run it

1. Replace the store mappings macro with your own stores and source tables. The original
   had a hardcoded list; it is a macro now so you configure it once.
2. Point the sources at your raw tables.
3. Build staging first and check the id format tests pass.

Every multi-store key includes the store. Platform ids are not guaranteed unique across
stores and that assumption is expensive to unwind later.

## What is not here

No customer contact data. Email, name, phone and street address are removed from every
select, with a comment where they were. Country and province codes remain, since fee and
refund analysis needs geography but not identity. No sample data and no client data.

## Honest notes

Snowflake dialect. These have not been executed since genericization, and the sources are
yours, so treat them as reference rather than an installable package. Validate against
your own numbers.

## License

MIT. See LICENSE.
