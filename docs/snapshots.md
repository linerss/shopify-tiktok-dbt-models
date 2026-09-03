# Snapshots, and why history stops at the snapshot layer

## The problem these solve

The intermediate models pick a current row with `qualify row_number() ... = 1` over an
append-only staging table. That works, but it leans on staging keeping every version the
connector ever sent. Rebuild staging, change a retention setting, or switch connectors and
the answer quietly changes.

`int_tiktok_shipping_line_items` is the clearest case. It orders **ascending** to keep the
earliest record, because the first version preserves the original sale economics before
later edits. That logic is correct and it is completely dependent on staging never being
rebuilt. Nothing in the code says so.

A snapshot makes that durable. dbt writes `dbt_valid_from` and `dbt_valid_to` and keeps
the version history in its own table, so "the first version we saw" and "the version as of
last Tuesday" become things you can select rather than things you hope survived.

## Snapshot the deduplicated row, not raw staging

A dbt snapshot expects one row per `unique_key` per run. Staging holds many. Point a
snapshot straight at staging and it compares a key against itself and records nonsense.

So each snapshot reduces to the current row first, using the same partition and ordering
its intermediate model uses, and then lets dbt track changes across runs. If you change a
dedup key in an intermediate model, change it in the matching snapshot too. They are a
pair.

## History does not flow downstream

Reporting reads current state only:

```sql
from {{ ref('snap_shopify_order_line_items') }}
where dbt_valid_to is null
```

Nobody is asking an ecommerce dashboard what it said last Tuesday. Carrying validity
windows into marts means every join has to reason about overlapping date ranges, every
aggregate needs a point-in-time filter, and one forgotten `where` clause silently doubles
revenue. The cost is real and the demand is not.

So the history exists, it is auditable, and it stays in the snapshot layer.

## When you will be glad it is there

Three cases, all after the fact:

- A number changed and nobody can say when or why. The snapshot has the before and after.
- Finance restates a prior period and you need the values as they were at close.
- A connector migration lands and you want to prove the new source agrees with the old one
  on records that existed before the cutover.

None of those need history in a mart. They need history to exist somewhere.

## Adopting them

The intermediate models still read staging. To move one across, replace the staging ref
and its `qualify` block with a snapshot ref and `where dbt_valid_to is null`. The dedup
already happened inside the snapshot, so the block comes out entirely.

Do one model, reconcile counts and totals against the current version, then move the next.

## Running them

```bash
dbt snapshot            # before dbt run, so models see current state
dbt run
dbt test
```

Snapshots only capture what exists when they run. They record change going forward, not
change that already happened, so the sooner one is in place the more history it holds.
