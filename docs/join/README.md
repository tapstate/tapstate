---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/join
---

# Join: limits and usage

Join maintains a flat result as source rows change, using the same SQL definition for
snapshot and change processing. This engineering draft describes the implementation in
this revision; it has not yet been reviewed for publication as user documentation.

## Choose a supported relationship

Use one driving fact source with dimensions joined directly to it. A common shape is many
orders referring to one customer:

```sql
SELECT o.id AS order_id, c.name AS customer_name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
```

Every alias in the join step's `from:` map must name a source table. A join cannot read the
output of another step, such as a `js`, `map` or another `join` step: its SQL is resolved against
the discovered columns of each input, and its rows are keyed on the driving table's primary key,
and a step's output has neither. Validation refuses such an alias with
`dsl.join-input-not-a-table`; do that step's work after the join instead. An alias bound to a
`/…/` pattern is refused as well, with `dsl.join-input-is-a-pattern`, even when the pattern matches a
single table: write the table's name.

Each dimension must be unique on the **complete join key**, which is not necessarily its
primary key. One fact row produces at most one output row. One-to-many and many-to-many
SQL fan-out are not supported: duplicate dimension keys replace the stored match with the
later-arriving row. There is no uniqueness guard, so a running task is not evidence that
this prerequisite holds. Check the source data and enforce the appropriate unique constraint
where possible before using the relationship.

For example, joining orders on `customers.region` is unsafe when multiple customers share
one region, even if every customer has a unique `id`. Adding the customer id to the output
key does not enable fan-out.

A dimension update can refresh many existing fact rows. This reverse propagation is supported;
it is different from producing multiple output rows for one fact row. A heavily shared
dimension key can still cause substantial recomputation and target writes.

## SQL subset

| Construct | Boundary |
|---|---|
| INNER / LEFT JOIN | Supported for equality associations directly from the driving source to each dimension. |
| RIGHT JOIN | Normalized to LEFT JOIN with the sides swapped; the normalized shape must meet the same driving-source restriction. |
| ON | Conjunctions (`AND`) of qualified-column equalities; composite keys are allowed. |
| ON with OR | LEFT JOIN onto one table only; each alternative is a conjunction of equalities. The first alternative that matches wins; see [Match on one column or another](#match-on-one-column-or-another). |
| Projection | Direct columns, aliases, and supported per-row expressions; not arbitrary SQL functions. |
| FULL OUTER, CROSS/comma, NATURAL, USING, LEFT SEMI | Unsupported. |
| Non-equality joins, dimension-to-dimension chains, nested joins on the right side | Unsupported. |
| WHERE, DISTINCT, GROUP BY / HAVING, aggregates, windows / OVER | Unsupported in the Join SQL. |
| Subqueries, ORDER BY, LIMIT / OFFSET | Unsupported. |

Unsupported SQL constructs are rejected during validation; additional runtime shape
restrictions can fail when the job initializes. Run `tapstate validate` and `tapstate explain` before
applying a pipeline; successful SQL parsing alone does not establish that the runtime shape
is supported. Column names are checked against the discovered source schemas when the pipeline
starts: a column no input table has fails the start with `actuation.join-sql-invalid`, and the
pipeline stays failed until the SQL or the source is corrected and it is started again. MERGE statements are outside the Join SELECT surface.

Any NULL component makes a join key unmatchable, including when the opposite side also has
NULL. A LEFT JOIN can publish an unmatched fact row with NULL dimension fields; a dimension
that arrives later can turn that row into a match.

## Match on one column or another

A dimension row is sometimes named by one column on some fact rows and by another on the rest,
for example when two upstream systems feed one transaction table. Write the alternatives with
`OR` in a `LEFT JOIN`:

```sql
SELECT t.tran_id, l.name AS location_name, l.locn_id AS location_id
FROM wallet_tran t
LEFT JOIN location l
  ON t.locn_code = l.locn_code
  OR t.hub_locn_id = l.hub_locn_id
```

The alternatives are tried in the order written, and each fact row takes the dimension row the
first matching alternative finds. Here a transaction whose location code matches a location
uses that location even when its hub id names a different one; only a transaction whose code
matches nothing falls back to its hub id. If no alternative matches, the dimension columns are
NULL, as for any `LEFT JOIN`. A fact row still produces exactly one output row: an `OR` join
never publishes one row per matching alternative.

Each alternative is kept up to date on its own. Inserting a location that the first
alternative now matches switches the affected rows to it; deleting it falls back to the next
alternative; editing a location's code or hub id moves the rows that matched the old value.
Every alternative must still be unique on its own key, as in the rest of this page, and NULL
keys still match nothing.

Limits:

- Only `LEFT JOIN`. An inner or right join on `OR` is refused, because which rows it should
  publish when several alternatives match is not something one output row per fact row can say.
- Each alternative is a conjunction of equalities between the joined table and the driving
  source, and must name the joined table.
- Read the joined table through qualified columns, such as `l.name`. `SELECT *`, `SELECT l.*`
  and a later join whose `ON` names the `OR`-joined table are refused, and an unqualified column
  that only the joined table has is refused as ambiguous.
- An output column read from the `OR`-joined table is computed, so it carries the SQL type
  rather than the source column's own type metadata.

## Publish the output key

For **default/upsert sync**, project every primary-key column of the driving fact table as a
direct column reference. Aliases are allowed, as in `o.id AS order_id` above. Computed
substitutes such as `UPPER(o.id)` do not count. For a composite primary key, publish all of
its columns. A missing key column is refused with
`actuation.join-output-key-not-published`.

The sync target key is derived from the fact primary key. Dimension keys can be projected
as ordinary fields but are not added to that target key. The output model uses the fact table
name and projected fields; configured table renaming still applies.

**Append-only sync is exempt from this projection requirement.** It does not thereby become
an upsert or a maintained current-state table: repeated output events can append rows.
If any sync target uses default/upsert mode, the projection requirement applies. Views
validate their own declared keys separately.

Directly projected source columns retain their declared types. Computed columns whose type
is not supplied to the target model rely on connector inference; verify the resulting target
schema for your connector before deployment.

## Check the result before relying on it

Confirm dimension-key uniqueness and the output key first. Compare the joined target against
the equivalent source SQL on representative data, including a dimension edit, deletion,
a fact changing its reference, and an initially missing dimension arriving later. A task
remaining RUNNING does not prove those results are correct.

These checks do not expand the SQL subset or remove the preview's deployment limits.
