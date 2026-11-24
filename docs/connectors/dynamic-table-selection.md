---
status: engineering-draft
publication: handoff
---

# Newly discovered source tables

A running snapshot-and-CDC pipeline can automatically add tables that match an unchanged source
selection. This applies to shared capture (`srs: true`) when the source selects all tables or uses a
regular expression, and the pipeline directly synchronizes them to targets with their original names.
The pipeline must have no view, transforms, or target rename rules.

The engine periodically refreshes source discovery in the background. Once it admits a new matching
table, it retains the pipeline state, reads that table's snapshot, and continues CDC for both the old
and new tables. Previously confirmed tables keep their progress and target preparation records.
Delivery can pause briefly while the pipeline adds the table. A manual stop or discovery operation
is not required. Refresh latency depends on source discovery and
how many running pipelines share the node.

Source settings, literal table selections, target rules, and pipeline declarations are not changed
by discovery. Changing these declarations, removing previously selected tables, or using a pipeline
outside the supported shape continues to require the normal explicit preparation workflow. Failed
pipelines with unrelated errors are not automatically restarted by a discovery refresh.

If you select tables with a regular expression, configure the source database's required CDC logging
on a newly created table before inserting rows to be replicated. The new table must satisfy the same
connector requirements as tables selected at the pipeline's first start.
