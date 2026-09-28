---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest
---

# Nest

A `nest` transform assembles rows from several tables into one document and then keeps that document
current: every insert, update and delete on any table feeding it lands in the document, without
anyone re-running a query.

**New to it?** Start with the tutorial - it builds three documents out of one nine-table shop and
teaches the rule that decides which document shapes a nest can express:
[Assembling one document out of many tables](../tutorials/nest-document-assembly/).

The pages here answer the two questions that come next, once something is running:

| Page | Answers |
|---|---|
| [Flat embeds](flat-embeds.md) | Merge one related row directly into its parent, including one-to-one and many-to-one direction, collision checks, one-to-many refusal, and a complete multi-flat example. |
| [Structural key changes](structural-key-changes.md) | A row's key was *edited* - it did not just change, it changed **where it belongs**. What that costs, what you switch on for it to work, what happens when you leave it off, and what it needs from the source. |
| [Throughput limits](throughput-limits.md) | The unit a nest sends is the **whole document**, so any row feeding a root rewrites that root in full. Arithmetic you can do on your own numbers, before you run it, to see whether your data shape fits. |
