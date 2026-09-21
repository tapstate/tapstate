---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/flat-embeds
---

# Flat embeds

A flat embed contributes one related row's fields directly to its parent document. It is the third
Nest child shape beside `object` and `array`:

```yaml
- from: invoice
  on: { invoice_order_id: id }
  as: flat
  key: [invoice_row_id]
```

`flat` has no container field, so it must omit both `path` and `arrayKey`. Its alias supplies a stable
internal state identity; use distinct aliases when several flat embeds read the same upstream stream.

## Cardinality

Flat supports both relationship directions that still produce one row per parent document:

- **one to one** - a child row carries the parent's key, such as one invoice belonging to one order;
- **many to one** - many parent documents point at one shared row, such as many orders referring to one
  customer. A change or deletion of that shared row is fanned out to every document that refers to it.

A one-to-many relationship is not flat. Tapstate does not try to infer this from a model, because a
source model can be missing or stale and uniqueness is ultimately a property of the rows that arrive.
When a second live row matches the same parent, the run stops with
`nest.flat-cardinality-violation`; it never chooses an arbitrary row. Use `as: array` for one-to-many.

## Field collisions

No declaration order wins a flat field collision. A flat field may not overlap:

- a field of the parent row;
- a path claimed by an `object` or `array` sibling;
- a field contributed by another flat sibling;
- a nested path inside the flat row.

When discovered models are available, job construction checks their field names and refuses a known
collision before starting. Some MongoDB collections and dynamic transform outputs have no complete model;
the renderer repeats the check against every actual row. A collision found there stops the run with
`nest.flat-field-conflict`, naming the flat embed, the fields, and what already owns them.

Rename or drop fields in an upstream `map` step. Join and identity fields still have to reach Nest, so a
common pattern is to rename them before Nest and drop the internal names afterwards.

## Complete pipeline example

This pipeline uses two flat children at once. `invoice` is one-to-one: each invoice carries the order key.
`customer` is many-to-one: many orders point at one customer row. The mapping steps give internal join
fields names that do not collide with the order, and the final mapping removes those internal fields from
the public document.

```yaml
version: tapstate/v1
kind: pipeline
id: order_documents
source: shop
transforms:
  - id: invoice_fields
    type: map
    from: [order_invoices]
    fields:
      invoice_row_id: $id
      invoice_order_id: $order_id
      id: false
      order_id: false

  - id: customer_fields
    type: map
    from: [customers]
    fields:
      customer_key: $id
      customer_name: $name
      customer_email: $email
      id: false
      name: false
      email: false

  - id: assembled
    type: nest
    from:
      order: orders
      invoice: invoice_fields
      customer: customer_fields
    root:
      from: order
      key: [id]
      embed:
        - from: invoice
          on: { invoice_order_id: id }
          as: flat
          key: [invoice_row_id]
        - from: customer
          on: { customer_key: customer_id }
          as: flat
          key: [customer_key]

  - id: public_document
    type: map
    from: assembled
    fields:
      invoice_row_id: false
      invoice_order_id: false
      customer_key: false

serve:
  from: public_document
  sync:
    - id: documents
      source: document_store
      write_mode: upsert
```

The workspace also needs the `shop` read Source and the `document_store` target Source. Validate and
discover the read Source before applying so model-based collision checks can run. The runtime check remains
authoritative even after discovery.

Flat fields that disappear on update or delete are carried as removals to the sink. The field history is
part of durable Nest state, so the same cleanup remains correct after a process restart.
