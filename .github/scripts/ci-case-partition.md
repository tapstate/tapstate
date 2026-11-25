# Archived original case partition

`ci-case-partition.sh` derives the original case set from variant A and the
additional case sets from variants B and C. Each variant is independently
replayed through the existing archived case, input, source, recipe, original
receipt and indexed Git/ZIP chain. It never executes archived code or changes
a checkout, workflow or PR.

Manual two-variant mechanism archives use `ci-mechanism-inputs.sh`. The original
PR partition keeps its three-variant, pull-request-only request contract.

The request has exactly `schema: 1`,
`type: archived-original-case-partition-request` and `variants`. The variant map
has exactly A, B and C; each entry has `archive_cases`, a relative path/SHA-256
reference to an archived case request, and `source_root`, the absolute path of
its owned worktree. All references are beneath the same evidence root.

```sh
bash .github/scripts/ci-case-partition.sh \
  --manifest /path/to/archive/partition-request.json \
  --evidence-root /path/to/archive \
  --output /path/to/archive/original-case-partition.json
```

The three original pull-request runs must have distinct heads and run IDs,
attempt 1, the same repository/PR/branch/base, and the same independently
replayed preserved-source and owned-recipe digests. Duplicate identities or
missing original cases/methods reject. An identity includes lane, module, test
kind, class, method and parameter. No caller-provided addition list is accepted.

Only fully parsed source inventories with no unresolved provider identities
produce an authoritative partition. Otherwise the original/addition fields
remain null, known source cases and unresolved diagnostics remain visible,
and the partition proof flags stay false. Matching producer declarations do
not resolve a computed provider.

This proves an identity partition against the archived observations. It does
not prove unchanged original assertions, normalized original P/I/D/R/U/spec,
workflow applicability, whole-build provenance, a first complete PR sample or
performance acceptance. Those proof flags remain false. Full component
fingerprints and separate behavior/coverage/gate comparisons remain required.

The offline controls replay three independent real Git/ZIP fixtures, exercise
additional/missing/duplicate identities, foreign bindings, unknown providers
and the actual exclusive-output CLI. Their declared bytecode/dependencies are
synthetic protocol data, not actual compiled builds or native CI success.
