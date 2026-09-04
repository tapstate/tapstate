# Rehearsal: the documentation-impact check, seen from a fork

This file exists only to carry a pull request. A workflow that runs on `pull_request` is taken
from the base repository's default branch, and a fork's pull request is given a read-only token
with no secrets -- so whether a check reports at all, and whether it can fail, is not answered by
running it on a branch inside this repository.

Nothing here is meant to be merged. The pull request that carries it is closed once the check has
been observed reporting, failing, and then passing.
