---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs
---

# Documentation

**The canonical user documentation is at `https://tapstate.dev/docs`.** What is in this
directory is written alongside the implementation and has not been through documentation
review: engineering drafts, executable samples, and material tied to a particular version.
Read a page here as an engineer's draft, and the published site as the documentation.

Every page here says which of the two it is, in a header at the top of the file - a draft
headed for publication, or a pointer to the published page that replaced it. The shapes,
and what a contributor does with them, are in
[CONTRIBUTING.md](../CONTRIBUTING.md#documentation). Anything under this directory that is
not a page - sample data, workspace files, scripts - carries no header: it belongs with the
code and is read by running it.


| Start here | |
|---|---|
| [First run](first-run/) | The guided path: install the CLI, answer a few questions, and bring a workspace to running - the contract every step is held to. |
| [Quickstart](quickstart-online.md) | The full demo (`install.tapstate.dev/demo`): sample databases, the server, and a live object assembled from two engines - and the same flow by hand, verb by verb, as the reference for what the guided path does. |
| [Running on your own databases](running-on-your-own-databases.md) | Run the server as a process on your machine, against a MySQL and a MongoDB you already have. |
| [Tutorials](tutorials/) | Worked scenarios, each with its own sample data, run end to end. |

| Features | |
|---|---|
| [Nest](nest/) | Assemble one document out of many tables and keep it current. |
| [Connectors](connectors/) | How the bundled connector catalog is generated, what has to be rebuilt when something moves upstream, and how this repository declares what a connector can do. |

## How this directory is laid out

Two shelves, because a reader arrives with one of two questions.

- **"Teach me"** goes to `tutorials/` - one directory per tutorial, holding the walkthrough and
  everything it needs. See [its README](tutorials/README.md) for what a new one has to do.
- **"I am using X and need to know Y"** goes to `<feature>/` - one directory per feature, with a
  `README.md` that says what the feature is and which page answers what.

A page's directory is what scopes it, so filenames do not repeat the feature name: it is
`nest/throughput-limits.md`, not `nest/nest-throughput-limits.md`. Anything that would sit loose at
this level either belongs to a feature or is a starting point, and there are only ever a few of
those.
