---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/tutorials
---

# Tutorials

Worked scenarios you can run end to end. Each one does something real and says what to look at
afterwards. They assume the stack from the [online quickstart](../quickstart-online.md) is up and say
what they need on top of it: one brings its own sample data, one runs against a source you already
have, and one stands the processes up from an IDE instead - start there if you are working on
Tapstate itself rather than using it.

| Tutorial | What you build | Time |
|---|---|---|
| [Expanding order items](expanding-order-items/) | Four MongoDB rows from two MySQL parents, followed by CDC deletion of a parent and its expanded rows. | ~15 min |
| [Assembling one document out of many tables](nest-document-assembly/) | Three live documents - order, customer and product - assembled from the same nine relational tables with a `nest` transform and kept current by change data capture. Includes the rule that decides which document shapes a nest can express. | ~30 min |
| [Browsing live data by hand](browsing-live-data-by-hand/) | Nothing - it reads. The five read verbs and the three MCP read tools, run against a source you already have. Written to be run by a person: four of its checks - how `watch` redraws, what closing a window releases, what an agent discovers mid-session - are ones no test can make. | ~30 min |
| [Running the server and CLI from an IDE](running-from-an-ide/) | The server out of IntelliJ IDEA against a MongoDB you supply, and the CLI driving it from a terminal, ending at a connected and authenticated CLI. Carries no sample data: it stops before anything moves, and hands over to [the nest tutorial](nest-document-assembly/). | ~20 min |

## Adding a tutorial

**One directory per tutorial, and everything it needs lives inside it.**

```
docs/tutorials/
├── README.md                     <- this index
└── <tutorial-slug>/
    ├── README.md                 <- the walkthrough itself
    ├── <data>.sql                <- sample data, if it has any
    └── <other assets>            <- workspace files, diagrams, scripts
```

The directory name is the slug a reader sees in the URL, so name it for the thing being taught
(`nest-document-assembly`), not for the page (`tutorial-3`). Putting the walkthrough in `README.md`
means a link to the directory renders it.

**Classify the page.** A new walkthrough is an engineering draft until documentation
engineering publishes it, so its `README.md` opens with the draft header and names where it
is headed:

```
---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/tutorials/<tutorial-slug>
---
```

Once the published page is canonical, the page here shrinks to a pointer carrying
`status: canonical-pointer` and its `canonical_url`, and the sample data stays. The sample
data itself is never classified - it is executable, not a page. A pull request that adds or
changes a page without one of the two headers is refused; the full contract is in
[CONTRIBUTING.md](../../CONTRIBUTING.md#documentation).

Two rules the existing one follows and the next one should too:

- **Sample data is generated deterministically** - no random values, no timestamps of the day it ran.
  A tutorial quotes row counts and document shapes in its text, and those numbers have to be the ones
  the reader sees. Seed with a numbers table and arithmetic, not with `RAND()`.
- **Say what does not work, not only what does.** The failure a reader would otherwise ship is worth
  more space than the happy path, especially where the product accepts something and goes wrong
  quietly rather than refusing it.
