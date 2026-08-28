# Security policy

## Reporting a vulnerability

**Report privately, not as a public issue.** Use GitHub's private vulnerability reporting:

**https://github.com/tapstate/tapstate/security/advisories/new**

Only the maintainers can see a report filed there, and you keep access to the thread, so
the whole exchange — including the fix and the disclosure timing — happens in one place.
If that form is unavailable to you for any reason, open a public issue saying only that you
have a security report and how to reach you, with no details.

What helps, in rough order of usefulness: the version or commit you ran, what an attacker
gains, and the smallest thing that demonstrates it. A proof of concept beats a description;
a description beats nothing, so do not sit on a report because you have not built one.

## What happens next

We aim to acknowledge a report within **5 working days** and to tell you, at that point,
whether we consider it a vulnerability and what we intend to do. If we disagree that it is
one, you will get the reasoning rather than silence — the same as any other report here.

We credit reporters in the advisory unless you ask us not to.

## Supported versions

Tapstate is pre-1.0 and moves fast. **Fixes land on `main` and ship in the next release; only
the latest release line receives them.** There are no backports to earlier 0.x lines.

| Version | Supported |
| --- | --- |
| 0.3.x | Yes |
| < 0.3 | No — upgrade |

## Scope

In scope: anything in this repository, and the images and binaries published from it.

Out of scope, because they are not ours to fix: vulnerabilities in a database, driver, or
connector dependency that we merely call — report those upstream. If our use of one is what
makes it exploitable, that *is* in scope, and it is worth reporting here even if you are
unsure which side of the line it falls on.
