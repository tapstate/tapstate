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

Tapstate is pre-1.0 and moves fast. There is deliberately no version table below: one goes
stale on the next release and still reads as current.

**The current major version is supported — all of its release lines, not only the newest.**
A fix lands on `main` and ships in the next release; when it matters to someone on an earlier
line of that same major version, it can also ship there as a patch release. Once a newer major
version exists, the previous one stops receiving fixes.

**Today that means every `0.x` release is supported**, since they are all one major version:
if `0.6.0` is out and you are still on `0.5`, a fix for you can ship as `0.5.1`. That changes
the day `1.0.0` is released — from then on the `0.x` series stops receiving fixes, and
upgrading to `1.x` is the path.

## Scope

In scope: anything in this repository, and the images and binaries published from it.

Out of scope, because they are not ours to fix: vulnerabilities in a database, driver, or
connector dependency that we merely call — report those upstream. If our use of one is what
makes it exploitable, that *is* in scope, and it is worth reporting here even if you are
unsure which side of the line it falls on.
