# 00: The problem

## 02:14

The page goes off. Checkout is failing. Not all of it, and not cleanly: about
one in six attempts, which is exactly the ratio that makes people argue about
whether it is real.

Four people join the call. Between them they open eleven browser tabs. The
dashboards agree that something is wrong and disagree about what. One person
is looking at checkout. One is looking at the database, because it is always
the database. One is scrolling a log search that returned nine thousand lines
and is reading them at the speed a human reads. One is asking, reasonably,
whether anything shipped today.

Nobody is wrong. Everybody is somewhere different.

At 02:31 someone says the payment provider looks slow. At 02:34 someone else
says no, the payment provider is slow *because* we are holding its connections
open, and the reason we are holding them open is upstream. At 02:41 they find
it: a connection pool sized for a service that was retired eight months ago,
saturating under a traffic pattern that only happens on the last day of the
month. The fix takes ninety seconds. Agreeing on the diagnosis took
twenty-seven minutes.

The remediation was never the hard part. **The consensus was.**

## What actually went wrong

Read that again and notice what nobody lacked.

They did not lack data. Every fact needed to diagnose this was already
recorded, minutes before anyone woke up. The pool saturation was in the logs.
The retry storm was in the logs. The timing correlation between the two was
sitting there, timestamped, waiting.

They did not lack skill. Any one of those four could have found this alone
given an hour. The organisation had already paid for the expertise.

They did not lack tooling, exactly. There were dashboards. There was a log
search. There was alerting, which is how they got woken up.

What they lacked was **a system that had already done the reading**. Something
that had been watching the whole time, that knew what this service normally
looks like at 2 AM on the last day of the month, that noticed the pool errors
and the timeouts were the same event rather than two, and that had a first
draft of the explanation ready before the first person finished logging in.

Not a system that decides. A system that arrives at the call with a theory,
some evidence, and a shape for the argument, so the humans spend their
twenty-seven minutes on judgement rather than on assembly.

## The four things that were missing

Everything else in this repository follows from these.

**1. Nobody was watching continuously.** Alerting fires on thresholds someone
guessed at months ago. Between thresholds there is nothing. The signal that
mattered here, a slow change in the shape of one service's traffic, crossed no
threshold at all.

**2. Nothing knew what normal looked like for this service.** "More than 100
errors per minute" is a fine rule for a service that normally emits two. It is
useless for the one that normally emits eighty, and actively harmful for the
one whose Tuesday-at-2-AM baseline is different from its Friday-at-noon
baseline. Normal is per service, and it moves.

**3. Related signals arrived as unrelated noise.** The pool errors, the
timeouts, and the downstream 5xx were one causal chain presented as three
independent streams. Grouping them is not cosmetic. It is the difference
between one incident with a story and three incidents with none.

**4. The reasoning step was entirely human, and humans were asleep.** Reading
forty log lines and proposing "the pool is saturated, which is why the
provider looks slow" is the kind of work that has a shape. It is not
creativity. It is pattern recognition over evidence, and it is the slowest
step in the entire response purely because it waits for a person.

## What "better" looks like

Not zero incidents. Incidents are not a defect, they are what running things
feels like.

Better is that at 02:14 the page includes a link, and behind that link is one
incident rather than nine thousand log lines. The incident says which service,
when it started, what changed relative to that service's own history, which
neighbouring services are also unhappy, and a first attempt at why. The first
attempt is sometimes wrong. That is fine, and it is why the evidence sits next
to it, so a human can disagree in thirty seconds instead of assembling the
counter-argument from scratch.

Twenty-seven minutes becomes five. Not because the machine is smarter than the
four people. Because the machine did the reading while they were asleep.

## What this is not

It is worth closing off the version of this that gets oversold.

This does not remove the human from the incident. It removes the first twenty
minutes of clerical work from the human. Somebody still decides whether to
roll back, whether to page the vendor, whether the fix is safe at 3 AM. Those
are judgement calls with consequences, and they stay where they are.

It also does not prevent anything. It is a diagnosis system, not a prevention
system. The pool was misconfigured for eight months and no amount of log
analysis would have said so until it broke.

And it is honest about the failure case. A first draft that is confidently
wrong is worse than no draft, because a wrong theory at 2 AM is contagious.
That constraint, more than any other, shapes what follows: every conclusion
carries its evidence, and the system says plainly when it is falling back to
something mechanical rather than reasoning.

---

Next: [01: Scope and non-goals](01-scope-and-non-goals.md)
