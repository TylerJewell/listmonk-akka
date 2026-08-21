# listmonk-akka

Sends an e-mail campaign to exactly the subscribers a list's opt-in rules make eligible,
in checkpointed batches whose fetch rate is bounded by how fast sending completes.

A port of [knadh/listmonk](https://github.com/knadh/listmonk) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

listmonk is a self-hosted newsletter and mailing list manager. It was ported to derive a
specification format precise enough to regenerate a system on a different stack — the
port is the vehicle, the specification is the deliverable.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `listmonk-port/`.

---

## listmonk → this port

📉 27,147 Go/SQL lines → **997 Java lines** (whole project)<br>
📉 1,111 Go/SQL lines → **588 Java lines** (like for like)<br>
📁 210 files → **34 files**<br>
⚡ ~3 ms, raw SQL query → **~220 ms**, this port's own HTTP surface — not comparable, see `bench/REPORT.md` §2<br>
🎯 4 segmentation/backpressure claims checked against the source by running something, **4 agreed**

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/listmonk-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.3 hours** from the first command to the published repository, **1.3** of them active<br>
💬 **731** exchanges with the model<br>
✍️ **550,249** tokens written by the model, **250,942,226** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **40** tests, plus 2 deliberate breakages to check the tests notice

The record of every decision, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

A **list** has an opt-in type that decides which subscribers on it are eligible for a
**campaign** — a subscription's own status, independent of any other list the same
subscriber is on. From the specification:

- **Which statuses are eligible depends on both the campaign's type and the list's
  opt-in type**, not either alone. A regular campaign on a single-opt-in list reaches
  everyone who hasn't unsubscribed; the same campaign on a double-opt-in list reaches
  only confirmed subscribers; an opt-in campaign on a double-opt-in list reaches only
  the unconfirmed ones — the inverse of the regular case.
- **A blocklisted subscriber is never eligible**, on any list, regardless of their
  status there — checked by running the source's own query against a real blocklisted
  subscriber, not read from the SQL and assumed.
- **The eligible set is frozen the moment a campaign starts**, at the highest eligible
  subscriber id and the count at that instant. A subscriber who becomes eligible after
  that is not reached by that run.
- **Fetching the next batch waits for the current one to finish sending.** A batch is
  sent under a hard concurrency cap and a rate limit, and the next fetch does not start
  until every send in the current batch has completed — so the pipeline can never queue
  further ahead of what sending can absorb than one batch's worth of work.
- **An error-count threshold pauses the campaign mid-batch, not just before the next
  one** — the remaining not-yet-sent subscribers in that batch are skipped, not sent and
  not counted as failures.
- **A pause takes effect between batches, never in the middle of one**, and a resume
  picks the checkpoint back up without recomputing the eligible set.

---

## Design decisions

**Segmentation is one row per subscription, not one row per subscriber.** A subscriber's
relationship to each list they're on — status, and whether they're blocklisted — is its
own durable record, the same granularity the source's own `subscriber_lists` table uses.
That is what lets one view answer "who's eligible for this list" without joining across
subscribers who belong to several lists at once.

**Blocklisting is looked up from the subscriber, not the segmentation view.** A view here
only catches up to a write after some delay; asking it "does this subscriber's brand-new
subscription need to start out blocklisted" right after that subscription was created
could ask before it had caught up. A subscriber's own blocklist flag answers that
instantly instead.

**The campaign record is the source of truth; the send pipeline is a process on top of it.**
An operator checking a campaign's status or counts can do so whether or not the workflow
driving it has run recently, the same separation the source keeps between its database
row and the in-memory worker that sends for it.

**The rate limit's counters live in the pipeline's durable record, not in a plain object.**
A step of a long-running process here is not guaranteed to run on the same copy of that
process's code the step before it did, so a limit that only counts in memory resets
itself without warning. Writing the count down after every batch is what makes it hold
for the whole campaign.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/listmonk-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9028.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once
- An SMTP server to send through — a local test server (e.g. `mailhog`, `maildev`) is
  enough; nothing here requires a real mail provider

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9028**.

### Try it

```bash
# a list, and one subscriber
curl -X POST localhost:9028/lists/weekly \
  -H 'Content-Type: application/json' -d '{"name":"Weekly digest","optinType":"SINGLE"}'
curl -X POST localhost:9028/lists/weekly/subscribers/1 \
  -H 'Content-Type: application/json' -d '{"email":"ada@example.com","name":"Ada"}'

# how many subscribers a campaign would reach right now
curl localhost:9028/lists/weekly/eligible-count/REGULAR

# a campaign, addressed to that list, started
curl -X POST localhost:9028/campaigns/first-send \
  -H 'Content-Type: application/json' \
  -d '{"name":"First send","subject":"Hi {{name}}","fromEmail":"you@example.com","body":"Hello {{email}}","listId":"weekly","type":"REGULAR"}'
curl -X POST localhost:9028/campaigns/first-send/start

# progress
curl localhost:9028/campaigns/first-send
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `LISTMONK_SMTP_HOST` | `localhost` | SMTP server host. |
| `LISTMONK_SMTP_PORT` | `1025` | SMTP server port. |
| `LISTMONK_SMTP_USERNAME` | empty | Empty means no SMTP authentication. |
| `LISTMONK_SMTP_PASSWORD` | empty | |
| `listmonk.send.default-batch-size` | `50` | How many subscribers one send batch covers. |
| `listmonk.send.default-concurrency` | `4` | How many sends are ever in flight at once. |
| `listmonk.send.default-message-rate-per-second` | `10` | The per-second send cap. |
| `listmonk.send.default-max-send-errors` | `100` | Cumulative failures before a campaign auto-pauses. |

The last four are set in `src/main/resources/application.conf` and applied to a campaign
at creation time.

---

## Where it differs from listmonk

Everything not listed here behaves the same way on purpose, including the parts that
look like mistakes.

- **One campaign reaches one list, not several with subscribers deduplicated across
  them.** listmonk lets a campaign target multiple lists and sends once to a subscriber
  on more than one; this port's segmentation rule — which statuses are eligible, given
  the campaign's type and the list's opt-in type — is unchanged by this, but the
  multi-list composition on top of it isn't implemented.
- **Message rendering is `{{email}}`/`{{name}}` substitution, not the source's Go
  template engine.** listmonk compiles a campaign body as a Go template with a large
  function library (conditionals, loops, link tracking, inline images). A campaign body
  written for listmonk will not render identically here.
- **Segmentation reads are eventually consistent; the source's are transactionally
  consistent.** Starting a campaign immediately after subscribing someone can, for a
  brief window, not see that subscription yet — found by running the two in sequence
  with no wait and watching it happen. `GET /lists/{id}/eligible-count/{type}` is backed
  by the same read path, so a caller who needs to know it has caught up can poll it.
- **The rate limit is continuous for as long as the pipeline's driving process stays
  live, and resets across a longer gap.** The source's equivalent counters live on one
  process for as long as it runs; this port's live in the send pipeline's own record and
  are rebuilt from scratch if that record is ever reactivated after being idle for a
  while.
- **Config is per-campaign, not process-wide.** listmonk's batch size, concurrency,
  message rate, and error threshold are one setting shared by every campaign a running
  instance ever sends. This port stores the same four numbers on each campaign at the
  moment it's created instead — operationally the same wherever every campaign in a
  deployment uses one configuration, which is the common case.
- **A campaign is started by an explicit call, not a scheduled time.** listmonk can hold
  a campaign in a "scheduled" state and start it automatically once its send time
  arrives; this port starts a campaign only when told to.
- **Not checked: multiple campaigns sending at once, or a very large single list.**
  Every measurement in `bench/REPORT.md` is one campaign, one list, at a scale in the
  thousands of subscribers.

---

## Licence

listmonk is AGPL-3.0, © Kailash Nadh and contributors. This port reimplements the
behaviour without copied source; see [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md) for
what that does and does not settle about this repository staying private.
