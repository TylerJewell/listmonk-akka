# Acknowledgements

This project is a port of **[knadh/listmonk](https://github.com/knadh/listmonk)**.

## Licence and copyright

`knadh/listmonk` is licensed under the **GNU Affero General Public License v3.0
(AGPL-3.0)** — read directly from `listmonk/LICENSE` in the cloned source, not
assumed from a badge. The project is maintained by Kailash Nadh and contributors.

AGPL-3.0 is a strong copyleft licence with a network clause: anyone who runs a
modified version of an AGPL-3.0 program as a network service must offer that
version's source to every user who interacts with it over the network, not only to
people who receive a distributed copy.

## Was anything copied verbatim?

No. Every file in `listmonk-akka/` is original Java written from scratch against the
Akka SDK. Nothing was copy-pasted from `listmonk/internal/` or `listmonk/queries/` —
the SQL in `queries/campaigns.sql` was read to understand the exact segmentation and
checkpointing rules (which subscriber statuses a regular vs. opt-in campaign draws
from, how `last_subscriber_id`/`max_subscriber_id` bound a batch), and those rules
are reproduced as Akka View queries and entity commands with independently written
Java, not a translation of the SQL text itself.

## Is behaviour derived even where no text was copied?

Yes, plainly. This is a behaviour port: the per-list optin-status segmentation rule,
the checkpointed batch cursor, the bounded-queue backpressure between fetching
subscribers and sending messages, the message-rate throttle, the sliding-window
throttle, and the error-threshold auto-pause are all read directly from listmonk's
`internal/manager` behaviour (confirmed by running `TestRealBackpressure` against the
actual `internal/manager` package — see `docs/question-log.md`) and reproduced
deliberately. That is what a port is.

## What licence does that force on this project?

Because behaviour (not text) is what was copied, and because `listmonk-akka` is not
distributed as a modified copy of listmonk's own source tree, `listmonk-akka` is not
itself required to be licensed AGPL-3.0 merely by having read listmonk's code. It is
still a derivative in the ordinary sense of "built by studying and reproducing another
system's behaviour," and this repository stays **private** (PIPELINE.md step f default)
specifically because that question — whether a behaviour-derived rebuild of an
AGPL-3.0 network service inherits AGPL-3.0's network clause once it is itself run as a
network service — was not resolved here. It is flagged here rather than guessed at:
publishing this repository publicly, or operating it as a network-accessible service,
should not happen without a specific legal read of that question, which is outside
what this port can settle by reading source code.

## Also used

- Akka (the Agentic Systems Platform) — Java SDK, `io.akka:akka-javasdk-parent`.
- Jakarta Mail (Eclipse Angus implementation) — for the SMTP messenger, the same role
  `github.com/knadh/smtppool` plays in the source.
