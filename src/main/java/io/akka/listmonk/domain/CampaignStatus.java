package io.akka.listmonk.domain;

/** A campaign's lifecycle status, mirroring the source's {@code campaign_status} enum
 * minus {@code scheduled} (SPEC-001 §1 — scheduling is out of scope). */
public enum CampaignStatus {
  DRAFT,
  RUNNING,
  PAUSED,
  CANCELLED,
  FINISHED
}
