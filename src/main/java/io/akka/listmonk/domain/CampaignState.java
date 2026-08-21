package io.akka.listmonk.domain;

/** A campaign's definition, checkpoint and counters — SPEC-001 §2 "Campaign". */
public record CampaignState(
    String id, String name, String subject, String fromEmail, String body, String listId,
    CampaignType type, CampaignStatus status, int toSend, int sent, long lastSubscriberId,
    long maxSubscriberId, int errorCount, int batchSize, int concurrency,
    int messageRatePerSecond, boolean slidingWindowEnabled, int slidingWindowSeconds,
    int slidingWindowLimit, int maxSendErrors, int lastAppliedBatchSeq) {

  public static CampaignState empty() {
    return new CampaignState(null, null, null, null, null, null, null, null,
        0, 0, 0, 0, 0, 0, 0, 0, false, 0, 0, 0, 0);
  }

  public boolean exists() {
    return id != null;
  }

  /** Review checklist N1: a {@code BatchRecorded} whose {@code batchSeq} is not strictly
   * greater than the last one applied is a duplicate delivery of the same recordBatch
   * call (e.g. a transient retry at the component-call layer), not a new batch — applying
   * it again would double-count {@code sent}/{@code errorCount}. */
  public boolean isDuplicateBatch(int batchSeq) {
    return batchSeq <= lastAppliedBatchSeq;
  }

  public CampaignState apply(CampaignEvent event) {
    return switch (event) {
      case CampaignEvent.CampaignCreated e -> new CampaignState(
          e.id(), e.name(), e.subject(), e.fromEmail(), e.body(), e.listId(), e.type(),
          CampaignStatus.DRAFT, 0, 0, 0, 0, 0, e.batchSize(), e.concurrency(),
          e.messageRatePerSecond(), e.slidingWindowEnabled(), e.slidingWindowSeconds(),
          e.slidingWindowLimit(), e.maxSendErrors(), 0);
      case CampaignEvent.CampaignStarted e -> withStatus(CampaignStatus.RUNNING)
          .withCheckpointCeiling(e.maxSubscriberId(), e.toSend());
      case CampaignEvent.BatchRecorded e -> new CampaignState(
          id, name, subject, fromEmail, body, listId, type, status,
          toSend, sent + e.sentDelta(), Math.max(lastSubscriberId, e.lastSubscriberId()),
          maxSubscriberId, errorCount + e.errorDelta(), batchSize, concurrency,
          messageRatePerSecond, slidingWindowEnabled, slidingWindowSeconds, slidingWindowLimit,
          maxSendErrors, e.batchSeq());
      case CampaignEvent.CampaignPaused e -> withStatus(CampaignStatus.PAUSED);
      case CampaignEvent.CampaignResumed e -> withStatus(CampaignStatus.RUNNING);
      case CampaignEvent.CampaignCancelled e -> withStatus(CampaignStatus.CANCELLED);
      case CampaignEvent.CampaignFinished e -> withStatus(CampaignStatus.FINISHED);
    };
  }

  private CampaignState withStatus(CampaignStatus newStatus) {
    return new CampaignState(id, name, subject, fromEmail, body, listId, type, newStatus,
        toSend, sent, lastSubscriberId, maxSubscriberId, errorCount, batchSize, concurrency,
        messageRatePerSecond, slidingWindowEnabled, slidingWindowSeconds, slidingWindowLimit,
        maxSendErrors, lastAppliedBatchSeq);
  }

  private CampaignState withCheckpointCeiling(long newMaxSubscriberId, int newToSend) {
    return new CampaignState(id, name, subject, fromEmail, body, listId, type, status,
        newToSend, sent, lastSubscriberId, newMaxSubscriberId, errorCount, batchSize,
        concurrency, messageRatePerSecond, slidingWindowEnabled, slidingWindowSeconds,
        slidingWindowLimit, maxSendErrors, lastAppliedBatchSeq);
  }

  /** Rule 10 — has cumulative errorCount already reached the threshold. */
  public boolean errorThresholdReached() {
    return maxSendErrors > 0 && errorCount >= maxSendErrors;
  }
}
