package io.akka.listmonk.domain;

import akka.javasdk.annotations.TypeName;

/** Everything that can happen to a campaign — SPEC-001 §3 rules 6, 9-11. */
public sealed interface CampaignEvent {

  @TypeName("campaign-created")
  record CampaignCreated(
      String id, String name, String subject, String fromEmail, String body, String listId,
      CampaignType type, int batchSize, int concurrency, int messageRatePerSecond,
      boolean slidingWindowEnabled, int slidingWindowSeconds, int slidingWindowLimit,
      int maxSendErrors) implements CampaignEvent {}

  @TypeName("campaign-started")
  record CampaignStarted(long maxSubscriberId, int toSend) implements CampaignEvent {}

  @TypeName("batch-recorded")
  record BatchRecorded(int batchSeq, int sentDelta, int errorDelta, long lastSubscriberId)
      implements CampaignEvent {}

  @TypeName("campaign-paused")
  record CampaignPaused(String reason) implements CampaignEvent {}

  @TypeName("campaign-resumed")
  record CampaignResumed() implements CampaignEvent {}

  @TypeName("campaign-cancelled")
  record CampaignCancelled() implements CampaignEvent {}

  @TypeName("campaign-finished")
  record CampaignFinished() implements CampaignEvent {}
}
