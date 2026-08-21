package io.akka.listmonk.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.StepName;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import com.typesafe.config.Config;
import io.akka.listmonk.domain.CampaignState;
import io.akka.listmonk.domain.CampaignStatus;
import io.akka.listmonk.domain.Segmentation;
import java.time.Duration;
import java.util.List;

/**
 * The send pipeline — SPEC-001 §3 rules 6-11: fetch a checkpointed, segmented batch, send
 * it under bounded concurrency and a rate limit, record the outcome, repeat until the
 * campaign is exhausted, paused, or cancelled.
 *
 * <p>One workflow instance per campaign, workflow id equal to the campaign id, so the
 * pipeline survives a restart at the last recorded batch (question-log row 1's
 * fetch-follows-send property, reproduced structurally per SPEC-001 §4 decision 1: the
 * "fetch-batch" step never runs again until "send-batch" has finished, which is what
 * bounds how far ahead of sending the pipeline can ever fetch).
 */
@Component(id = "campaign-send")
public class CampaignSendWorkflow extends Workflow<CampaignSendWorkflow.SendState> {

  public record SendState(
      String campaignId, String listId, List<String> statuses, long maxSubscriberId,
      int batchSize, int concurrency, int messageRatePerSecond, boolean slidingWindowEnabled,
      int slidingWindowSeconds, int slidingWindowLimit, int maxSendErrors, String fromEmail,
      String subject, String body, RateLimiter.Snapshot rateLimiterSnapshot, int nextBatchSeq) {

    /** Review checklist N1: {@code nextBatchSeq} is read once per {@code send-batch}
     * invocation and only advanced here, alongside the rate limiter snapshot, once that
     * attempt's {@code recordBatch} call has returned — so a retry of the same attempt
     * (not a new one) always carries the same {@code batchSeq}. */
    SendState afterBatch(RateLimiter.Snapshot snapshot) {
      return new SendState(campaignId, listId, statuses, maxSubscriberId, batchSize,
          concurrency, messageRatePerSecond, slidingWindowEnabled, slidingWindowSeconds,
          slidingWindowLimit, maxSendErrors, fromEmail, subject, body, snapshot,
          nextBatchSeq + 1);
    }
  }

  private final ComponentClient componentClient;
  private final Messenger messenger;

  public CampaignSendWorkflow(ComponentClient componentClient, Config config) {
    this.componentClient = componentClient;
    this.messenger = new SmtpMessenger(
        config.getString("listmonk.smtp.host"),
        config.getInt("listmonk.smtp.port"),
        config.getString("listmonk.smtp.username"),
        config.getString("listmonk.smtp.password"),
        config.getBoolean("listmonk.smtp.starttls"));
  }

  @Override
  public WorkflowSettings settings() {
    return WorkflowSettings.builder()
        .defaultStepTimeout(Duration.ofMinutes(2))
        // send-batch has already made real, non-idempotent SMTP calls by the time it
        // could fail (e.g. recordBatch racing a concurrent pause) -- retrying it would
        // resend messages the batch already delivered. Fail forward to fetch-batch
        // instead, which re-checks campaign status and either continues or stops
        // cleanly; it will not re-run any send that already completed.
        .stepRecovery(CampaignSendWorkflow::sendBatchStep,
            akka.javasdk.workflow.Workflow.RecoverStrategy.maxRetries(0)
                .failoverTo(CampaignSendWorkflow::fetchBatchStep))
        .build();
  }

  /** Rule 6: accepted only once per campaign (idempotent on a repeat call, the same
   * at-least-once-delivery reasoning {@code opal-akka}'s {@code FanOutWorkflow} documents). */
  public Effect<Done> start(String campaignId) {
    if (currentState() != null) {
      return effects().reply(Done.getInstance());
    }
    return effects()
        .updateState(new SendState(campaignId, null, List.of(), 0, 0, 0, 0, false, 0, 0, 0,
            null, null, null, RateLimiter.Snapshot.none(), 1))
        .transitionTo(CampaignSendWorkflow::initStep)
        .thenReply(Done.getInstance());
  }

  /** Rule 11 (resume): re-enters the fetch loop after a {@code CampaignEntity.resume()}
   * has already moved the campaign back to RUNNING. The workflow itself was left paused
   * (not ended) by {@link #fetchBatchStep}, specifically so it can be resumed here instead
   * of started over — starting over would run {@link #initStep} again and recompute
   * {@code maxSubscriberId}/{@code toSend}, which rule 11 forbids. */
  public Effect<Done> resumeSend() {
    if (currentState() == null) {
      return effects().error("send workflow not started");
    }
    return effects()
        .transitionTo(CampaignSendWorkflow::fetchBatchStep)
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<SendState> getState() {
    if (currentState() == null) {
      return effects().error("send workflow not started");
    }
    return effects().reply(currentState());
  }

  /** Rule 6: freezes the checkpoint ceiling and toSend, then starts the campaign. */
  @StepName("init")
  private StepEffect initStep() {
    CampaignState campaign = componentClient.forEventSourcedEntity(currentState().campaignId())
        .method(CampaignEntity::get).invoke();
    ListEntity.State list = componentClient.forKeyValueEntity(campaign.listId())
        .method(ListEntity::get).invoke();

    List<String> statuses = Segmentation.eligibleStatuses(campaign.type(), list.optinType());

    var highest = componentClient.forView().method(SubscriberListView::highestEligible)
        .invoke(new SubscriberListView.ListQuery(campaign.listId(), statuses));
    long maxSubscriberId = highest.subscribers().isEmpty()
        ? 0 : highest.subscribers().get(0).subscriberId();

    var count = componentClient.forView().method(SubscriberListView::eligibleCount)
        .invoke(new SubscriberListView.ListQuery(campaign.listId(), statuses));

    componentClient.forEventSourcedEntity(currentState().campaignId())
        .method(CampaignEntity::start)
        .invoke(new CampaignEntity.Start(maxSubscriberId, (int) count.total()));

    var next = new SendState(
        currentState().campaignId(), campaign.listId(), statuses, maxSubscriberId,
        campaign.batchSize(), campaign.concurrency(), campaign.messageRatePerSecond(),
        campaign.slidingWindowEnabled(), campaign.slidingWindowSeconds(),
        campaign.slidingWindowLimit(), campaign.maxSendErrors(), campaign.fromEmail(),
        campaign.subject(), campaign.body(), RateLimiter.Snapshot.none(), 1);
    return stepEffects().updateState(next).thenTransitionTo(CampaignSendWorkflow::fetchBatchStep);
  }

  /** Rule 7: checks the campaign is still RUNNING (rule 11 — pause/cancel take effect here,
   * between batches, never mid-batch), then fetches the next checkpointed batch. */
  @StepName("fetch-batch")
  private StepEffect fetchBatchStep() {
    CampaignState campaign = componentClient.forEventSourcedEntity(currentState().campaignId())
        .method(CampaignEntity::get).invoke();
    if (campaign.status() == CampaignStatus.PAUSED) {
      // Resumable, not terminal (rule 11) — resumeSend() transitions back here later.
      return stepEffects().thenPause("campaign paused");
    }
    if (campaign.status() != CampaignStatus.RUNNING) {
      return stepEffects().thenEnd();
    }

    var batch = componentClient.forView().method(SubscriberListView::nextBatch).invoke(
        new SubscriberListView.NextBatchQuery(
            currentState().listId(), currentState().statuses(), campaign.lastSubscriberId(),
            currentState().maxSubscriberId(), currentState().batchSize()));

    if (batch.subscribers().isEmpty()) {
      componentClient.forEventSourcedEntity(currentState().campaignId())
          .method(CampaignEntity::finish).invoke();
      return stepEffects().thenEnd();
    }

    return stepEffects()
        .thenTransitionTo(CampaignSendWorkflow::sendBatchStep)
        .withInput(batch);
  }

  /** Rule 8, 9, 10: dispatches the batch under bounded concurrency and a rate limit,
   * records the outcome, then loops back to fetch the next batch.
   *
   * <p>The rate limiter is rebuilt from {@code state.rateLimiterSnapshot()} on every call
   * and its updated snapshot is written back before transitioning — not held as an
   * instance field — because a step invocation is not guaranteed to run on the same Java
   * object as the step before it (found by running the pause/resume integration test: a
   * field-held limiter reset to fresh on every batch and never actually capped anything
   * across the campaign). The durable {@code SendState} is the only thing guaranteed to
   * carry forward.
   *
   * <p>Every value {@link #attemptSend} needs is captured into a local before
   * {@code sendBatch} spawns its worker threads (rather than read from
   * {@code currentState()} on those threads) — {@code currentState()} is only guaranteed
   * to be read safely from the workflow's own step-invoking thread. */
  @StepName("send-batch")
  private StepEffect sendBatchStep(SubscriberListView.Batch batch) {
    SendState state = currentState();
    CampaignState campaign = componentClient.forEventSourcedEntity(state.campaignId())
        .method(CampaignEntity::get).invoke();

    var byId = batch.subscribers().stream()
        .collect(java.util.stream.Collectors.toMap(
            SubscriberListView.SubscriptionEntry::subscriberId, r -> r));

    var rateLimiter = RateLimiter.realTime(
        state.messageRatePerSecond(), state.slidingWindowEnabled(),
        state.slidingWindowSeconds() * 1000L, state.slidingWindowLimit(),
        state.rateLimiterSnapshot());
    var sender = new BoundedSender(state.concurrency(), rateLimiter);

    var outcome = sender.sendBatch(
        batch.subscribers().stream().map(SubscriberListView.SubscriptionEntry::subscriberId).toList(),
        campaign.errorCount(), state.maxSendErrors(),
        id -> attemptSend(state, byId.get(id)));

    componentClient.forEventSourcedEntity(state.campaignId())
        .method(CampaignEntity::recordBatch)
        .invoke(new CampaignEntity.RecordBatch(
            state.nextBatchSeq(), outcome.sent(), outcome.failed(), outcome.lastDispatchedId()));

    return stepEffects()
        .updateState(state.afterBatch(rateLimiter.snapshot()))
        .thenTransitionTo(CampaignSendWorkflow::fetchBatchStep);
  }

  private boolean attemptSend(SendState state, SubscriberListView.SubscriptionEntry row) {
    String unsubscribeUrl = "https://unsubscribe.example/" + state.listId()
        + "/" + row.subscriberId();
    var message = MessageRenderer.render(
        state.fromEmail(), state.subject(), state.body(), row.email(), row.name(),
        unsubscribeUrl);
    try {
      messenger.send(message);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
