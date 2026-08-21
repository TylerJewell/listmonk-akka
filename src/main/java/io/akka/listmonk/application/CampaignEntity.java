package io.akka.listmonk.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import io.akka.listmonk.domain.CampaignEvent;
import io.akka.listmonk.domain.CampaignState;
import io.akka.listmonk.domain.CampaignStatus;
import io.akka.listmonk.domain.CampaignType;

/**
 * A campaign's definition, checkpoint and counters — the durable source of truth the
 * {@link CampaignSendWorkflow} drives and checkpoints against, SPEC-001 §3 rules 6, 9-11.
 *
 * <p>Kept as its own entity, addressable independent of the workflow, because an operator
 * inspecting a campaign's status/counts should be able to do so even if the workflow
 * instance driving it has not run recently — the same separation the source keeps between
 * the {@code campaigns} table (durable, queryable) and the in-memory {@code pipe} (ephemeral,
 * only exists while sending).
 */
@Component(id = "campaign")
public class CampaignEntity extends EventSourcedEntity<CampaignState, CampaignEvent> {

  public record Create(
      String name, String subject, String fromEmail, String body, String listId,
      CampaignType type, int batchSize, int concurrency, int messageRatePerSecond,
      boolean slidingWindowEnabled, int slidingWindowSeconds, int slidingWindowLimit,
      int maxSendErrors) {}

  public record Start(long maxSubscriberId, int toSend) {}

  public record RecordBatch(int batchSeq, int sentDelta, int errorDelta, long lastSubscriberId) {}

  @Override
  public CampaignState emptyState() {
    return CampaignState.empty();
  }

  public Effect<Done> create(Create command) {
    if (currentState().exists()) {
      return effects().error("campaign " + commandContext().entityId() + " already exists");
    }
    return effects()
        .persist(new CampaignEvent.CampaignCreated(
            commandContext().entityId(), command.name(), command.subject(),
            command.fromEmail(), command.body(), command.listId(), command.type(),
            command.batchSize(), command.concurrency(), command.messageRatePerSecond(),
            command.slidingWindowEnabled(), command.slidingWindowSeconds(),
            command.slidingWindowLimit(), command.maxSendErrors()))
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 6: accepted only from DRAFT, and freezes the checkpoint ceiling. */
  public Effect<Done> start(Start command) {
    if (currentState().status() != CampaignStatus.DRAFT) {
      return effects().error("campaign is " + currentState().status() + ", not draft");
    }
    return effects()
        .persist(new CampaignEvent.CampaignStarted(command.maxSubscriberId(), command.toSend()))
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 9, 10: records a batch's outcome and auto-pauses if the error threshold is now
   * reached. Accepted from RUNNING, PAUSED or CANCELLED — a batch already dispatched when
   * an operator pause/cancel (or another batch's threshold breach) lands still needs to
   * record what it did (rule 11: a pause doesn't recall a dispatched batch), so this must
   * not reject just because the status changed underneath it. Only rejected once FINISHED,
   * which the send pipeline itself only ever sets after every batch is already recorded.
   *
   * <p>Review checklist N1: {@code command.batchSeq()} is a no-op, not an error, on replay
   * — a duplicate delivery of the same call (e.g. a transient retry at the component-call
   * layer) must not double-count {@code sent}/{@code errorCount}. */
  public Effect<Done> recordBatch(RecordBatch command) {
    if (currentState().status() == CampaignStatus.FINISHED) {
      return effects().error("campaign is " + currentState().status());
    }
    if (currentState().isDuplicateBatch(command.batchSeq())) {
      return effects().reply(Done.getInstance());
    }
    var recorded = new CampaignEvent.BatchRecorded(
        command.batchSeq(), command.sentDelta(), command.errorDelta(),
        command.lastSubscriberId());
    // The auto-pause transition only makes sense coming from RUNNING; recording a batch
    // that lands after the campaign is already PAUSED/CANCELLED must not move it back to
    // PAUSED (that would silently undo an operator's Cancel).
    if (currentState().status() == CampaignStatus.RUNNING) {
      var afterBatch = currentState().apply(recorded);
      if (afterBatch.errorThresholdReached()) {
        return effects()
            .persist(recorded, new CampaignEvent.CampaignPaused("error threshold reached"))
            .thenReply(state -> Done.getInstance());
      }
    }
    return effects().persist(recorded).thenReply(state -> Done.getInstance());
  }

  /** Rule 11: accepted only from RUNNING; does not recall a batch already dispatched. */
  public Effect<Done> pause() {
    if (currentState().status() != CampaignStatus.RUNNING) {
      return effects().error("campaign is " + currentState().status() + ", not running");
    }
    return effects().persist(new CampaignEvent.CampaignPaused("operator pause"))
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 11: accepted only from PAUSED; does not recompute the checkpoint ceiling. */
  public Effect<Done> resume() {
    if (currentState().status() != CampaignStatus.PAUSED) {
      return effects().error("campaign is " + currentState().status() + ", not paused");
    }
    return effects().persist(new CampaignEvent.CampaignResumed())
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 11: accepted only from RUNNING or PAUSED; terminal. */
  public Effect<Done> cancel() {
    if (currentState().status() != CampaignStatus.RUNNING
        && currentState().status() != CampaignStatus.PAUSED) {
      return effects().error("campaign is " + currentState().status() + ", cannot cancel");
    }
    return effects().persist(new CampaignEvent.CampaignCancelled())
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 7 step (c): the send pipeline records FINISHED once a fetch comes back empty. */
  public Effect<Done> finish() {
    if (currentState().status() != CampaignStatus.RUNNING) {
      return effects().error("campaign is " + currentState().status() + ", not running");
    }
    return effects().persist(new CampaignEvent.CampaignFinished())
        .thenReply(state -> Done.getInstance());
  }

  public ReadOnlyEffect<CampaignState> get() {
    if (!currentState().exists()) {
      return effects().error("campaign " + commandContext().entityId() + " not found");
    }
    return effects().reply(currentState());
  }

  @Override
  public CampaignState applyEvent(CampaignEvent event) {
    return currentState().apply(event);
  }
}
