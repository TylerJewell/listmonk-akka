package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.listmonk.domain.CampaignEvent;
import io.akka.listmonk.domain.CampaignState;
import io.akka.listmonk.domain.CampaignStatus;
import io.akka.listmonk.domain.CampaignType;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 6, 9, 10, 11. */
public class CampaignEntityTest {

  private EventSourcedTestKit<CampaignState, CampaignEvent, CampaignEntity> kit() {
    return EventSourcedTestKit.of("camp-1", CampaignEntity::new);
  }

  private void create(EventSourcedTestKit<CampaignState, CampaignEvent, CampaignEntity> kit) {
    kit.method(CampaignEntity::create).invoke(new CampaignEntity.Create(
        "n", "s", "a@b.com", "body", "list-a", CampaignType.REGULAR,
        50, 4, 10, false, 0, 0, 3));
  }

  @Test
  public void startCapturesCheckpointCeilingOnce() {
    var kit = kit();
    create(kit);
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    var state = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(state.status()).isEqualTo(CampaignStatus.RUNNING);
    assertThat(state.maxSubscriberId()).isEqualTo(100);
    assertThat(state.toSend()).isEqualTo(42);
  }

  @Test
  public void startRejectedUnlessDraft() {
    var kit = kit();
    create(kit);
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    var result = kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(200, 99));
    assertThat(result.isError()).isTrue();
    // The checkpoint ceiling from the first Start is untouched by the rejected second one.
    var state = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(state.maxSubscriberId()).isEqualTo(100);
  }

  @Test
  public void recordBatchAccumulatesCounts() {
    var kit = kit();
    create(kit);
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(1, 5, 1, 10));
    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(2, 3, 0, 25));

    var state = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(state.sent()).isEqualTo(8);
    assertThat(state.errorCount()).isEqualTo(1);
    assertThat(state.lastSubscriberId()).isEqualTo(25);
  }

  @Test
  public void duplicateBatchSeqIsANoOp() {
    var kit = kit();
    create(kit);
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(1, 5, 1, 10));
    // Review checklist N1: the same batchSeq delivered again (e.g. a transient retry of
    // the recordBatch call itself) must not double-count sent/errorCount.
    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(1, 5, 1, 10));

    var state = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(state.sent()).isEqualTo(5);
    assertThat(state.errorCount()).isEqualTo(1);
  }

  @Test
  public void checkpointAdvancesToHighestDispatchedId() {
    var kit = kit();
    create(kit);
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(1, 2, 0, 30));
    // A batch reporting a lower last-dispatched id (e.g. every send in it was skipped, id 0)
    // never moves the checkpoint backward.
    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(2, 0, 0, 0));

    var state = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(state.lastSubscriberId()).isEqualTo(30);
  }

  @Test
  public void thresholdBreachPausesCampaign() {
    var kit = kit();
    create(kit); // maxSendErrors = 3
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    kit.method(CampaignEntity::recordBatch).invoke(new CampaignEntity.RecordBatch(1, 5, 2, 10));
    var result = kit.method(CampaignEntity::recordBatch)
        .invoke(new CampaignEntity.RecordBatch(2, 1, 1, 15));

    var state = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(state.errorCount()).isEqualTo(3);
    assertThat(state.status()).isEqualTo(CampaignStatus.PAUSED);
  }

  @Test
  public void pauseCancelResumeTransitions() {
    var kit = kit();
    create(kit);
    kit.method(CampaignEntity::start).invoke(new CampaignEntity.Start(100, 42));

    kit.method(CampaignEntity::pause).invoke();
    assertThat(kit.method(CampaignEntity::get).invoke().getReply().status())
        .isEqualTo(CampaignStatus.PAUSED);

    // Resume does not recompute the checkpoint ceiling.
    kit.method(CampaignEntity::resume).invoke();
    var afterResume = kit.method(CampaignEntity::get).invoke().getReply();
    assertThat(afterResume.status()).isEqualTo(CampaignStatus.RUNNING);
    assertThat(afterResume.maxSubscriberId()).isEqualTo(100);

    kit.method(CampaignEntity::cancel).invoke();
    assertThat(kit.method(CampaignEntity::get).invoke().getReply().status())
        .isEqualTo(CampaignStatus.CANCELLED);

    // Cancel is terminal: neither resume nor pause is accepted afterward.
    assertThat(kit.method(CampaignEntity::resume).invoke().isError()).isTrue();
    assertThat(kit.method(CampaignEntity::pause).invoke().isError()).isTrue();
  }

  @Test
  public void finishRejectedUnlessRunning() {
    var kit = kit();
    create(kit);
    assertThat(kit.method(CampaignEntity::finish).invoke().isError()).isTrue();
  }
}
