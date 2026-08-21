package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.listmonk.domain.SubscriberListEvent;
import io.akka.listmonk.domain.SubscriberListState;
import io.akka.listmonk.domain.SubscriptionStatus;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 1-4. */
public class SubscriberListEntityTest {

  private EventSourcedTestKit<SubscriberListState, SubscriberListEvent, SubscriberListEntity> kit() {
    return EventSourcedTestKit.of("1:list-a", SubscriberListEntity::new);
  }

  @Test
  public void subscribingCreatesUnconfirmed() {
    var kit = kit();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", false));

    var state = kit.method(SubscriberListEntity::get).invoke().getReply();
    assertThat(state.status()).isEqualTo(SubscriptionStatus.UNCONFIRMED);
    assertThat(state.email()).isEqualTo("a@x.test");
    assertThat(state.blocklisted()).isFalse();
  }

  @Test
  public void repeatSubscribeIsANoOp() {
    var kit = kit();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", false));
    kit.method(SubscriberListEntity::confirmOptin).invoke();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", false));

    // A repeat subscribe does not reset an already-confirmed subscription back to unconfirmed.
    var state = kit.method(SubscriberListEntity::get).invoke().getReply();
    assertThat(state.status()).isEqualTo(SubscriptionStatus.CONFIRMED);
  }

  @Test
  public void confirmIsIdempotent() {
    var kit = kit();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", false));
    kit.method(SubscriberListEntity::confirmOptin).invoke();
    kit.method(SubscriberListEntity::confirmOptin).invoke();

    var state = kit.method(SubscriberListEntity::get).invoke().getReply();
    assertThat(state.status()).isEqualTo(SubscriptionStatus.CONFIRMED);
  }

  @Test
  public void unsubscribeIsTerminal() {
    var kit = kit();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", false));
    kit.method(SubscriberListEntity::unsubscribe).invoke();
    kit.method(SubscriberListEntity::confirmOptin).invoke();

    var state = kit.method(SubscriberListEntity::get).invoke().getReply();
    // Confirming after unsubscribe does not resurrect the subscription.
    assertThat(state.status()).isEqualTo(SubscriptionStatus.UNSUBSCRIBED);
  }

  @Test
  public void blocklistCarriesToNewSubscription() {
    var kit = kit();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", true));

    var state = kit.method(SubscriberListEntity::get).invoke().getReply();
    assertThat(state.blocklisted()).isTrue();
  }

  @Test
  public void setBlocklistedTogglesWithoutChangingStatus() {
    var kit = kit();
    kit.method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(1, "list-a", "a@x.test", "A", false));
    kit.method(SubscriberListEntity::confirmOptin).invoke();
    kit.method(SubscriberListEntity::setBlocklisted).invoke(true);

    var state = kit.method(SubscriberListEntity::get).invoke().getReply();
    assertThat(state.blocklisted()).isTrue();
    assertThat(state.status()).isEqualTo(SubscriptionStatus.CONFIRMED);
  }
}
