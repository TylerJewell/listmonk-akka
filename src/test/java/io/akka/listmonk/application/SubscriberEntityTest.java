package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.KeyValueEntityTestKit;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rule 4 — review checklist O2. */
public class SubscriberEntityTest {

  private static KeyValueEntityTestKit<SubscriberEntity.State, SubscriberEntity> kit() {
    return KeyValueEntityTestKit.of("1", SubscriberEntity::new);
  }

  @Test
  public void newSubscriberIsNotBlocklisted() {
    var kit = kit();
    assertThat(kit.method(SubscriberEntity::get).invoke().getReply().blocklisted()).isFalse();
  }

  @Test
  public void joinListRecordsMembershipAndReturnsCurrentBlocklistStatus() {
    var kit = kit();
    var reply = kit.method(SubscriberEntity::joinList).invoke("list-a").getReply();
    assertThat(reply).isFalse();

    var state = kit.method(SubscriberEntity::get).invoke().getReply();
    assertThat(state.listIds()).containsExactly("list-a");
  }

  @Test
  public void joiningTheSameListTwiceDoesNotDuplicateMembership() {
    var kit = kit();
    kit.method(SubscriberEntity::joinList).invoke("list-a");
    kit.method(SubscriberEntity::joinList).invoke("list-a");

    var state = kit.method(SubscriberEntity::get).invoke().getReply();
    assertThat(state.listIds()).containsExactly("list-a");
  }

  @Test
  public void setBlocklistedReturnsFullMembershipForFanOut() {
    var kit = kit();
    kit.method(SubscriberEntity::joinList).invoke("list-a");
    kit.method(SubscriberEntity::joinList).invoke("list-b");

    var reply = kit.method(SubscriberEntity::setBlocklisted).invoke(true).getReply();
    assertThat(reply.blocklisted()).isTrue();
    assertThat(reply.listIds()).containsExactlyInAnyOrder("list-a", "list-b");
  }

  @Test
  public void joiningAfterBlocklistedCarriesTheFlag() {
    var kit = kit();
    kit.method(SubscriberEntity::setBlocklisted).invoke(true);

    var blocklistedAtJoin = kit.method(SubscriberEntity::joinList).invoke("list-a").getReply();
    assertThat(blocklistedAtJoin).isTrue();
  }
}
