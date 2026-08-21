package io.akka.listmonk.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * One subscriber's blocklist status and the set of lists they belong to — SPEC-001 §3
 * rule 4.
 *
 * <p>The entity id is the subscriber id. Membership is tracked here, not read back from
 * {@link SubscriberListView}, deliberately: a View is an eventually-consistent projection
 * of {@link SubscriberListEntity}'s events, so a blocklist call immediately following a
 * subscribe call could race the view and see no membership yet — Akka entities, unlike
 * views, are read-your-writes consistent, so routing the fan-out through this entity's own
 * state (updated synchronously by every {@link SubscriptionOperations#subscribe} call)
 * cannot miss a membership that already committed.
 */
@Component(id = "subscriber")
public class SubscriberEntity extends KeyValueEntity<SubscriberEntity.State> {

  public record State(boolean blocklisted, Set<String> listIds) {
    static State empty() {
      return new State(false, Set.of());
    }
  }

  @Override
  public State emptyState() {
    return State.empty();
  }

  /** Records that this subscriber now belongs to {@code listId}; returns the current
   * blocklist status so the caller can create the subscription with the right value. */
  public Effect<Boolean> joinList(String listId) {
    if (currentState().listIds().contains(listId)) {
      return effects().reply(currentState().blocklisted());
    }
    var updated = new LinkedHashSet<>(currentState().listIds());
    updated.add(listId);
    var next = new State(currentState().blocklisted(), Set.copyOf(updated));
    return effects().updateState(next).thenReply(next.blocklisted());
  }

  /** Rule 4: returns the full membership set so the caller can fan the flag out to every
   * existing {@link SubscriberListEntity}. */
  public Effect<State> setBlocklisted(boolean blocklisted) {
    var next = new State(blocklisted, currentState().listIds());
    return effects().updateState(next).thenReply(next);
  }

  public ReadOnlyEffect<State> get() {
    return effects().reply(currentState());
  }
}
