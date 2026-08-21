package io.akka.listmonk.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import io.akka.listmonk.domain.SubscriberListEvent;
import io.akka.listmonk.domain.SubscriberListState;

/**
 * One subscriber's relationship to one list — SPEC-001 §3 rules 1-4.
 *
 * <p>The entity id is {@code subscriberId:listId}, the same granularity as the source's
 * {@code subscriber_lists} join row (one row per subscription), which is what lets
 * {@link SubscriberListView} put one row per subscription in its table: an Akka
 * {@code TableUpdater} maps one entity to exactly one row (question-log row 12), so the
 * view's grain has to match the entity's.
 */
@Component(id = "subscriber-list")
public class SubscriberListEntity extends EventSourcedEntity<SubscriberListState, SubscriberListEvent> {

  public record Subscribe(
      long subscriberId, String listId, String email, String name, boolean blocklisted) {}

  @Override
  public SubscriberListState emptyState() {
    return SubscriberListState.empty();
  }

  /** Rule 1: creates the subscription as UNCONFIRMED if none exists; a repeat subscribe is
   * a no-op (idempotent), matching the source's upsert-on-conflict-do-nothing shape. */
  public Effect<Done> subscribe(Subscribe command) {
    if (currentState().exists()) {
      return effects().reply(Done.getInstance());
    }
    return effects()
        .persist(new SubscriberListEvent.Subscribed(
            command.subscriberId(), command.listId(), command.email(), command.name(),
            command.blocklisted()))
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 2: idempotent once already CONFIRMED. Rule 3: a no-op once UNSUBSCRIBED —
   * unsubscribe is terminal, so confirming afterward must not resurrect the subscription. */
  public Effect<Done> confirmOptin() {
    if (!currentState().exists()) {
      return effects().error("subscription " + commandContext().entityId() + " not found");
    }
    if (currentState().status() == io.akka.listmonk.domain.SubscriptionStatus.CONFIRMED
        || currentState().status() == io.akka.listmonk.domain.SubscriptionStatus.UNSUBSCRIBED) {
      return effects().reply(Done.getInstance());
    }
    return effects().persist(new SubscriberListEvent.OptinConfirmed())
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 3: terminal — a repeat unsubscribe is a no-op. */
  public Effect<Done> unsubscribe() {
    if (!currentState().exists()) {
      return effects().error("subscription " + commandContext().entityId() + " not found");
    }
    if (currentState().status() == io.akka.listmonk.domain.SubscriptionStatus.UNSUBSCRIBED) {
      return effects().reply(Done.getInstance());
    }
    return effects().persist(new SubscriberListEvent.Unsubscribed())
        .thenReply(state -> Done.getInstance());
  }

  /** Rule 4: called by {@link SubscriptionOperations} for every existing subscription of a
   * subscriber being blocklisted (or un-blocklisted). */
  public Effect<Done> setBlocklisted(boolean blocklisted) {
    if (!currentState().exists()) {
      return effects().error("subscription " + commandContext().entityId() + " not found");
    }
    if (currentState().blocklisted() == blocklisted) {
      return effects().reply(Done.getInstance());
    }
    return effects().persist(new SubscriberListEvent.BlocklistedSet(blocklisted))
        .thenReply(state -> Done.getInstance());
  }

  public ReadOnlyEffect<SubscriberListState> get() {
    if (!currentState().exists()) {
      return effects().error("subscription " + commandContext().entityId() + " not found");
    }
    return effects().reply(currentState());
  }

  @Override
  public SubscriberListState applyEvent(SubscriberListEvent event) {
    return currentState().apply(event);
  }
}
