package io.akka.listmonk.domain;

/**
 * One subscriber's relationship to one list — SPEC-001 §2 "Subscription", §3 rules 1-4.
 *
 * <p>{@code blocklisted} is denormalized here (not looked up from a separate entity at read
 * time) because the segmentation view (question-log row 5) has to filter on it without a
 * join — Akka Views have no cross-entity join, so the fact has to already be on the row.
 */
public record SubscriberListState(
    long subscriberId, String listId, String email, String name, SubscriptionStatus status,
    boolean blocklisted) {

  public static SubscriberListState empty() {
    return new SubscriberListState(0, null, null, null, null, false);
  }

  public boolean exists() {
    return listId != null;
  }

  public SubscriberListState apply(SubscriberListEvent event) {
    return switch (event) {
      case SubscriberListEvent.Subscribed e -> new SubscriberListState(
          e.subscriberId(), e.listId(), e.email(), e.name(), SubscriptionStatus.UNCONFIRMED,
          e.blocklisted());
      case SubscriberListEvent.OptinConfirmed e -> new SubscriberListState(
          subscriberId, listId, email, name, SubscriptionStatus.CONFIRMED, blocklisted);
      case SubscriberListEvent.Unsubscribed e -> new SubscriberListState(
          subscriberId, listId, email, name, SubscriptionStatus.UNSUBSCRIBED, blocklisted);
      case SubscriberListEvent.BlocklistedSet e ->
          new SubscriberListState(subscriberId, listId, email, name, status, e.blocklisted());
    };
  }
}
