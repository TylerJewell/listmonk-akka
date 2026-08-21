package io.akka.listmonk.application;

import akka.javasdk.client.ComponentClient;

/**
 * Subscribe / confirm / unsubscribe / blocklist, across the two entities those operations
 * touch — SPEC-001 §3 rules 1-4.
 *
 * <p>A plain orchestrating class, not a component of its own, the same shape honcho's
 * {@code DeriverPipeline} uses: {@link #blocklist} is the one operation that fans out to
 * more than one entity (every subscription the subscriber currently has), found from
 * {@link SubscriberEntity}'s own membership set rather than {@link SubscriberListView} —
 * see {@link SubscriberEntity}'s class doc for why the view would race this.
 */
public final class SubscriptionOperations {

  private final ComponentClient componentClient;

  public SubscriptionOperations(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  private static String subscriptionId(long subscriberId, String listId) {
    return subscriberId + ":" + listId;
  }

  public void subscribe(long subscriberId, String listId, String email, String name) {
    boolean blocklisted = componentClient
        .forKeyValueEntity(Long.toString(subscriberId))
        .method(SubscriberEntity::joinList)
        .invoke(listId);
    // Normalized here, at the one place every subscription is created, rather than left
    // nullable through the entity/view/render chain: a null field on a view row is a
    // known Akka Views rough edge (review checklist E4), and a caller-omitted name is
    // exactly as absent as an explicitly empty one for rendering purposes (rule 13).
    String normalizedName = name == null ? "" : name;
    componentClient
        .forEventSourcedEntity(subscriptionId(subscriberId, listId))
        .method(SubscriberListEntity::subscribe)
        .invoke(new SubscriberListEntity.Subscribe(
            subscriberId, listId, email, normalizedName, blocklisted));
  }

  public void confirmOptin(long subscriberId, String listId) {
    componentClient
        .forEventSourcedEntity(subscriptionId(subscriberId, listId))
        .method(SubscriberListEntity::confirmOptin)
        .invoke();
  }

  public void unsubscribe(long subscriberId, String listId) {
    componentClient
        .forEventSourcedEntity(subscriptionId(subscriberId, listId))
        .method(SubscriberListEntity::unsubscribe)
        .invoke();
  }

  /** Rule 4: sets the subscriber-level flag first, so a subscription created concurrently
   * on another list is created against the value already committed here; then propagates
   * to every subscription that already exists, per the membership set {@code setBlocklisted}
   * returns. */
  public void blocklist(long subscriberId) {
    var state = componentClient
        .forKeyValueEntity(Long.toString(subscriberId))
        .method(SubscriberEntity::setBlocklisted)
        .invoke(true);

    for (var listId : state.listIds()) {
      componentClient
          .forEventSourcedEntity(subscriptionId(subscriberId, listId))
          .method(SubscriberListEntity::setBlocklisted)
          .invoke(true);
    }
  }
}
