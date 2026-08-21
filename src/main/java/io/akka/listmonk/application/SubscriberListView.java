package io.akka.listmonk.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.listmonk.domain.SubscriberListEvent;
import io.akka.listmonk.domain.SubscriberListState;
import java.util.List;

/**
 * Segmentation — SPEC-001 §3 rule 5 — and the checkpoint queries rule 6 needs at campaign
 * start, all built from {@link SubscriberListEntity}'s events so that one row here is
 * exactly one subscription (question-log row 12).
 */
@Component(id = "subscriber-list-view")
public class SubscriberListView extends View {

  public record SubscriptionEntry(long subscriberId, String listId, String email, String name, String status,
      boolean blocklisted) {}

  public record Batch(List<SubscriptionEntry> subscribers) {}

  public record NextBatchQuery(
      String listId, List<String> statuses, long cursor, long maxSubscriberId, int limit) {}

  public record ListQuery(String listId, List<String> statuses) {}

  public record Count(long total) {}

  @Consume.FromEventSourcedEntity(SubscriberListEntity.class)
  public static class Updater extends TableUpdater<SubscriptionEntry> {
    public Effect<SubscriptionEntry> onEvent(SubscriberListEvent event) {
      SubscriptionEntry current = rowState();
      return switch (event) {
        case SubscriberListEvent.Subscribed e -> effects().updateRow(
            new SubscriptionEntry(e.subscriberId(), e.listId(), e.email(), e.name(), "UNCONFIRMED",
                e.blocklisted()));
        case SubscriberListEvent.OptinConfirmed e -> effects().updateRow(
            new SubscriptionEntry(current.subscriberId(), current.listId(), current.email(), current.name(),
                "CONFIRMED", current.blocklisted()));
        case SubscriberListEvent.Unsubscribed e -> effects().updateRow(
            new SubscriptionEntry(current.subscriberId(), current.listId(), current.email(), current.name(),
                "UNSUBSCRIBED", current.blocklisted()));
        case SubscriberListEvent.BlocklistedSet e -> effects().updateRow(
            new SubscriptionEntry(current.subscriberId(), current.listId(), current.email(), current.name(),
                current.status(), e.blocklisted()));
      };
    }
  }

  /** Rule 7 step (b): the next checkpointed, segmented batch. */
  @Query("""
      SELECT * AS subscribers FROM subscriber_lists
      WHERE listId = :listId AND status = ANY(:statuses) AND blocklisted = false
        AND subscriberId > :cursor AND subscriberId <= :maxSubscriberId
      ORDER BY subscriberId
      LIMIT :limit
      """)
  public QueryEffect<Batch> nextBatch(NextBatchQuery query) {
    return queryResult();
  }

  /** Rule 6: the checkpoint ceiling, captured once at Start — the highest eligible
   * subscriber id at this instant. */
  @Query("""
      SELECT * AS subscribers FROM subscriber_lists
      WHERE listId = :listId AND status = ANY(:statuses) AND blocklisted = false
      ORDER BY subscriberId DESC
      LIMIT 1
      """)
  public QueryEffect<Batch> highestEligible(ListQuery query) {
    return queryResult();
  }

  /** Rule 6: toSend, captured once at Start. */
  @Query("""
      SELECT count(*) AS total FROM subscriber_lists
      WHERE listId = :listId AND status = ANY(:statuses) AND blocklisted = false
      """)
  public QueryEffect<Count> eligibleCount(ListQuery query) {
    return queryResult();
  }
}
