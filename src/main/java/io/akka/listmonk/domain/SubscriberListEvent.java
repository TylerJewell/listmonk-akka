package io.akka.listmonk.domain;

import akka.javasdk.annotations.TypeName;

/** Everything that can happen to one (subscriber, list) subscription — SPEC-001 §3 rules 1-4. */
public sealed interface SubscriberListEvent {

  @TypeName("subscribed")
  record Subscribed(long subscriberId, String listId, String email, String name, boolean blocklisted)
      implements SubscriberListEvent {}

  @TypeName("optin-confirmed")
  record OptinConfirmed() implements SubscriberListEvent {}

  @TypeName("unsubscribed")
  record Unsubscribed() implements SubscriberListEvent {}

  @TypeName("blocklisted-set")
  record BlocklistedSet(boolean blocklisted) implements SubscriberListEvent {}
}
