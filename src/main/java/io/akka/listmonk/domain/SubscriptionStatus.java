package io.akka.listmonk.domain;

/** A subscription's status, mirroring the source's {@code subscription_status} enum. */
public enum SubscriptionStatus {
  UNCONFIRMED,
  CONFIRMED,
  UNSUBSCRIBED
}
