package io.akka.listmonk.domain;

/** One rendered message ready to hand to a {@link io.akka.listmonk.application.Messenger}. */
public record OutgoingMessage(String from, String to, String subject, String body,
    String unsubscribeUrl) {}
