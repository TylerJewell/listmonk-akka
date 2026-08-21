package io.akka.listmonk.application;

import io.akka.listmonk.domain.OutgoingMessage;

/** A messaging backend, mirroring the source's {@code Messenger} interface. */
public interface Messenger {
  void send(OutgoingMessage message) throws Exception;
}
