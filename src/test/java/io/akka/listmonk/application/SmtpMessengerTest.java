package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.akka.listmonk.FakeSmtpServer;
import io.akka.listmonk.domain.OutgoingMessage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * "Sends a message" should mean a real network call, checked the same way question-log
 * row 1 checked the source's backpressure claim: run it, against a real socket speaking
 * real SMTP, rather than trusting that a send "would have" gone out.
 */
public class SmtpMessengerTest {

  @Test
  public void sendsOverRealSocketToAFakeSmtpServer() throws Exception {
    try (var server = new FakeSmtpServer()) {
      var messenger = new SmtpMessenger("localhost", server.port(), "", "", false);
      var message = new OutgoingMessage(
          "from@x.test", "to@x.test", "hello", "body text", "https://unsub");

      messenger.send(message);

      await().atMost(5, TimeUnit.SECONDS).until(() -> server.deliveredCount() == 1);
      assertThat(server.deliveredBodies().get(0)).contains("body text");
      assertThat(server.deliveredBodies().get(0)).contains("List-Unsubscribe");
    }
  }
}
