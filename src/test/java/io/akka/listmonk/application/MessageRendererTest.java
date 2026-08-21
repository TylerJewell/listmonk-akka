package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 12, 13. */
public class MessageRendererTest {

  @Test
  public void substitutesEmailAndName() {
    var msg = MessageRenderer.render(
        "from@x.test", "Hi {{name}}", "Your email is {{email}}", "to@x.test", "Ada",
        "https://unsub");
    assertThat(msg.subject()).isEqualTo("Hi Ada");
    assertThat(msg.body()).isEqualTo("Your email is to@x.test");
  }

  @Test
  public void leavesUnknownPlaceholdersVerbatim() {
    var msg = MessageRenderer.render(
        "from@x.test", "s", "Track {{ TrackLink \"https://x\" }}", "to@x.test", "Ada",
        "https://unsub");
    assertThat(msg.body()).isEqualTo("Track {{ TrackLink \"https://x\" }}");
  }

  @Test
  public void includesUnsubscribeLink() {
    var msg = MessageRenderer.render(
        "from@x.test", "s", "b", "to@x.test", "Ada", "https://unsub/list-a/1");
    assertThat(msg.unsubscribeUrl()).isEqualTo("https://unsub/list-a/1");
  }

  @Test
  public void nullNameSubstitutesEmptyString() {
    var msg = MessageRenderer.render(
        "from@x.test", "Hi {{name}}", "b", "to@x.test", null, "https://unsub");
    assertThat(msg.subject()).isEqualTo("Hi ");
  }
}
