package io.akka.listmonk.application;

import io.akka.listmonk.domain.OutgoingMessage;

/**
 * Renders one campaign message for one subscriber — SPEC-001 §3 rules 12, 13.
 *
 * <p>Substitutes only {@code {{email}}} and {@code {{name}}}; any other {@code {{...}}}
 * placeholder is left verbatim (§4 decision 3 — not compatible with the source's
 * Go {@code html/template} + sprig engine, which is out of scope for this slice).
 */
public final class MessageRenderer {

  private MessageRenderer() {}

  public static OutgoingMessage render(String fromEmail, String subject, String body,
      String toEmail, String subscriberName, String unsubscribeUrl) {
    return new OutgoingMessage(
        fromEmail, toEmail, substitute(subject, toEmail, subscriberName),
        substitute(body, toEmail, subscriberName), unsubscribeUrl);
  }

  private static String substitute(String template, String email, String name) {
    return template.replace("{{email}}", email).replace("{{name}}", name == null ? "" : name);
  }
}
