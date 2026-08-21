package io.akka.listmonk.application;

import io.akka.listmonk.domain.OutgoingMessage;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Sends over real SMTP, the same role {@code github.com/knadh/smtppool} plays in the
 * source (`internal/messenger/email/email.go`). One connection per send rather than the
 * source's pooling (SPEC-001 §1 — connection pooling across multiple configured servers
 * is out of scope for this slice).
 */
public final class SmtpMessenger implements Messenger {

  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final boolean startTls;

  public SmtpMessenger(String host, int port, String username, String password,
      boolean startTls) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.startTls = startTls;
  }

  @Override
  public void send(OutgoingMessage message) throws Exception {
    var props = new Properties();
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", String.valueOf(port));
    props.put("mail.smtp.starttls.enable", String.valueOf(startTls));
    props.put("mail.smtp.auth", String.valueOf(!username.isBlank()));

    Session session = username.isBlank()
        ? Session.getInstance(props)
        : Session.getInstance(props, new jakarta.mail.Authenticator() {
            @Override
            protected jakarta.mail.PasswordAuthentication getPasswordAuthentication() {
              return new jakarta.mail.PasswordAuthentication(username, password);
            }
          });

    MimeMessage mime = new MimeMessage(session);
    mime.setFrom(new InternetAddress(message.from()));
    mime.setRecipients(Message.RecipientType.TO, InternetAddress.parse(message.to()));
    mime.setSubject(message.subject());
    mime.setText(message.body());
    mime.setHeader("List-Unsubscribe", "<" + message.unsubscribeUrl() + ">");

    Transport.send(mime);
  }
}
