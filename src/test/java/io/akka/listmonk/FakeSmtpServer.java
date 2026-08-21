package io.akka.listmonk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal SMTP server accepting one connection per delivery (jakarta.mail's
 * {@code Transport.send} opens and closes a connection per call), so integration tests can
 * point {@link io.akka.listmonk.application.CampaignSendWorkflow} at a real socket and count
 * real deliveries rather than trusting that a send "would have" gone out.
 */
public final class FakeSmtpServer implements AutoCloseable {

  private final ServerSocket serverSocket;
  private final AtomicInteger deliveredCount = new AtomicInteger(0);
  private final ConcurrentLinkedQueue<String> deliveredBodies = new ConcurrentLinkedQueue<>();
  private final java.util.concurrent.ExecutorService pool = Executors.newCachedThreadPool();
  private volatile boolean closed = false;

  public FakeSmtpServer() throws IOException {
    serverSocket = new ServerSocket(0);
    Thread acceptor = new Thread(this::acceptLoop);
    acceptor.setDaemon(true);
    acceptor.start();
  }

  public int port() {
    return serverSocket.getLocalPort();
  }

  public int deliveredCount() {
    return deliveredCount.get();
  }

  public java.util.List<String> deliveredBodies() {
    return java.util.List.copyOf(deliveredBodies);
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket socket = serverSocket.accept();
        pool.submit(() -> handle(socket));
      } catch (IOException e) {
        if (!closed) {
          // keep accepting; a single bad connection should not kill the server
        }
        if (closed) return;
      }
    }
  }

  private void handle(Socket socket) {
    try (socket;
        var out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
        var in = new BufferedReader(
            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
      out.print("220 fake.smtp.test ESMTP\r\n");
      out.flush();
      String line;
      var body = new StringBuilder();
      boolean inData = false;
      while ((line = in.readLine()) != null) {
        if (inData) {
          if (line.equals(".")) {
            inData = false;
            deliveredBodies.add(body.toString());
            deliveredCount.incrementAndGet();
            out.print("250 OK message queued\r\n");
            out.flush();
            continue;
          }
          body.append(line).append("\n");
          continue;
        }
        String upper = line.toUpperCase();
        if (upper.startsWith("EHLO") || upper.startsWith("HELO")) {
          out.print("250-fake.smtp.test\r\n250 OK\r\n");
        } else if (upper.startsWith("DATA")) {
          out.print("354 Start mail input\r\n");
          inData = true;
        } else if (upper.startsWith("QUIT")) {
          out.print("221 Bye\r\n");
          out.flush();
          return;
        } else {
          out.print("250 OK\r\n");
        }
        out.flush();
      }
    } catch (IOException ignored) {
      // connection dropped; nothing to recover
    }
  }

  @Override
  public void close() {
    closed = true;
    try {
      serverSocket.close();
    } catch (IOException ignored) {
      // best-effort shutdown
    }
    pool.shutdownNow();
  }
}
