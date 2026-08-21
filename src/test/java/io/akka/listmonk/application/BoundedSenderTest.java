package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rule 8 — real backpressure — checked the same way question-log row 1 checked
 * the source: run it and watch whether admission actually blocks, rather than reading the
 * code and hoping.
 */
public class BoundedSenderTest {

  private static RateLimiter noThrottle() {
    LongSupplier now = () -> 0L;
    LongConsumer sleep = ms -> {};
    return new RateLimiter(Integer.MAX_VALUE, false, 0, 0, RateLimiter.Snapshot.none(), now, sleep);
  }

  @Test
  public void neverExceedsConcurrencyLimit() {
    var inFlight = new AtomicInteger(0);
    var maxObserved = new AtomicInteger(0);
    var sender = new BoundedSender(3, noThrottle());

    List<Long> ids = List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    var outcome = sender.sendBatch(ids, 0, 0, id -> {
      int now = inFlight.incrementAndGet();
      maxObserved.updateAndGet(max -> Math.max(max, now));
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      inFlight.decrementAndGet();
      return true;
    });

    assertThat(maxObserved.get()).isLessThanOrEqualTo(3);
    assertThat(outcome.sent()).isEqualTo(10);
    assertThat(outcome.lastDispatchedId()).isEqualTo(10);
  }

  /** The direct analogue of the source probe (question-log row 1, `TestRealBackpressure`):
   * with every slot occupied by a send that hasn't returned, admitting one more has to
   * block the calling thread until a slot frees, not buffer unboundedly. */
  @Test
  public void admissionBlocksUntilASlotFrees() throws InterruptedException {
    var release = new CountDownLatch(1);
    var started = new CountDownLatch(2);
    var sender = new BoundedSender(2, noThrottle());

    var driver = new Thread(() -> sender.sendBatch(List.of(1L, 2L, 3L), 0, 0, id -> {
      started.countDown();
      if (id <= 2) {
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    }));
    driver.start();

    // The first two sends occupy both concurrency slots and block on `release`.
    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

    // The driver thread must now be stuck admitting the 3rd send -- it cannot still be
    // alive-but-idle; it has to be blocked inside sendBatch, not finished.
    Thread.sleep(300);
    assertThat(driver.isAlive()).isTrue();

    release.countDown();
    driver.join(2000);
    assertThat(driver.isAlive()).isFalse();
  }

  @Test
  public void stopsDispatchingOnceThresholdReachedMidBatch() {
    var attempted = new AtomicInteger(0);
    var sender = new BoundedSender(1, noThrottle()); // concurrency 1 makes order deterministic

    List<Long> ids = List.of(1L, 2L, 3L, 4L, 5L);
    // alreadyErrorCount=1, maxSendErrors=2: one more failure reaches the threshold.
    var outcome = sender.sendBatch(ids, 1, 2, id -> {
      attempted.incrementAndGet();
      return id != 2; // id 2 fails
    });

    assertThat(attempted.get()).isEqualTo(2); // stopped right after the failing send
    assertThat(outcome.sent()).isEqualTo(1);
    assertThat(outcome.failed()).isEqualTo(1);
    assertThat(outcome.skipped()).isEqualTo(3);
    // The skipped ids (3, 4, 5) never advance the checkpoint past the last dispatched one.
    assertThat(outcome.lastDispatchedId()).isEqualTo(2);
  }

  @Test
  public void noThresholdConfiguredNeverStops() {
    var sender = new BoundedSender(2, noThrottle());
    var outcome = sender.sendBatch(List.of(1L, 2L, 3L), 100, 0, id -> false);
    assertThat(outcome.skipped()).isEqualTo(0);
    assertThat(outcome.failed()).isEqualTo(3);
  }
}
