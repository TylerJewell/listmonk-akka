package io.akka.listmonk.application;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dispatches one batch under a hard concurrency cap and a rate limit, and does not return
 * until every dispatched send has completed — SPEC-001 §3 rules 8, 9, 10.
 *
 * <p>The concurrency cap is a {@link Semaphore}: the thread driving {@link #sendBatch}
 * blocks in {@code admission.acquire()} when {@code concurrency} sends are already in
 * flight, exactly the property question-log row 1 found in the source's channel send
 * (a full queue blocks the producer rather than growing or dropping). Waiting for every
 * outstanding permit back before returning is what makes the *next* batch fetch wait for
 * *this* batch's sends to finish (rule 8's "no fetch-ahead").
 */
public final class BoundedSender {

  /** One attempt at sending to one id. Never throws — a failure is a {@code false} return,
   * matching the source's {@code err != nil} branch rather than an exception. */
  public interface Attempt {
    boolean send(long id);
  }

  public record Outcome(int sent, int failed, int skipped, long lastDispatchedId) {}

  private final int concurrency;
  private final RateLimiter rateLimiter;

  public BoundedSender(int concurrency, RateLimiter rateLimiter) {
    this.concurrency = Math.max(concurrency, 1);
    this.rateLimiter = rateLimiter;
  }

  /**
   * @param alreadyErrorCount errors already recorded on the campaign before this batch
   * @param maxSendErrors the threshold; {@code <= 0} means no threshold
   */
  public Outcome sendBatch(List<Long> ids, int alreadyErrorCount, int maxSendErrors,
      Attempt attempt) {
    var admission = new Semaphore(concurrency);
    var pool = Executors.newFixedThreadPool(concurrency);
    var sent = new AtomicInteger();
    var failed = new AtomicInteger();
    var skipped = new AtomicInteger();
    var lastDispatchedId = new AtomicLong(0);
    var thresholdReached = new java.util.concurrent.atomic.AtomicBoolean(false);

    try {
      for (long id : ids) {
        if (thresholdReached.get()) {
          skipped.incrementAndGet();
          continue;
        }
        rateLimiter.acquire();
        admission.acquireUninterruptibly();
        pool.submit(() -> {
          try {
            // Checked again here, not only in the loop above: an item can be admitted
            // (a slot was free) just before a concurrently-running send crosses the
            // threshold. This is the same place the source checks — worker() tests
            // `stopped` immediately before sending, for every message including ones
            // already queued, not once per producer iteration (question-log row 9).
            if (thresholdReached.get()) {
              skipped.incrementAndGet();
              return;
            }
            boolean ok = attempt.send(id);
            lastDispatchedId.updateAndGet(current -> Math.max(current, id));
            if (ok) {
              sent.incrementAndGet();
            } else {
              int failedSoFar = failed.incrementAndGet();
              if (maxSendErrors > 0
                  && alreadyErrorCount + failedSoFar >= maxSendErrors) {
                thresholdReached.set(true);
              }
            }
          } finally {
            admission.release();
          }
        });
      }
      // Wait for every dispatched send to finish: reclaiming all `concurrency` permits
      // is only possible once nothing is still holding one.
      admission.acquireUninterruptibly(concurrency);
    } finally {
      pool.shutdown();
      try {
        pool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    return new Outcome(sent.get(), failed.get(), skipped.get(), lastDispatchedId.get());
  }
}
