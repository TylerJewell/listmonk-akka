package io.akka.listmonk.application;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Two independent throttles, mirroring the source's two independent mechanisms
 * (question-log rows 7, 8): a per-second cap (source: {@code numMsg >= MessageRate} then
 * sleep a second, per worker) and a longer sliding-window cap (source:
 * {@code slidingCount}/{@code slidingStart}, shared across every campaign). Both throttle
 * by actually blocking the calling thread, not by tracking a number nobody waits on —
 * that is what makes {@link BoundedSender}'s use of this a real rate limit rather than an
 * accounting exercise.
 *
 * <p>{@code now}/{@code sleep} are injectable so a test can prove the throttle blocks for
 * approximately the right real duration without a multi-second sliding window making the
 * test itself slow.
 *
 * <p>{@link #snapshot()}/{@link #resume} exist because this object's own counters are not
 * durable, and the caller's isn't either: an Akka {@code Workflow} step is not guaranteed
 * to run on the same Java object instance as the step before it (found by running the
 * pause/resume integration test — a rate limiter held only as an instance field reset to
 * "fresh" on every batch even though the code never explicitly recreated it, because each
 * step invocation could be a new object). The counters have to round-trip through the
 * workflow's own durable state instead.
 */
public final class RateLimiter {

  private final int perSecondLimit;
  private final boolean slidingWindowEnabled;
  private final long slidingWindowMillis;
  private final int slidingWindowLimit;
  private final LongSupplier now;
  private final LongConsumer sleep;

  private int perSecondCount;
  private long perSecondWindowStart;
  private int slidingCount;
  private long slidingWindowStart;

  public record Snapshot(
      int perSecondCount, long perSecondWindowStart, int slidingCount, long slidingWindowStart) {
    public static Snapshot none() {
      return new Snapshot(0, 0, 0, 0);
    }
  }

  public RateLimiter(int perSecondLimit, boolean slidingWindowEnabled, long slidingWindowMillis,
      int slidingWindowLimit, Snapshot snapshot, LongSupplier now, LongConsumer sleep) {
    this.perSecondLimit = Math.max(perSecondLimit, 1);
    this.slidingWindowEnabled = slidingWindowEnabled;
    this.slidingWindowMillis = slidingWindowMillis;
    this.slidingWindowLimit = slidingWindowLimit;
    this.now = now;
    this.sleep = sleep;
    long start = now.getAsLong();
    this.perSecondCount = snapshot.perSecondCount();
    this.perSecondWindowStart = snapshot.perSecondWindowStart() == 0
        ? start : snapshot.perSecondWindowStart();
    this.slidingCount = snapshot.slidingCount();
    this.slidingWindowStart = snapshot.slidingWindowStart() == 0
        ? start : snapshot.slidingWindowStart();
  }

  public static RateLimiter realTime(int perSecondLimit, boolean slidingWindowEnabled,
      long slidingWindowMillis, int slidingWindowLimit, Snapshot snapshot) {
    return new RateLimiter(perSecondLimit, slidingWindowEnabled, slidingWindowMillis,
        slidingWindowLimit, snapshot, System::currentTimeMillis, RateLimiter::sleepMillis);
  }

  private static void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Blocks the caller until admitting one more send would not exceed either throttle. */
  public synchronized void acquire() {
    acquirePerSecond();
    if (slidingWindowEnabled && slidingWindowLimit > 0 && slidingWindowMillis > 1000) {
      acquireSlidingWindow();
    }
  }

  public synchronized Snapshot snapshot() {
    return new Snapshot(perSecondCount, perSecondWindowStart, slidingCount, slidingWindowStart);
  }

  private void acquirePerSecond() {
    long elapsed = now.getAsLong() - perSecondWindowStart;
    if (elapsed >= 1000) {
      perSecondWindowStart = now.getAsLong();
      perSecondCount = 0;
    }
    if (perSecondCount >= perSecondLimit) {
      sleep.accept(1000);
      perSecondWindowStart = now.getAsLong();
      perSecondCount = 0;
    }
    perSecondCount++;
  }

  private void acquireSlidingWindow() {
    long diff = now.getAsLong() - slidingWindowStart;
    if (diff >= slidingWindowMillis) {
      slidingWindowStart = now.getAsLong();
      slidingCount = 0;
    }
    slidingCount++;
    if (slidingCount >= slidingWindowLimit) {
      long wait = slidingWindowMillis - diff;
      if (wait > 0) {
        sleep.accept(wait);
      }
      slidingCount = 0;
      slidingWindowStart = now.getAsLong();
    }
  }
}
