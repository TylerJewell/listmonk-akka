package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rule 8 (rate limiting half); question-log rows 7, 8. */
public class RateLimiterTest {

  /** A controllable clock: {@code sleep} advances it by exactly the requested amount,
   * so the limiter's own math (not wall-clock jitter) is what the assertions check. */
  private static final class FakeClock {
    long now = 0;
    final List<Long> sleeps = new ArrayList<>();

    long nowMillis() {
      return now;
    }

    void sleep(long millis) {
      sleeps.add(millis);
      now += millis;
    }
  }

  @Test
  public void capsStartsPerSecond() {
    var clock = new FakeClock();
    var limiter = new RateLimiter(3, false, 0, 0, RateLimiter.Snapshot.none(), clock::nowMillis, clock::sleep);

    limiter.acquire();
    limiter.acquire();
    limiter.acquire();
    assertThat(clock.sleeps).isEmpty();

    // The 4th acquire in the same one-second window has to wait for the window to roll.
    limiter.acquire();
    assertThat(clock.sleeps).containsExactly(1000L);
  }

  @Test
  public void perSecondWindowResetsOnceAWholeSecondHasPassed() {
    var clock = new FakeClock();
    var limiter = new RateLimiter(2, false, 0, 0, RateLimiter.Snapshot.none(), clock::nowMillis, clock::sleep);

    limiter.acquire();
    limiter.acquire();
    clock.now += 1000; // a full second elapses without going through acquire()
    limiter.acquire(); // starts a fresh window, no wait
    assertThat(clock.sleeps).isEmpty();
  }

  @Test
  public void slidingWindowCapsOverLongerSpan() {
    var clock = new FakeClock();
    // No per-second cap in play (set high) so only the sliding window throttles.
    var limiter = new RateLimiter(1000, true, 5000, 3, RateLimiter.Snapshot.none(), clock::nowMillis, clock::sleep);

    limiter.acquire();
    limiter.acquire();
    assertThat(clock.sleeps).isEmpty();

    // The 3rd acquire within the 5-second, 3-message window has to wait out the remainder
    // of the window (mirrors the source's slidingCount reaching SlidingWindowRate, which
    // triggers the wait on the same message that crosses the limit, not the one after).
    limiter.acquire();
    assertThat(clock.sleeps).hasSize(1);
    assertThat(clock.sleeps.get(0)).isLessThanOrEqualTo(5000L).isGreaterThan(0L);
  }

  @Test
  public void snapshotAndResumeCarriesTheCapAcrossANewInstance() {
    var clock = new FakeClock();
    var first = new RateLimiter(2, false, 0, 0, RateLimiter.Snapshot.none(),
        clock::nowMillis, clock::sleep);
    first.acquire();
    first.acquire();
    assertThat(clock.sleeps).isEmpty();

    // A brand-new instance, as a fresh workflow step invocation would construct, restored
    // from the first instance's snapshot -- this is the fix for the bug the pause/resume
    // integration test caught: a rate limiter that starts over every step never actually
    // caps anything across a whole campaign.
    var second = new RateLimiter(2, false, 0, 0, first.snapshot(), clock::nowMillis, clock::sleep);
    second.acquire();
    assertThat(clock.sleeps).containsExactly(1000L);
  }

  @Test
  public void disabledSlidingWindowNeverWaits() {
    var clock = new FakeClock();
    var limiter = new RateLimiter(1000, false, 5000, 1, RateLimiter.Snapshot.none(), clock::nowMillis, clock::sleep);

    for (int i = 0; i < 10; i++) {
      limiter.acquire();
    }
    assertThat(clock.sleeps).isEmpty();
  }
}
