package io.akka.listmonk;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKit;
import akka.javasdk.testkit.TestKitSupport;
import com.typesafe.config.ConfigFactory;
import io.akka.listmonk.api.CampaignEndpoint;
import io.akka.listmonk.domain.CampaignState;
import io.akka.listmonk.domain.CampaignStatus;
import io.akka.listmonk.domain.CampaignType;
import io.akka.listmonk.domain.OptinType;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rule 11 (pause/resume), against a real running pipeline with a small enough
 * batch size (set via config for this class only) that a run has several batches — pausing
 * after the first one lands the pause between batches, the only place rule 11 allows it to
 * take effect.
 */
public class CampaignPauseResumeIntegrationTest extends TestKitSupport {

  private static FakeSmtpServer smtpServer;

  static {
    try {
      smtpServer = new FakeSmtpServer();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected TestKit.Settings testKitSettings() {
    return TestKit.Settings.DEFAULT.withAdditionalConfig(ConfigFactory.parseString(
        "listmonk.smtp.host = localhost\n"
            + "listmonk.smtp.port = " + smtpServer.port() + "\n"
            + "listmonk.send.default-batch-size = 2\n"
            // Slow enough that a real send + record round trip per batch gives the test
            // room to observe an intermediate state and issue the pause before the whole
            // (small) campaign finishes on its own.
            + "listmonk.send.default-message-rate-per-second = 2\n"));
  }

  private String id() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  public void pauseStopsBetweenBatchesThenResumeFinishes() {
    var listId = "list-" + id();
    httpClient.POST("/lists/" + listId)
        .withRequestBody(new CampaignEndpoint.CreateListBody("test list", OptinType.SINGLE))
        .invoke();
    for (long i = 1; i <= 10; i++) {
      httpClient.POST("/lists/" + listId + "/subscribers/" + i)
          .withRequestBody(new CampaignEndpoint.SubscribeBody("s" + i + "@x.test", "N" + i))
          .invoke();
    }
    // SPEC-001 §4 decision 6: wait for the segmentation view to catch up with all ten
    // subscriptions before starting, or Start could freeze a checkpoint ceiling below 10.
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      var preview = httpClient.GET("/lists/" + listId + "/eligible-count/" + CampaignType.REGULAR)
          .responseBodyAs(CampaignEndpoint.EligiblePreview.class).invoke().body();
      assertThat(preview.count()).isEqualTo(10);
    });

    var campaignId = "camp-" + id();
    httpClient.POST("/campaigns/" + campaignId)
        .withRequestBody(new CampaignEndpoint.CreateCampaignBody(
            "c", "s", "from@x.test", "b", listId, CampaignType.REGULAR))
        .invoke();
    httpClient.POST("/campaigns/" + campaignId + "/start").invoke();

    // Wait until at least one full batch (of 2) has landed, then pause immediately.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> campaignState(campaignId).sent() >= 2);
    httpClient.POST("/campaigns/" + campaignId + "/pause").invoke();

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
        .until(() -> campaignState(campaignId).status() == CampaignStatus.PAUSED);

    var paused = campaignState(campaignId);
    // Batches are dispatched whole (rule 8) and only recorded once fully dispatched (rule 9),
    // so a pause landing between batches always leaves `sent` a multiple of the batch size.
    assertThat(paused.sent() % 2).isEqualTo(0);
    assertThat(paused.sent()).isLessThan(10);

    httpClient.POST("/campaigns/" + campaignId + "/resume").invoke();

    Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      var campaign = campaignState(campaignId);
      assertThat(campaign.status()).isEqualTo(CampaignStatus.FINISHED);
      assertThat(campaign.sent()).isEqualTo(10);
      // Resume did not recompute the checkpoint ceiling captured at the original Start.
      assertThat(campaign.maxSubscriberId()).isEqualTo(10);
    });
  }

  private CampaignState campaignState(String campaignId) {
    return httpClient.GET("/campaigns/" + campaignId)
        .responseBodyAs(CampaignState.class).invoke().body();
  }
}
