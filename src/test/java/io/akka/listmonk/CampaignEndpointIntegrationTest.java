package io.akka.listmonk;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKit;
import akka.javasdk.testkit.TestKitSupport;
import com.typesafe.config.ConfigFactory;
import io.akka.listmonk.api.CampaignEndpoint;
import io.akka.listmonk.domain.CampaignStatus;
import io.akka.listmonk.domain.CampaignType;
import io.akka.listmonk.domain.OptinType;
import io.akka.listmonk.domain.SubscriptionStatus;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001, end-to-end against a real running Akka runtime rather than the state-machine
 * tests alone: create a list, subscribe a segmented mix of subscribers, start a campaign,
 * and check that exactly the eligible ones are actually delivered to over a real SMTP
 * socket — segmentation (rule 5) and the send pipeline (rules 6-11) together, not each in
 * isolation.
 */
public class CampaignEndpointIntegrationTest extends TestKitSupport {

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
        "listmonk.smtp.host = localhost\nlistmonk.smtp.port = " + smtpServer.port()));
  }

  private String id() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  /** SPEC-001 §4 decision 6: the segmentation view lags its source entities, so a test
   * that subscribes and then immediately starts a campaign has to wait for the view to
   * catch up first, the same way any caller relying on a fresh subscription being counted
   * would have to. */
  private void awaitEligibleCount(String listId, CampaignType type, long expected) {
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      var preview = httpClient.GET("/lists/" + listId + "/eligible-count/" + type)
          .responseBodyAs(CampaignEndpoint.EligiblePreview.class).invoke().body();
      assertThat(preview.count()).isEqualTo(expected);
    });
  }

  @Test
  public void endToEndSingleListRegularCampaign() {
    var listId = "list-" + id();
    httpClient.POST("/lists/" + listId)
        .withRequestBody(new CampaignEndpoint.CreateListBody("test list", OptinType.SINGLE))
        .invoke();

    // 1: unconfirmed (eligible), 2: confirmed (eligible), 3: unsubscribed (not eligible).
    httpClient.POST("/lists/" + listId + "/subscribers/1")
        .withRequestBody(new CampaignEndpoint.SubscribeBody("s1@x.test", "One"))
        .invoke();
    httpClient.POST("/lists/" + listId + "/subscribers/2")
        .withRequestBody(new CampaignEndpoint.SubscribeBody("s2@x.test", "Two"))
        .invoke();
    httpClient.POST("/lists/" + listId + "/subscribers/2/confirm").invoke();
    httpClient.POST("/lists/" + listId + "/subscribers/3")
        .withRequestBody(new CampaignEndpoint.SubscribeBody("s3@x.test", "Three"))
        .invoke();
    httpClient.POST("/lists/" + listId + "/subscribers/3/unsubscribe").invoke();
    awaitEligibleCount(listId, CampaignType.REGULAR, 2);

    var campaignId = "camp-" + id();
    httpClient.POST("/campaigns/" + campaignId)
        .withRequestBody(new CampaignEndpoint.CreateCampaignBody(
            "test campaign", "hello {{name}}", "from@x.test", "body for {{email}}", listId,
            CampaignType.REGULAR))
        .invoke();

    int deliveredBefore = smtpServer.deliveredCount();
    httpClient.POST("/campaigns/" + campaignId + "/start").invoke();

    Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      var campaign = httpClient.GET("/campaigns/" + campaignId)
          .responseBodyAs(io.akka.listmonk.domain.CampaignState.class).invoke().body();
      assertThat(campaign.status()).isEqualTo(CampaignStatus.FINISHED);
    });

    var campaign = httpClient.GET("/campaigns/" + campaignId)
        .responseBodyAs(io.akka.listmonk.domain.CampaignState.class).invoke().body();
    assertThat(campaign.toSend()).isEqualTo(2);
    assertThat(campaign.sent()).isEqualTo(2);
    assertThat(smtpServer.deliveredCount() - deliveredBefore).isEqualTo(2);
    assertThat(smtpServer.deliveredBodies()).anyMatch(b -> b.contains("body for s1@x.test"));
    assertThat(smtpServer.deliveredBodies()).anyMatch(b -> b.contains("body for s2@x.test"));
    assertThat(smtpServer.deliveredBodies()).noneMatch(b -> b.contains("s3@x.test"));
  }

  @Test
  public void subscriberAddedAfterStartIsNotSent() {
    var listId = "list-" + id();
    httpClient.POST("/lists/" + listId)
        .withRequestBody(new CampaignEndpoint.CreateListBody("test list", OptinType.SINGLE))
        .invoke();
    httpClient.POST("/lists/" + listId + "/subscribers/10")
        .withRequestBody(new CampaignEndpoint.SubscribeBody("s10@x.test", "Ten"))
        .invoke();
    awaitEligibleCount(listId, CampaignType.REGULAR, 1);

    var campaignId = "camp-" + id();
    httpClient.POST("/campaigns/" + campaignId)
        .withRequestBody(new CampaignEndpoint.CreateCampaignBody(
            "c", "s", "from@x.test", "b", listId, CampaignType.REGULAR))
        .invoke();

    int deliveredBefore = smtpServer.deliveredCount();
    httpClient.POST("/campaigns/" + campaignId + "/start").invoke();

    // A subscriber added immediately after start must not be picked up by this run (rule 6).
    httpClient.POST("/lists/" + listId + "/subscribers/11")
        .withRequestBody(new CampaignEndpoint.SubscribeBody("s11@x.test", "Eleven"))
        .invoke();

    Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      var campaign = httpClient.GET("/campaigns/" + campaignId)
          .responseBodyAs(io.akka.listmonk.domain.CampaignState.class).invoke().body();
      assertThat(campaign.status()).isEqualTo(CampaignStatus.FINISHED);
    });

    assertThat(smtpServer.deliveredCount() - deliveredBefore).isEqualTo(1);
    assertThat(smtpServer.deliveredBodies()).noneMatch(b -> b.contains("s11@x.test"));
  }

  @Test
  public void blocklistedSubscriberIsNeverSelected() {
    var listId = "list-" + id();
    httpClient.POST("/lists/" + listId)
        .withRequestBody(new CampaignEndpoint.CreateListBody("test list", OptinType.SINGLE))
        .invoke();
    httpClient.POST("/lists/" + listId + "/subscribers/30")
        .withRequestBody(new CampaignEndpoint.SubscribeBody("s30@x.test", "Thirty"))
        .invoke();
    httpClient.POST("/subscribers/30/blocklist").invoke();

    var subscription = httpClient.GET("/lists/" + listId + "/subscribers/30")
        .responseBodyAs(io.akka.listmonk.domain.SubscriberListState.class).invoke().body();
    assertThat(subscription.blocklisted()).isTrue();
    assertThat(subscription.status()).isEqualTo(SubscriptionStatus.UNCONFIRMED);
    awaitEligibleCount(listId, CampaignType.REGULAR, 0);

    var campaignId = "camp-" + id();
    httpClient.POST("/campaigns/" + campaignId)
        .withRequestBody(new CampaignEndpoint.CreateCampaignBody(
            "c", "s", "from@x.test", "b", listId, CampaignType.REGULAR))
        .invoke();
    httpClient.POST("/campaigns/" + campaignId + "/start").invoke();

    Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      var campaign = httpClient.GET("/campaigns/" + campaignId)
          .responseBodyAs(io.akka.listmonk.domain.CampaignState.class).invoke().body();
      assertThat(campaign.status()).isEqualTo(CampaignStatus.FINISHED);
      assertThat(campaign.toSend()).isEqualTo(0);
      assertThat(campaign.sent()).isEqualTo(0);
    });
  }
}
