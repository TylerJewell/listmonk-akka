package io.akka.listmonk.api;

import akka.Done;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import com.typesafe.config.Config;
import io.akka.listmonk.application.CampaignEntity;
import io.akka.listmonk.application.CampaignSendWorkflow;
import io.akka.listmonk.application.ListEntity;
import io.akka.listmonk.application.SubscriberListEntity;
import io.akka.listmonk.application.SubscriptionOperations;
import io.akka.listmonk.domain.CampaignState;
import io.akka.listmonk.domain.CampaignType;
import io.akka.listmonk.domain.OptinType;
import io.akka.listmonk.domain.SubscriberListState;

/**
 * The HTTP surface for the send pipeline slice: lists, subscriptions, and campaign
 * create/start/pause/resume/cancel/read — the "something outside a test can reach this
 * port's own capability" step d requires for a headless port.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class CampaignEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;
  private final SubscriptionOperations subscriptions;
  private final Config config;

  public CampaignEndpoint(ComponentClient componentClient, Config config) {
    this.componentClient = componentClient;
    this.subscriptions = new SubscriptionOperations(componentClient);
    this.config = config;
  }

  public record CreateListBody(String name, OptinType optinType) {}

  @Post("/lists/{listId}")
  public Done createList(String listId, CreateListBody body) {
    return componentClient.forKeyValueEntity(listId)
        .method(ListEntity::create)
        .invoke(new ListEntity.Create(body.name(), body.optinType()));
  }

  @Get("/lists/{listId}")
  public ListEntity.State getList(String listId) {
    return componentClient.forKeyValueEntity(listId).method(ListEntity::get).invoke();
  }

  public record EligiblePreview(long count) {}

  /** How many subscribers a campaign of {@code campaignType} addressed to this list would
   * reach right now (SPEC-001 §3 rule 5) — the same audience-size preview listmonk's own
   * admin UI shows before a campaign is sent. Backed by {@link SubscriberListView}, an
   * eventually-consistent projection of every subscribe/confirm/unsubscribe/blocklist
   * event, so a subscription that was just created may not be reflected yet (SPEC-001 §4
   * decision 6) — this is also the read a caller should poll if they need to know the
   * view has caught up before starting a campaign. */
  @Get("/lists/{listId}/eligible-count/{campaignType}")
  public EligiblePreview eligibleCount(String listId, String campaignType) {
    var list = componentClient.forKeyValueEntity(listId).method(ListEntity::get).invoke();
    var statuses = io.akka.listmonk.domain.Segmentation.eligibleStatuses(
        CampaignType.valueOf(campaignType), list.optinType());
    var count = componentClient.forView().method(
        io.akka.listmonk.application.SubscriberListView::eligibleCount)
        .invoke(new io.akka.listmonk.application.SubscriberListView.ListQuery(listId, statuses));
    return new EligiblePreview(count.total());
  }

  public record SubscribeBody(String email, String name) {}

  @Post("/lists/{listId}/subscribers/{subscriberId}")
  public Done subscribe(String listId, long subscriberId, SubscribeBody body) {
    subscriptions.subscribe(subscriberId, listId, body.email(), body.name());
    return Done.getInstance();
  }

  @Post("/lists/{listId}/subscribers/{subscriberId}/confirm")
  public Done confirmOptin(String listId, long subscriberId) {
    subscriptions.confirmOptin(subscriberId, listId);
    return Done.getInstance();
  }

  @Post("/lists/{listId}/subscribers/{subscriberId}/unsubscribe")
  public Done unsubscribe(String listId, long subscriberId) {
    subscriptions.unsubscribe(subscriberId, listId);
    return Done.getInstance();
  }

  @Post("/subscribers/{subscriberId}/blocklist")
  public Done blocklist(long subscriberId) {
    subscriptions.blocklist(subscriberId);
    return Done.getInstance();
  }

  @Get("/lists/{listId}/subscribers/{subscriberId}")
  public SubscriberListState getSubscription(String listId, long subscriberId) {
    return componentClient.forEventSourcedEntity(subscriberId + ":" + listId)
        .method(SubscriberListEntity::get).invoke();
  }

  public record CreateCampaignBody(
      String name, String subject, String fromEmail, String body, String listId,
      CampaignType type) {}

  @Post("/campaigns/{campaignId}")
  public Done createCampaign(String campaignId, CreateCampaignBody body) {
    var send = config.getConfig("listmonk.send");
    return componentClient.forEventSourcedEntity(campaignId)
        .method(CampaignEntity::create)
        .invoke(new CampaignEntity.Create(
            body.name(), body.subject(), body.fromEmail(), body.body(), body.listId(),
            body.type(), send.getInt("default-batch-size"), send.getInt("default-concurrency"),
            send.getInt("default-message-rate-per-second"), false, 0, 0,
            send.getInt("default-max-send-errors")));
  }

  @Post("/campaigns/{campaignId}/start")
  public Done startCampaign(String campaignId) {
    return componentClient.forWorkflow(campaignId)
        .method(CampaignSendWorkflow::start).invoke(campaignId);
  }

  @Post("/campaigns/{campaignId}/pause")
  public Done pauseCampaign(String campaignId) {
    return componentClient.forEventSourcedEntity(campaignId)
        .method(CampaignEntity::pause).invoke();
  }

  @Post("/campaigns/{campaignId}/resume")
  public Done resumeCampaign(String campaignId) {
    var reply = componentClient.forEventSourcedEntity(campaignId)
        .method(CampaignEntity::resume).invoke();
    // The send workflow was left paused, not ended, by fetchBatchStep specifically so it
    // can re-enter the fetch loop here without re-running initStep (rule 11: resuming must
    // not recompute maxSubscriberId/toSend).
    componentClient.forWorkflow(campaignId).method(CampaignSendWorkflow::resumeSend).invoke();
    return reply;
  }

  @Post("/campaigns/{campaignId}/cancel")
  public Done cancelCampaign(String campaignId) {
    return componentClient.forEventSourcedEntity(campaignId)
        .method(CampaignEntity::cancel).invoke();
  }

  @Get("/campaigns/{campaignId}")
  public CampaignState getCampaign(String campaignId) {
    return componentClient.forEventSourcedEntity(campaignId).method(CampaignEntity::get).invoke();
  }
}
