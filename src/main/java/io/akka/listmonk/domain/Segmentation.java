package io.akka.listmonk.domain;

import java.util.List;

/**
 * Which subscription statuses are send-eligible for a (campaign type, list optin type)
 * pair — SPEC-001 §3 rule 5, checked by running the source's own query
 * (question-log rows 2-4).
 */
public final class Segmentation {

  private Segmentation() {}

  public static List<String> eligibleStatuses(CampaignType campaignType, OptinType listOptin) {
    if (campaignType == CampaignType.OPTIN) {
      return listOptin == OptinType.DOUBLE
          ? List.of(SubscriptionStatus.UNCONFIRMED.name())
          : List.of();
    }
    return listOptin == OptinType.DOUBLE
        ? List.of(SubscriptionStatus.CONFIRMED.name())
        : List.of(SubscriptionStatus.UNCONFIRMED.name(), SubscriptionStatus.CONFIRMED.name());
  }
}
