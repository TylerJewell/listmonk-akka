package io.akka.listmonk.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.KeyValueEntityTestKit;
import io.akka.listmonk.domain.OptinType;
import org.junit.jupiter.api.Test;

/** SPEC-001 §2 "List" — review checklist O2. */
public class ListEntityTest {

  private static KeyValueEntityTestKit<ListEntity.State, ListEntity> kit() {
    return KeyValueEntityTestKit.of("list-a", ListEntity::new);
  }

  @Test
  public void createStoresNameAndOptinType() {
    var kit = kit();
    kit.method(ListEntity::create).invoke(new ListEntity.Create("weekly digest", OptinType.DOUBLE));

    var state = kit.method(ListEntity::get).invoke().getReply();
    assertThat(state.id()).isEqualTo("list-a");
    assertThat(state.name()).isEqualTo("weekly digest");
    assertThat(state.optinType()).isEqualTo(OptinType.DOUBLE);
  }

  @Test
  public void repeatCreateIsRejected() {
    var kit = kit();
    kit.method(ListEntity::create).invoke(new ListEntity.Create("weekly digest", OptinType.DOUBLE));

    var result = kit.method(ListEntity::create)
        .invoke(new ListEntity.Create("different name", OptinType.SINGLE));
    assertThat(result.isError()).isTrue();

    var state = kit.method(ListEntity::get).invoke().getReply();
    assertThat(state.name()).isEqualTo("weekly digest");
  }

  @Test
  public void getOnAnUncreatedListIsRefused() {
    var result = kit().method(ListEntity::get).invoke();
    assertThat(result.isError()).isTrue();
  }
}
