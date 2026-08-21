package io.akka.listmonk.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import io.akka.listmonk.domain.OptinType;

/** A list's name and opt-in type — SPEC-001 §2 "List". The entity id is the list id. */
@Component(id = "list")
public class ListEntity extends KeyValueEntity<ListEntity.State> {

  public record State(String id, String name, OptinType optinType) {
    boolean exists() {
      return id != null;
    }
  }

  public record Create(String name, OptinType optinType) {}

  @Override
  public State emptyState() {
    return new State(null, null, null);
  }

  public Effect<Done> create(Create command) {
    if (currentState().exists()) {
      return effects().error("list " + commandContext().entityId() + " already exists");
    }
    return effects()
        .updateState(new State(commandContext().entityId(), command.name(), command.optinType()))
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<State> get() {
    if (!currentState().exists()) {
      return effects().error("list " + commandContext().entityId() + " not found");
    }
    return effects().reply(currentState());
  }
}
