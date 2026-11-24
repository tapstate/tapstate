package io.tapstate.adapters.mongostore;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.SinkPreparationStore;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MongoDiscoveredTargetAdmissionTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
    private static SinkPreparationStore.Input input(String node, String table, OnFullLoad policy) {
        return new SinkPreparationStore.Input(new SinkPreparationStore.Key(SCOPE, PipelineNode.Purpose.SYNC, node, table),
                "a".repeat(64), policy, true);
    }
    private static Document encoded(SinkPreparationStore.Input input) {
        return new Document("key", MongoSinkPreparationStore.id(input.key())).append("modelHash", input.modelHash())
                .append("policy", input.policy().yaml()).append("fullLoad", input.fullLoad());
    }
    private static Document claim() {
        return new Document("pipelineSources", new Document("contentHash", "inventory"))
                .append("discoveredTableExpansion", new Document("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                        .append("inputsHash", "inventory").append("addedTables", List.of("NEW")));
    }
    @Test void newTableMustBelongToTheExactAdmittedInventoryAndAnExistingSyncNode() {
        var original = List.of(encoded(input("sync_1", "OLD", OnFullLoad.APPEND)));
        var candidate = input("sync_1", "NEW", OnFullLoad.APPEND);
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(claim(), original, candidate)).isTrue();
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(new Document(), original, candidate)).isFalse();
        var stale = claim(); stale.get("pipelineSources", Document.class).put("contentHash", "changed");
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(stale, original, candidate)).isFalse();
        var otherScope = claim(); otherScope.get("discoveredTableExpansion", Document.class).put("scope", new Document());
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(otherScope, original, candidate)).isFalse();
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(claim(), original, input("sync_1", "UNSELECTED", OnFullLoad.APPEND))).isFalse();
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(claim(), original, input("another", "NEW", OnFullLoad.APPEND))).isFalse();
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(claim(), original, input("sync_1", "NEW", OnFullLoad.CLEAR))).isFalse();
        assertThat(MongoSinkPreparationStore.admitsDiscoveredTarget(claim(), List.of(), candidate)).isFalse();
    }
}
