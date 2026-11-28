package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoFirstClearResultTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
    private static final PipelineIntentSnapshot CLEARED = new PipelineIntentSnapshot(SCOPE, 8,
            new DesiredState("flow", PipelineState.STOPPED, "definition"));
    private static final String FIELD = "firstClearingStopResult";

    @Test
    void onlyTheOriginalScopeAndStopRevisionCanAdoptTheReceipt() {
        var receipt = clearedReceipt();
        assertThat(MongoPipelineIntentStore.firstClear(mapping(receipt), CLEARED, 7))
                .contains(PipelineIntentStore.FirstClearResult.CLEARED);
        assertThat(MongoPipelineIntentStore.firstClear(mapping(receipt), CLEARED, 6)).isEmpty();
        var otherScope = new PipelineIntentScope("cluster", 1, "other", "flow", 1);
        var otherReceipt = new Document(receipt).append("scope", MongoPipelinePreparationFacts.scope(otherScope));
        assertThat(MongoPipelineIntentStore.firstClear(mapping(otherReceipt), CLEARED, 7)).isEmpty();
        var latest = new PipelineIntentSnapshot(SCOPE, 9, CLEARED.desired());
        assertThat(MongoPipelineIntentStore.firstClear(mapping(receipt), latest, 7)).isEmpty();
        var resumed = new PipelineIntentSnapshot(SCOPE, 9, new DesiredState("flow", PipelineState.RUNNING, "definition"));
        assertThat(MongoPipelineIntentStore.firstClear(mapping(receipt), resumed, 7)).isEmpty();
        assertThat(MongoPipelineIntentStore.firstClear(mapping(receipt), new PipelineIntentSnapshot(SCOPE, 9, null), 7))
                .isEmpty();
    }

    @Test
    void malformedOrContradictoryReceiptsAreUnreadable() {
        var receipt = clearedReceipt();
        for (var invalid : List.of("CLEARED", new Document(receipt).append("unknown", true),
                new Document(receipt).append("formatVersion", 2), new Document(receipt).append("result", "UNPROVEN"))) {
            assertUnreadable(mapping(invalid), CLEARED, 7);
        }
        var stillClearing = new PipelineIntentSnapshot(SCOPE, 7,
                new DesiredState("flow", PipelineState.STOPPED, "definition", true));
        assertUnreadable(mapping(new Document(receipt).append("intentRevision", 7L)), stillClearing, 7);
    }

    private static Document clearedReceipt() {
        return new Document("formatVersion", 1).append("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                .append("stopRevision", 7L).append("intentRevision", 8L).append("result", "CLEARED");
    }

    private static Document mapping(Object receipt) {
        return new Document(FIELD, receipt);
    }

    private static void assertUnreadable(Document mapping, PipelineIntentSnapshot live, long stopRevision) {
        assertThatThrownBy(() -> MongoPipelineIntentStore.firstClear(mapping, live, stopRevision))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
