package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MongoSrsModelContextsTest {
    @Test
    void equalInitialSequencesRemainValidBeyondTheBoxedLongCache() {
        var scope = new CacheBusinessScope("cluster", 1, "cache");
        var contexts = new MongoSrsModelContexts(null, scope);
        var fence = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "capture"),
                new WorkloadOwner("node", "boot"), 1, 0, 1);
        var table = new SourceTable("orders", List.of(new SourceField("id", "int")), List.of("id"), List.of());
        var model = new SrsModel(new SrsModelKey("chain", "orders", "cache", 7, 0), "ring", table, 128);
        var initial = contexts.initial(model, fence, "model-hash");
        assertThat(initial.firstSequence()).isEqualTo(128);
        assertThat(contexts.decode(initial.row()).publishedSequence()).isEqualTo(128);
    }
}
