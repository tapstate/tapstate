package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DefaultSourceTestAttemptCodecTest {
    private static DefaultSourceTestAttempt checking() {
        return new DefaultSourceTestAttempt("views", "mongodb", "a".repeat(64), UUID.randomUUID().toString(),
                new WorkloadOwner("node", "boot"), 3, 1000, 61000, DefaultSourceTestAttempt.State.CHECKING, null, null);
    }
    @Test void pendingAndTerminalRowsRoundTripWithoutSavingCredentials() {
        var pending = checking();
        assertThat(MongoCacheConnectionTestResultStore.attempt(MongoCacheConnectionTestResultStore.document(pending))).isEqualTo(pending);
        var report = new ConnectionTestResult("views", "mongodb", ConnectionTestResult.Outcome.PASSED, List.of(), 2000, true);
        var done = pending.finish(DefaultSourceTestAttempt.State.PASSED, 2000, report);
        assertThat(MongoCacheConnectionTestResultStore.attempt(MongoCacheConnectionTestResultStore.document(done))).isEqualTo(done);
        assertThat(MongoCacheConnectionTestResultStore.document(done).keySet()).doesNotContain("uri", "settings", "password");
    }
    @Test void theBoundUsesEncodedBsonBytesIncludingTheOriginalScope() {
        var row = MongoCacheConnectionTestResultStore.document(checking()).append("cacheScope",
                new org.bson.Document("clusterId", "cluster").append("bindingVersion", 1L).append("cacheIncarnation", "cache"));
        row.append("diagnostic", java.util.Collections.nCopies(110_000, "x"));
        assertThat(row.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8).length).isLessThan(1024 * 1024);
        assertThatThrownBy(() -> MongoCacheConnectionTestResultStore.requireBounded(row, "views"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE));
    }

    @Test void unexpectedFieldsAndBadTypesAreCodedUnreadableRatherThanAdopted() {
        var original = MongoCacheConnectionTestResultStore.document(checking());
        for (var row : List.of(new org.bson.Document(original).append("unknown", true),
                new org.bson.Document(original).append("sessionGeneration", "3"),
                new org.bson.Document(original).append("deadlineAt", 120000L))) {
            assertThatThrownBy(() -> MongoCacheConnectionTestResultStore.attempt(row))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }
}
