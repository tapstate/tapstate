package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkRunTest {
    @TempDir Path folder;

    @Test
    void acceptsCompletionOnlyWhenObservedBeforeTheStageDeadline() {
        var clock = new AtomicLong();
        var timeout = Duration.ofSeconds(1);
        BenchmarkRunIT.waitFor(timeout, "target", () -> {
            clock.set(timeout.toNanos() - 1);
            return true;
        }, clock::get);

        clock.set(0);
        var late = assertThrows(RuntimeException.class, () -> BenchmarkRunIT.waitFor(timeout, "target", () -> {
            clock.set(timeout.toNanos() + 1);
            return true;
        }, clock::get));
        assertEquals("Timed out waiting for target", late.getMessage());

        clock.set(0);
        var expired = assertThrows(RuntimeException.class, () -> BenchmarkRunIT.waitFor(Duration.ZERO, "target",
                () -> fail("The fixture polled after the deadline"), clock::get));
        assertEquals("Timed out waiting for target", expired.getMessage());
    }

    @Test
    void usesTheVerifiedPrivateCopyAfterTheSharedConnectorChanges() throws Exception {
        Path source = Files.createDirectory(folder.resolve("shared"));
        Path privateDirectory = Files.createDirectory(folder.resolve("verified"));
        Path connector = Files.writeString(source.resolve("mongodb-connector.jar"), "original");
        String expectedDigest = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(connector)));
        var lock = BsonDocument.parse("""
                {"connectors":[{"id":"mongodb","bytes":8,"sha256":"%s"}]}
                """.formatted(expectedDigest));

        Path staged = BenchmarkRunIT.stageConnector(lock, source, privateDirectory, "mongodb");
        Files.writeString(connector, "replaced");

        assertEquals(privateDirectory.resolve(connector.getFileName()), staged);
        assertEquals("original", Files.readString(staged));
        Path seed = Files.copy(staged, folder.resolve("op-seed.jar"));
        assertEquals("original", Files.readString(seed));
        Path secondRun = Files.createDirectory(folder.resolve("another-run"));
        assertThrows(IllegalArgumentException.class,
                () -> BenchmarkRunIT.stageConnector(lock, source, secondRun, "mongodb"));
    }
}
