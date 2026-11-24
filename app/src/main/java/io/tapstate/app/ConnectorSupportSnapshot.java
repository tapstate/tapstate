package io.tapstate.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** Reads and atomically replaces the one local official support snapshot. */
final class ConnectorSupportSnapshot {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorSupportSnapshot.class);
    private final Path file;

    ConnectorSupportSnapshot(Path dataDirectory) {
        file = dataDirectory.resolve("cache").resolve("connector-support.json");
    }

    ConnectorSupportManifest restore(String bundledJson) throws IOException {
        var bundled = ConnectorSupportManifest.read(bundledJson);
        try {
            var local = ConnectorSupportManifest.read(Files.readString(file));
            if (local.version() >= bundled.version()) return local;
        } catch (NoSuchFileException missing) {
            // The bundled manifest is the offline input for a fresh installation.
        } catch (IOException unreadable) {
            LOG.warn("Could not read the local connector support manifest. Using the bundled manifest.");
        }
        try {
            replace(bundledJson);
        } catch (IOException unwritable) {
            LOG.warn("Could not persist the bundled connector support manifest. Using it for this process.");
        }
        return bundled;
    }

    void replace(String validatedJson) throws IOException {
        Path parent = Files.createDirectories(file.getParent());
        Path temporary = Files.createTempFile(parent, ".connector-support-", ".tmp");
        try {
            try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = StandardCharsets.UTF_8.encode(validatedJson);
                while (bytes.hasRemaining()) channel.write(bytes);
                channel.force(true);
            }
            Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
