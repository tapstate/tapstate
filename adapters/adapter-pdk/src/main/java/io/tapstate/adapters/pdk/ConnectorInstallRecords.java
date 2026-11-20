package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.ConnectorArtifactMetadata.Origin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Durable local installation records. The installer serializes each artifact's operations. */
final class ConnectorInstallRecords {
    private static final long SCHEMA_VERSION = 1;
    private final Path directory;

    ConnectorInstallRecords(Path directory) {
        this.directory = directory;
    }

    enum State {
        PENDING, COMPLETE
    }

    record Installation(String connectorId, String version, Origin origin, long sizeBytes, State state) {
        Installation {
            Objects.requireNonNull(origin);
            Objects.requireNonNull(state);
            if (connectorId.isBlank() || version.isBlank() || sizeBytes < 1) {
                throw new IllegalArgumentException("Invalid connector installation facts");
            }
        }

        Installation complete() {
            return new Installation(connectorId, version, origin, sizeBytes, State.COMPLETE);
        }
    }

    List<String> hashes() throws IOException {
        if (!Files.isDirectory(directory)) return List.of();
        try (var files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString())
                    .filter(name -> name.matches("[0-9a-f]{64}\\.json"))
                    .map(name -> name.substring(0, 64)).sorted().toList();
        }
    }

    void remove(String hash) throws IOException {
        Files.deleteIfExists(path(hash));
    }

    Optional<Installation> read(String hash) throws IOException {
        Path file = path(hash);
        try {
            Object data = JsonReader.parse(Files.readString(file));
            if (!(data instanceof Map<?, ?> values) || !Long.valueOf(SCHEMA_VERSION).equals(values.get("schemaVersion"))
                    || !(values.get("connectorId") instanceof String connectorId)
                    || !(values.get("version") instanceof String version)
                    || !(values.get("origin") instanceof String origin)
                    || !(values.get("sizeBytes") instanceof Long sizeBytes)
                    || !(values.get("state") instanceof String state)) {
                throw new IOException("Invalid connector install record");
            }
            return Optional.of(new Installation(connectorId, version, Origin.valueOf(origin), sizeBytes, State.valueOf(state)));
        } catch (NoSuchFileException missing) {
            return Optional.empty();
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid connector install record", invalid);
        }
    }

    void save(String hash, Installation installation) throws IOException {
        Path destination = path(hash);
        byte[] json = JsonWriter.write(Map.of(
                "schemaVersion", SCHEMA_VERSION,
                "connectorId", installation.connectorId(),
                "version", installation.version(),
                "origin", installation.origin().name(),
                "sizeBytes", installation.sizeBytes(),
                "state", installation.state().name()
        )).getBytes(StandardCharsets.UTF_8);

        Files.createDirectories(directory);
        Path temporary = Files.createTempFile(directory, "." + hash + "-", ".tmp");
        try {
            try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(json);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private Path path(String hash) {
        if (!hash.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Invalid connector artifact hash");
        return directory.resolve(hash + ".json");
    }
}
