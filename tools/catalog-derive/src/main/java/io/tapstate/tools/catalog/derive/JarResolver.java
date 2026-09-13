package io.tapstate.tools.catalog.derive;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Resolves a connector id to the built dist jar catalog-derive classloads. The dist jar is named
 * {@code <id>-connector-<version>.jar}, so the public id and connector suffix prefix-match exactly its own
 * jar. A missing jar is an expected, recoverable condition — some connectors in source are not part
 * of the OSS dist build — so {@link #find} returns empty and the caller skips and records it. An
 * ambiguous match (two jars share the prefix) is always a real error and fails loud either way.
 */
final class JarResolver {

    private final Path distDir;

    JarResolver(Path distDir) {
        this.distDir = distDir;
    }

    /** The connector's jar, or empty when the connector was not built into the dist. */
    Optional<Path> find(String connectorId) {
        String prefix = connectorId + "-connector-";
        List<Path> matches;
        try (Stream<Path> files = Files.list(distDir)) {
            matches = files
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.startsWith(prefix) && name.endsWith(".jar");
                    })
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("listing dist dir " + distDir, e);
        }
        if (matches.size() > 1) {
            throw new IllegalStateException(
                    "ambiguous dist jars for connector " + connectorId + " under " + distDir + ": " + matches);
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    /** The connector's jar, or throws when it was not built — for callers that require its presence. */
    Path resolve(String connectorId) {
        return find(connectorId).orElseThrow(() -> new IllegalStateException(
                "no dist jar " + connectorId + "-connector-*.jar for connector " + connectorId + " under " + distDir));
    }
}
