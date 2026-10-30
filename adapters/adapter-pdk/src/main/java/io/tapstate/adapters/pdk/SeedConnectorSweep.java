package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RegistrationSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The startup seed sweep: registers every {@code *.jar} directly in a seed directory through the
 * same content-hash register-if-absent path an explicit register uses, leaving the files where they
 * are. Registration is idempotent in the store, so restarting a node — or several nodes sweeping the
 * same directory concurrently — is harmless: bytes already registered come back as "already
 * registered", never as a second copy.
 *
 * <p>A defective artifact (a coded connector refusal, or a file that cannot be read) gets its own
 * failed outcome and does not stop the sweep — one bad jar must not keep the rest of a release's
 * connectors from registering, or a node from starting. A missing seed directory is an empty sweep:
 * shipping without one is a valid deployment. A seed path that exists but is not a directory, or a
 * directory that cannot be listed, is a misconfigured or faulted environment and surfaces as an
 * unchecked I/O exception rather than an empty sweep. Programmer errors are not contained; they
 * crash as they are.
 */
public final class SeedConnectorSweep {

    private static final String JAR_SUFFIX = ".jar";

    private final ConnectorArtifactRegistrar registrar;

    public SeedConnectorSweep(ConnectorArtifactRegistrar registrar) {
        this.registrar = Objects.requireNonNull(registrar, "registrar");
    }

    /** Sweeps {@code seedDir}, returning one outcome per {@code *.jar} file, sorted by file name. */
    public List<SeedOutcome> sweep(Path seedDir) {
        return sweep(seedDir, null);
    }

    /** Sweeps only explicitly selected file names; the caller validates its immutable release first. */
    public List<SeedOutcome> sweep(Path seedDir, Set<String> selectedFileNames) {
        Objects.requireNonNull(seedDir, "seedDir");
        Set<String> selected = selectedFileNames == null ? null : Set.copyOf(selectedFileNames);
        if (Files.notExists(seedDir)) {
            return List.of();
        }
        if (!Files.isDirectory(seedDir)) {
            // A seed path that exists but is not a directory is a misconfiguration, not an absent
            // seed: silently sweeping nothing would drop every shipped connector without a trace.
            throw new UncheckedIOException(
                    "seed path is not a directory: " + seedDir, new NotDirectoryException(seedDir.toString()));
        }
        List<SeedOutcome> outcomes = new ArrayList<>();
        for (Path artifact : seedJars(seedDir)) {
            if (selected != null && !selected.contains(artifact.getFileName().toString())) continue;
            try {
                outcomes.add(new SeedOutcome.Seeded(
                        artifact, registrar.register(artifact, RegistrationSource.SEED)));
            } catch (TapstateException | UncheckedIOException e) {
                outcomes.add(new SeedOutcome.Failed(artifact, e));
            }
        }
        return List.copyOf(outcomes);
    }

    /** The regular {@code *.jar} files (extension matched case-insensitively) directly in the seed
     *  directory, sorted by name. */
    private static List<Path> seedJars(Path seedDir) {
        try (Stream<Path> entries = Files.list(seedDir)) {
            return entries
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString()
                            .toLowerCase(Locale.ROOT).endsWith(JAR_SUFFIX))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("listing seed directory " + seedDir, e);
        }
    }
}
