package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Structural scan of a managed workspace — "structure is truth". Each {@code <root>/<kind>/} directory
 * holds that kind's artifacts; this reads every {@code *.tap.yml} directly under the known kind
 * directories (non-recursive — a kind directory is flat) and parses each into its model. The structural
 * kind is the directory, independent of what the file declares, so a misplaced or unreadable file is
 * surfaced honestly rather than dropped. The browse / describe verbs ({@code ls} / {@code desc}) read
 * the project through this one scan.
 *
 * <p>This is one of two scans of a project directory, and the split is deliberate. This one answers
 * "what is filed where" for a reader, so it reads only the kind directories and keeps what it cannot
 * parse. {@link WorkspaceSources} answers "which ids can a wizard offer", so it reads every file anywhere
 * and drops what it cannot parse. Neither reads the project file at the root: it names the project and
 * declares no resource, so it sits outside every kind directory and is set aside by content in the other.
 */
final class WorkspaceScan {

    private WorkspaceScan() {
    }

    /** The resource kinds, in display order — also the structural subdirectories of a workspace. */
    static final List<String> KINDS = List.of("source", "pipeline", "transform", "view", "serve");

    /** One scanned artifact: its structural kind (the directory), its file, and the parsed resource (null = unreadable). */
    record Artifact(String kind, Path file, Resource resource) {

        /** The resolved id — the parsed top-level id, or the filename stem when the file is unreadable. */
        String id() {
            return resource != null ? resource.id() : stem(file);
        }

        /** Whether the file declares a kind different from the directory it sits in (structure is truth). */
        boolean misplaced() {
            return resource != null && !resource.kind().equals(kind);
        }
    }

    /** All artifacts across the workspace's kind directories, in (kind display order, filename) order. */
    static List<Artifact> of(Path root) {
        List<Artifact> out = new ArrayList<>();
        DslParser parser = new DslParser();
        for (String kind : KINDS) {
            Path dir = root.resolve(kind);
            if (!Files.isDirectory(dir)) {
                continue;
            }
            for (Path file : files(dir)) {
                try {
                    out.add(new Artifact(kind, file, parser.parse(Files.readString(file))));
                } catch (RuntimeException | IOException unreadable) {
                    // structure is truth: the file is here, so it is kept — unreadable, never hidden
                    out.add(new Artifact(kind, file, null));
                }
            }
        }
        return out;
    }

    /** The {@code *.tap.yml} files directly in {@code dir} (non-recursive — the kind directory is flat). */
    private static List<Path> files(Path dir) {
        try (Stream<Path> list = Files.list(dir)) {
            return list.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".tap.yml"))
                    .sorted().toList();
        } catch (IOException unreadableDir) {
            // an unreadable directory yields no listing; validate owns the IO fault
            return List.of();
        }
    }

    private static String stem(Path file) {
        return file.getFileName().toString().replaceFirst("\\.tap\\.yml$", "");
    }
}
