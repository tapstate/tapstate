package io.tapstate.core.dsl;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.ProjectManifest;
import io.tapstate.core.model.Resource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Loads a workspace directory into a validated {@link Workspace} (plan poc1 B3-6). Reads every
 * {@code *.tap.yml} under the directory in filename order, parses each into its resource model —
 * attributing any per-file parse error to its source filename ({@link DslException#source()}) —
 * then builds and validates the batch via {@link Workspace#of} (duplicate-id + reference closure)
 * and the connector capability matrix via {@link CapabilityRules} (plan C3). The closure is the
 * directory itself: the offline projection of the artifact store.
 */
public final class WorkspaceLoader {

    private WorkspaceLoader() {
    }

    /** Holds the bundled catalog, loaded on first use (the offline capability-matrix scope). */
    private static final class Bundled {
        static final TapstateCatalog CATALOG = TapstateCatalog.load();
    }

    /** Loads {@code dir} as one workspace batch against the bundled connector catalog. */
    public static Workspace load(Path dir) {
        return load(dir, Bundled.CATALOG);
    }

    /**
     * Loads {@code dir} as one workspace batch; throws {@link DslException} on the first violation.
     * The capability-matrix tier (mode × connector, config type / enum) is judged against
     * {@code catalog} — the offline bundled ∪ cache projection.
     */
    public static Workspace load(Path dir, TapstateCatalog catalog) {
        DslParser parser = new DslParser();
        String project = projectId(dir);
        List<Resource> resources = new ArrayList<>();
        for (Path file : artifacts(dir)) {
            String name = file.getFileName().toString();
            String text = read(file);
            // A project file names the project; it is not one of its resources.
            if (ProjectManifest.KIND.equals(DslParser.declaredKind(text))) {
                continue;
            }
            try {
                Resource resource = parser.parse(text);
                ProjectLabel.requireConsistent(resource, project);
                resources.add(resource);
            } catch (DslException e) {
                throw e.withSource(name);   // a parse error is located at exactly this file
            }
        }
        return Workspace.of(resources, catalog);
    }

    /**
     * The project file at the root of {@code dir}, parsed, or empty when the directory has none. A file
     * that is present but malformed is refused with its own diagnostic rather than read as absent: the
     * name it was meant to give the project would otherwise be silently replaced by the directory's.
     */
    public static Optional<ProjectManifest> manifest(Path dir) {
        Path file = dir.resolve(ProjectManifest.FILE_NAME);
        if (!Files.isRegularFile(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(new DslParser().parseProject(read(file)));
        } catch (DslException e) {
            throw e.withSource(ProjectManifest.FILE_NAME);
        }
    }

    /** The id of the project rooted at {@code dir}: what its project file declares, else its name. */
    public static String projectId(Path dir) {
        return manifest(dir).map(ProjectManifest::id).orElseGet(() -> defaultProjectId(dir));
    }

    /**
     * The id a directory without a project file is known by: its own name. A name carrying the
     * reserved addressing separator or anything that is not a visible character is spelled with an
     * underscore in its place, since an id may carry neither.
     */
    public static String defaultProjectId(Path directory) {
        Path name = directory.toAbsolutePath().normalize().getFileName();
        String raw = name == null ? "" : name.toString();
        StringBuilder id = new StringBuilder(raw.length());
        raw.codePoints().forEach(c -> id.appendCodePoint(
                c == '.' || Character.isWhitespace(c) || Character.isISOControl(c) ? '_' : c));
        return id.isEmpty() ? ProjectManifest.KIND : id.toString();
    }

    private static List<Path> artifacts(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".tap.yml"))
                    .sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
