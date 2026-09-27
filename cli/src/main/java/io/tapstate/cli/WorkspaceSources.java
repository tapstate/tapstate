package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.ProjectManifest;
import io.tapstate.core.model.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Best-effort discovery of existing {@code kind: source} ids in a directory, for the pipeline
 * wizard's reference menus. Every {@code *.tap.yml} is parsed in isolation; non-source kinds and any
 * file that fails to parse are silently skipped — a malformed neighbour must never break scaffolding.
 * The project file is set aside by its kind before parsing: it declares no resource to offer.
 *
 * <p>The structural scan the browse verbs read is {@link WorkspaceScan}; its comment says why there are
 * two and what each is for.
 */
final class WorkspaceSources {

    private WorkspaceSources() {
    }

    /** The sorted ids of every well-formed {@code kind: source} resource found anywhere under {@code dir}. */
    static List<String> idsIn(Path dir) {
        return idsOfKind(dir, "source");
    }

    /** The sorted ids of every well-formed resource of {@code kind} found anywhere under {@code dir}. */
    static List<String> idsOfKind(Path dir, String kind) {
        DslParser parser = new DslParser();
        List<String> ids = new ArrayList<>();
        for (Path file : artifacts(dir)) {
            try {
                String text = Files.readString(file);
                if (ProjectManifest.KIND.equals(DslParser.declaredKind(text))) {
                    continue;
                }
                Resource resource = parser.parse(text);
                if (resource.kind().equals(kind)) {
                    ids.add(resource.id());
                }
            } catch (RuntimeException | IOException skip) {
                // not a readable, well-formed resource — best-effort discovery ignores it
            }
        }
        ids.sort(null);
        return ids;
    }

    private static List<Path> artifacts(Path dir) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".tap.yml"))
                    .sorted().toList();
        } catch (IOException unwalkable) {
            // best-effort discovery: an unreadable tree yields no candidates, never a crash
            return List.of();
        }
    }
}
