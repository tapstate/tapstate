package io.tapstate.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * The {@code blank} recipe: the escape hatch of the starter catalog, for an author who wants to write
 * the workspace themselves. It writes one source and one pipeline as skeletons - the shape of each
 * resource with placeholder values and a comment on every line that has to be changed.
 *
 * <p>The files are bundled text copied verbatim rather than rendered through the canonical writer,
 * because the comments are the point: canonical form carries no comments, and a skeleton with no
 * comments is a puzzle. They are held to the canonical shape by a test that validates them instead.
 *
 * <p><b>The skeleton validates as written.</b> A skeleton is a workspace you edit, not a list of
 * errors you clear first - the summary's own next step is {@code tapstate validate}, and a recipe
 * whose whole point is a clean starting point must not fail it. What the placeholders do not do is
 * connect: {@code blank} is the one catalog entry that is not {@code runnable}, and the summary says
 * so in the line that follows its file list.
 */
final class BlankRecipe {

    /** The two files, in the order a reader meets them. */
    static final List<String> RESOURCES =
            List.of("source/example_source.tap.yml", "pipeline/example_pipeline.tap.yml");

    private BlankRecipe() {
    }

    /** The skeletons as they are written: bundled bytes, verbatim. */
    static List<WorkspaceWrite.File> bundledFiles() {
        return RESOURCES.stream().map(resource -> WorkspaceWrite.File.owned(resource, bundled(resource))).toList();
    }

    /** One bundled skeleton. Absent means a broken build, not a user error, so it crashes bare. */
    static String bundled(String resource) {
        String name = "/blank/" + resource.substring(resource.indexOf('/') + 1);
        try (InputStream in = BlankRecipe.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("the blank resource " + name + " is not on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException unreadable) {
            throw new UncheckedIOException("cannot read the bundled blank resource " + name, unreadable);
        }
    }
}
