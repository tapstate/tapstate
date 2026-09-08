package io.tapstate.e2e;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * What the run looked like at the moment it failed, written down before the containers go away.
 *
 * <p>Without this a red end-to-end case leaves one assertion message. Everything that would explain it -
 * how many rows each engine actually holds, what the assembled document actually contains, what the
 * pipeline says about itself - dies with the containers, so the next move is to add a print statement
 * and run the whole thing again, and each run of these cases starts two database engines. On a case
 * that fails once in a while, that is the difference between diagnosing it and waiting for it.
 *
 * <p><b>No address is ever written here, and that is a constraint rather than an oversight.</b> These
 * files are uploaded as build artifacts from a public repository, and a connection URI carries the
 * credentials embedded in it. So the scene names places the way a specification names them - the table
 * alias and the collection - and never the string used to dial one. Not collecting the secret is the
 * only version of this that cannot leak it later.
 */
final class FailureScene {

    /**
     * Anything shaped like an address, in text this class did not compose.
     *
     * <p>Not collecting an address covers what is written deliberately; it does not cover what is
     * echoed. A driver's exception message can name the endpoint it failed to reach, and a document
     * read back can hold a uri in a column - both arrive here as somebody else's text and would be
     * appended verbatim. One filter over every such value keeps the promise the header makes.
     */
    private static final java.util.regex.Pattern ADDRESS =
            java.util.regex.Pattern.compile("[a-zA-Z][a-zA-Z0-9+.-]*://\\S*");

    private FailureScene() {
    }

    /** One value from outside, with anything address-shaped taken out of it. */
    private static String scrubbed(Object value) {
        return value == null ? "null" : ADDRESS.matcher(value.toString()).replaceAll("<address elided>");
    }

    /**
     * Writes the scene for one run, and never throws: it is called from a failure path, and a collector
     * that fails takes the real failure down with it. Anything it could not read is written as the
     * reason it could not.
     */
    static void write(Path file, Envelope envelope, TierBinding binding, String pipelineId) {
        StringBuilder scene = new StringBuilder();
        scene.append("# the scene at the moment ").append(envelope.name()).append(" failed\n")
                .append("# no address appears here: these files leave a public repository as build\n")
                .append("# artifacts, and a connection uri carries its credentials inside it.\n\n");

        scene.append("## pipeline ").append(pipelineId).append('\n');
        reading(scene, "state", () -> binding.state(pipelineId));
        reading(scene, "failure code", () -> binding.failureCode(pipelineId));
        reading(scene, "error count", () -> binding.errorCount(pipelineId));
        reading(scene, "changes that could not be placed", () -> binding.deadLettered(pipelineId));

        scene.append("\n## how many rows each place holds\n");
        for (TableAlias table : placesNamedBy(envelope)) {
            scene.append("  ").append(table).append(" = ");
            try {
                scene.append(binding.count(table));
            } catch (RuntimeException couldNotRead) {
                scene.append("unreadable (").append(couldNotRead.getClass().getSimpleName()).append(": ")
                        .append(scrubbed(couldNotRead.getMessage())).append(')');
            }
            scene.append('\n');
        }

        scene.append("\n## the documents this specification asserted on\n");
        for (Step step : envelope.steps()) {
            Matcher matcher = switch (step) {
                case Step.Await await -> await.matcher();
                case Step.Assertion assertion -> assertion.matcher();
                case Step.Lifecycle ignored -> null;
                case Step.StreamLifecycle ignored -> null;
                case Step.Composed ignored -> null;
                case Step.Cdc ignored -> null;
            };
            if (matcher instanceof Matcher.Doc doc) {
                document(scene, binding, doc);
            }
        }

        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, scene);
        } catch (IOException cannotWrite) {
            // Never thrown on. This runs from a failure path, and a collector that raises its own
            // problem replaces the failure it was called to describe - the reader then debugs the
            // scene writer instead of the case that went red. Said on stderr and dropped.
            System.err.println("could not write the failure scene to " + file + ": " + cannotWrite);
        }
    }

    private static void document(StringBuilder scene, TierBinding binding, Matcher.Doc doc) {
        TableAlias table = doc.table();
        scene.append("  ").append(table).append(" where ").append(scrubbed(doc.where())).append(" -> ");
        try {
            Optional<Map<String, Object>> found = binding.fetch(table, doc.where());
            scene.append(found.map(FailureScene::scrubbed).orElse("no document matches"));
        } catch (RuntimeException couldNotRead) {
            scene.append("unreadable (").append(scrubbed(couldNotRead.getMessage())).append(')');
        }
        scene.append('\n');
    }

    /** Every place this specification names, in the order it names them, each once. */
    private static Set<TableAlias> placesNamedBy(Envelope envelope) {
        Set<TableAlias> places = new LinkedHashSet<>();
        envelope.seed().forEach(seed -> places.add(seed.table()));
        for (Step step : envelope.steps()) {
            switch (step) {
                case Step.Cdc cdc -> places.add(cdc.table());
                case Step.Await await -> places.addAll(tablesOf(await.matcher()));
                case Step.Assertion assertion -> places.addAll(tablesOf(assertion.matcher()));
                case Step.Lifecycle ignored -> { }
                case Step.StreamLifecycle ignored -> { }
                case Step.Composed ignored -> { }
            }
        }
        return places;
    }

    private static List<TableAlias> tablesOf(Matcher matcher) {
        List<TableAlias> tables = new ArrayList<>();
        if (matcher instanceof Matcher.Count count) {
            tables.addAll(count.expected().keySet());
        } else if (matcher instanceof Matcher.Doc doc) {
            tables.add(doc.table());
        }
        return tables;
    }

    private static void reading(StringBuilder scene, String what, Reading reading) {
        scene.append("  ").append(what).append(" = ");
        try {
            scene.append(reading.take().map(FailureScene::scrubbed).orElse("nothing published yet"));
        } catch (RuntimeException couldNotRead) {
            scene.append("unreadable (").append(scrubbed(couldNotRead.getMessage())).append(')');
        }
        scene.append('\n');
    }

    @FunctionalInterface
    private interface Reading {
        Optional<?> take();
    }
}
