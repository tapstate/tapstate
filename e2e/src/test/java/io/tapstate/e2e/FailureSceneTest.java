package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scene has to be worth having and must never carry a secret, and the second is the harder one to
 * keep: these files leave a public repository as build artifacts, and the addresses this harness holds
 * carry credentials inside them. So the assertion is not "the password was redacted" but "no address
 * was ever collected" - a redaction is a filter somebody can widen a hole in, and this way there is
 * nothing to filter.
 */
class FailureSceneTest {

    private static final String PASSWORD = "hunter2-should-never-appear";

    @Test
    void writesWhatWouldExplainTheFailure(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("scene.txt");

        FailureScene.write(file, envelope(), new SceneBinding(), "order_pipeline");

        String scene = Files.readString(file);
        assertThat(scene)
                .as("the readings that say what the pipeline thinks of itself")
                .contains("state = RUNNING")
                .contains("failure code = nothing published yet")
                .contains("error count = 0");
        assertThat(scene)
                .as("how much is actually in each place the specification names, both engines and the store")
                .contains("orders_db.orders = 5")
                .contains("fulfillment_db.shipments = 6")
                .contains("views.order_state = 5");
        assertThat(scene)
                .as("and the document the failing assertion was about, as it really is")
                .contains("views.order_state where {id=1}")
                .contains("shipments=[a, b]");
    }

    @Test
    void neverWritesAnAddressAndThereforeNeverWritesACredential(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("scene.txt");

        FailureScene.write(file, envelope(), new SceneBinding(), "order_pipeline");

        String scene = Files.readString(file);
        assertThat(scene)
                .as("a connection uri carries its credentials inside it, so none may be here")
                .doesNotContain("://")
                .doesNotContain(PASSWORD);
    }

    /**
     * The half that "collect no address" does not cover: text this class did not compose. A driver's
     * exception names the endpoint it could not reach, and a document read back can hold a uri in a
     * column - both are somebody else's text, appended verbatim before this.
     */
    @Test
    void takesAddressesOutOfTextItDidNotCompose(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("scene.txt");

        FailureScene.write(file, envelope(), new LeakyBinding(), "order_pipeline");

        String scene = Files.readString(file);
        assertThat(scene)
                .as("an address in an exception message, and one inside a fetched document")
                .doesNotContain("://")
                .doesNotContain(PASSWORD);
        assertThat(scene).contains("<address elided>");
    }

    @Test
    void survivesReadingsThatCannotBeTakenAtAll(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("scene.txt");

        // Collected from a failure path: a collector that throws replaces the real failure with its own.
        FailureScene.write(file, envelope(), new BrokenBinding(), "order_pipeline");

        assertThat(Files.readString(file))
                .contains("unreadable")
                .as("what could not be read says so, and what could still gets written")
                .contains("order_pipeline");
    }

    private static Envelope envelope() {
        return EnvelopeParser.parse("""
                name: scene
                pipeline: order_pipeline.tap.yml
                seed:
                  orders_db.orders: { rows: 5 }
                  fulfillment_db.shipments: { rows: 6 }
                steps:
                  - start
                  - await: { count: { views.order_state: 5 } }
                  - assert: { doc: { views.order_state: { where: { id: 1 }, expect: { customer: alice }, size: { shipments: 2 } } } }
                """);
    }

    /** Answers every reading, and holds an address with a password in it the way a real binding does. */
    private static class SceneBinding implements TierBinding {

        final EndpointAddress secret =
                new EndpointAddress("orders_db", Map.of("uri", "mongodb://admin:" + PASSWORD + "@host/db"));

        @Override
        public long count(TableAlias table) {
            return switch (table.table()) {
                case "shipments" -> 6L;
                default -> 5L;
            };
        }

        @Override
        public Optional<Map<String, Object>> fetch(TableAlias table, Map<String, Object> where) {
            return Optional.of(Map.of("id", 1, "customer", "alice", "shipments", List.of("a", "b")));
        }

        @Override
        public Optional<PipelineState> state(String pipelineId) {
            return Optional.of(PipelineState.RUNNING);
        }

        @Override
        public Optional<Long> errorCount(String pipelineId) {
            return Optional.of(0L);
        }

        @Override
        public Optional<String> failureCode(String pipelineId) {
            return Optional.empty();
        }

        @Override
        public Optional<Long> deadLettered(String pipelineId) {
            return Optional.of(0L);
        }

        @Override
        public void registerConnector(String connectorId) {
        }

        @Override
        public void applyResources(List<String> resourceFiles) {
        }

        @Override
        public void readResources(List<String> resourceFiles) {
        }

        @Override
        public void discoverSchema(String resourceId) {
        }

        @Override
        public void seed(TableAlias table, List<Map<String, Object>> rows) {
        }

        @Override
        public void drive(String pipelineId, LifecycleVerb verb) {
        }

        @Override
        public void driveStream(String sourceId, StreamVerb verb) {
        }

        @Override
        public void restart(String pipelineId, boolean rereadEverything) {
        }

        @Override
        public void cdc(TableAlias table, CdcOp op, long rows) {
        }

        @Override
        public void update(TableAlias table, Map<String, Object> where, Map<String, Object> set) {
        }

        @Override
        public void delete(TableAlias table, Map<String, Object> where) {
        }

        @Override
        public void insert(TableAlias table, List<Map<String, Object>> rows) {
        }

        @Override
        public void redeliver(TableAlias table) {
        }
    }

    /** Answers with a uri in a document and throws one in a message: both paths, one binding. */
    private static final class LeakyBinding extends SceneBinding {

        @Override
        public Optional<Map<String, Object>> fetch(TableAlias table, Map<String, Object> where) {
            return Optional.of(Map.of("id", 1, "callback", "mongodb://admin:" + PASSWORD + "@host/db"));
        }

        @Override
        public long count(TableAlias table) {
            throw new IllegalStateException(
                    "could not reach mongodb://admin:" + PASSWORD + "@host/db");
        }
    }

    /** Every reading fails, which is the state a scene is most often collected in. */
    private static final class BrokenBinding extends SceneBinding {

        @Override
        public long count(TableAlias table) {
            throw new IllegalStateException("the container is already gone");
        }

        @Override
        public Optional<Map<String, Object>> fetch(TableAlias table, Map<String, Object> where) {
            throw new IllegalStateException("the container is already gone");
        }

        @Override
        public Optional<PipelineState> state(String pipelineId) {
            throw new IllegalStateException("the server stopped answering");
        }
    }
}
