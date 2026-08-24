package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The in-place view follows a change made upstream of it, and redraws rather than appends.
 *
 * <p>What is under test is the whole distance from a row a user edits to the screen they are watching:
 * a change is made to the upstream table, a pipeline carries it into the view, and the view the command
 * is holding has to come to say the new thing. Every other witness of this command drives it against a
 * collection the harness writes into directly, which proves the view notices a collection changing but
 * says nothing about the path a change actually travels.
 *
 * <p>The row carries an array, and the change makes the array longer. An array is here because it is
 * the shape a flattening read face silently ruins: a list rendered by its Java text is still a line
 * with the right values in it, so a reader skimming the screen cannot tell it from the list, and every
 * assertion phrased as "the values are on the screen" passes on both. The assertions below are phrased
 * on the rendered array instead, which is why they part the two.
 *
 * <p>Mongo on both ends. That is not a shortcut around a more realistic upstream - it is what lets one
 * connector and one container witness the whole path, and the upstream is a real database making a real
 * change either way. The synthetic connector cannot stand in on either end: it stores every cell as
 * text, so a list reaching it is flattened and its comma breaks the file, and its tail delivers only
 * rows ordered past the last one it delivered, which are by construction new keys - so no change it can
 * deliver ever modifies a row the view is already holding.
 *
 * <p>Gated on real connector jars, so it runs on the real-connector lane. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=WatchRedrawsIT -Dtest=none
 * </pre>
 */
class WatchRedrawsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    private static final String UPSTREAM_ID = "src_upstream";
    private static final String VIEW_ID = "tgt_view";
    private static final String COLLECTION = "orders";
    private static final String UPSTREAM_DATABASE = "e2e_watch_upstream";
    private static final String VIEW_DATABASE = "e2e_watch_view";
    private static final String PIPELINE_ID = "upstream_to_view";

    /**
     * The one row there is. One rather than several on purpose: unasked, the command holds the first row
     * the database hands back, in an order the view's own footer says is not stable. With a single row
     * there is no order to depend on, so "the row it is holding" and "the row that changes" are the same
     * row by construction rather than by luck.
     */
    private static final String ROW_ID = "only";

    /** The array as it starts, and as the first frame must draw it. */
    private static final String HELD = "[\"north\", \"south\"]";

    /** The array after the upstream row is edited: the same list with one more in it. */
    private static final String GROWN = "[\"north\", \"south\", \"east\"]";

    /** The element the edit adds. Named apart so the barrier below can say the first frame lacks it. */
    private static final String ADDED = "east";

    /**
     * Cursor movement up over the lines already drawn - the whole of drawing in place, and the thing an
     * appending implementation does not emit. Matched as a pattern rather than a literal because the
     * count is the height of the frame before it, which is a fact about the row rather than about this.
     */
    private static final String CURSOR_UP = "\\[\\d+A";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void redrawsInPlaceWhatItHoldsAfterTheUpstreamRowIsEdited() throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("e2e_watch_store");
        String upstreamUri = SharedMongo.replicaSetUrl(UPSTREAM_DATABASE);
        String viewUri = SharedMongo.replicaSetUrl(VIEW_DATABASE);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints();
                MongoClient upstream = MongoClients.create(new ConnectionString(upstreamUri))) {

            mongo.insert(EndpointAddress.uri(upstreamUri), COLLECTION, new Document("_id", ROW_ID)
                    .append("customer", "first")
                    .append("regions", List.of("north", "south")));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_upstream.tap.yml", upstreamYaml(upstreamUri));
            resources.put("tgt_view.tap.yml", viewYaml(viewUri));
            resources.put("pipeline.tap.yml", pipelineYaml());
            control.apply(resources);

            // The upstream model is discovered because the key it reports is what the sink writes the
            // view's rows under. Without it the sink has no key, and the edit below lands as a second row
            // instead of changing the first - which the view, holding the first, would never show.
            control.discoverSchema(UPSTREAM_ID, "mongodb", connectorConfig(upstreamUri, UPSTREAM_DATABASE));
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            // The view has to hold the row before the command can be asked to hold it; started and
            // arrived are different moments, and starting the command in between would have it draw the
            // empty view and say so.
            awaitViewRow(mongo, viewUri, document -> document.get("regions") instanceof List<?> list
                    && list.size() == 2);

            try (CliProcess watch = CliProcess.onATerminal(Map.of("TAPSTATE_PASSWORD", PASSWORD),
                    "-c", server.baseUrl().toString(), "-u", USER,
                    "watch", VIEW_ID + "." + COLLECTION)) {

                // (1) The array reaches the screen as an array. A read face that handed the list back as
                // its Java text would put north and south on this line just the same; the brackets and
                // the quotes are the difference, so they are what is asserted.
                String first = watch.awaitOutput(seen -> seen.contains(HELD), TIMEOUT,
                        "the view to draw the array the row holds");

                // Two barriers on that first frame, and each disarms one way a later assertion could pass
                // without meaning anything. Without the first, an implementation that drew the grown array
                // from the start would satisfy (2) before the edit was even made. Without the second, (3)
                // could be read off a frame that was never redrawn at all - the first frame deliberately
                // emits no cursor movement, so that it lands below the prompt instead of eating what is
                // above it.
                assertThat(first)
                        .as("the first frame, before anything upstream was edited")
                        .doesNotContain(ADDED);
                assertThat(first)
                        .as("the first frame, which is drawn where the prompt left off rather than in place")
                        .doesNotContainPattern(CURSOR_UP);

                // The edit. Made on the upstream table, by a client of that database - not on the view,
                // and not through any face of the product. Everything after this is the product carrying
                // it.
                upstream.getDatabase(UPSTREAM_DATABASE).getCollection(COLLECTION).updateOne(
                        new Document("_id", ROW_ID),
                        new Document("$set", new Document("regions", List.of("north", "south", ADDED))));

                // (2) The screen comes to say the new thing. An implementation that drew once and then
                // stopped looking fails here, and so does one whose view follows the collection it was
                // pointed at but not the pipeline writing into it.
                String redrawn = watch.awaitOutput(seen -> seen.contains(GROWN), TIMEOUT,
                        "the view to redraw the row it holds with the array the upstream edit grew");

                // (3) It redrew rather than appended. The bytes that put the next frame over the last one
                // are the only difference between the two, and down a pipe they are why this command
                // refuses to run at all - so their absence is not a cosmetic loss.
                assertThat(redrawn)
                        .as("what the view wrote once the upstream edit reached it")
                        .containsPattern(CURSOR_UP);
            }

            // The edit changed the row rather than adding one. Left unsaid, an implementation that wrote
            // a second row could still satisfy (2) - the view would be holding whichever of the two came
            // back first, and half the time that is the new one.
            //
            // Said plainly: no mutation was found that reaches this assertion. Both attempts at one -
            // taking the discovery away, and dropping the key the sink upserts on - break the path so
            // early that (2) times out first, so this line is carried on the argument above rather than
            // on a witness. It stays because the hole it names is real and cheap to close, not because
            // anything has shown it closing.
            assertThat(mongo.count(EndpointAddress.uri(viewUri), COLLECTION))
                    .as("rows in the view after an edit to the single upstream row")
                    .isEqualTo(1);
        }
    }

    /** Waits for the view to hold a row the predicate accepts, naming what it held when it does not. */
    private static void awaitViewRow(MongoEndpoints mongo, String viewUri, Predicate<Document> wanted)
            throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> held = List.of();
        while (System.nanoTime() - deadline < 0) {
            held = mongo.documents(EndpointAddress.uri(viewUri), COLLECTION);
            if (held.stream().anyMatch(wanted)) {
                return;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("the pipeline did not carry the row into the view; it held: " + held);
    }

    private static Map<String, Object> connectorConfig(String uri, String database) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("uri", uri);
        config.put("database", database);
        return config;
    }

    /** The upstream table, read as a snapshot and then followed, so an edit after start still arrives. */
    private static String upstreamYaml(String upstreamUri) {
        return """
                version: tapstate/v1
                kind: source
                id: src_upstream
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(upstreamUri, UPSTREAM_DATABASE);
    }

    /**
     * The view: the pipeline's target, and the same declaration the command reads. The database is named
     * beside the address rather than left inside it, because a read is confined to the database its
     * connection declares and one that declares none is a source this face cannot read at all.
     */
    private static String viewYaml(String viewUri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_view
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(viewUri, VIEW_DATABASE);
    }

    /**
     * The filter admits updates as well as the snapshot and inserts. Admitting only the first two is the
     * shape the neighbouring witnesses use, and under it the edit this one turns on would be dropped
     * before it ever reached the view.
     */
    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: upstream_to_view
                source: src_upstream
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [orders], type: filter, expr: "op == 'r' || op == 'i' || op == 'u'" }
                serve:
                  from: rows_through
                  sync:
                    - source: tgt_view
                """;
    }
}
