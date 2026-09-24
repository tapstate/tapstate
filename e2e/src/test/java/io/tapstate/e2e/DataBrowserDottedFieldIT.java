package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A column whose own name holds a dot is reachable, and reachable as itself.
 *
 * <p>A dot in a written field name is ambiguous: documents nest, so it usually steps into one, but a
 * source is free to name a column {@code price.usd} and that name reaches the store as a top-level key.
 * Read as a step, a request for that column asks for a {@code usd} inside a {@code price} - matching
 * nothing, reporting nothing wrong, and looking exactly like a column that happens to be empty. The
 * escape is what separates the two readings, and the two readings are what this witnesses.
 *
 * <p>Both documents are needed and neither would do alone. With only the literal-keyed one, an
 * implementation that reads every dot as a literal name answers both spellings correctly by accident;
 * with only the nested one, the implementation that had this defect answers correctly too. It is the
 * pair, seeded together and filtered apart, that says the two spellings are actually being told apart.
 *
 * <p>Written straight into the store rather than through a pipeline. No write path of the product's
 * produces a column named this way - the harness's own connector cannot express one - and a shape the
 * product cannot create is one no route through the product can put there. What is under test is how
 * the read face reads a field name, not how such a name comes to exist.
 *
 * <p>Both read faces, because they translate the same spelling by different routes: a one-shot read
 * sends it to the store as the store's own dialect, a follow evaluates it here against rows already in
 * hand. Two readings of one spelling is two answers to one question, and the way that fails is a filter
 * quietly meaning different things in different views, with nothing reporting the disagreement.
 *
 * <p>Real MongoDB and the real connector, not the harness's synthetic one, and that is forced rather
 * than chosen: the expression form this translates into ({@code $getField} under an {@code $expr}) is
 * the store's own, and a synthetic connector agreeing with it would only be this harness's second
 * opinion about what the store does. Gated on a directory of real connector jars, so it runs on the
 * real-connector lane. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=DataBrowserDottedFieldIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class DataBrowserDottedFieldIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static final String SOURCE_ID = "src_mongo";
    private static final String COLLECTION = "orders";
    private static final String DATABASE = "e2e_dotted_field";

    /**
     * The name of the column, as the store holds it: one top-level key, with a dot in the middle of it.
     *
     * <p>The same characters are also the written filter that asks for a {@code usd} inside a
     * {@code price}, and that is not an accident of naming - it is the ambiguity itself, kept in one
     * place so the two uses below are visibly the same string doing two jobs.
     */
    private static final String LITERAL_COLUMN = "price.usd";

    /** The written field name that asks for the column called {@code price.usd}. */
    private static final String ESCAPED = "price\\.usd";

    /** The written field name that asks for the {@code usd} inside a {@code price}. */
    private static final String BARE = LITERAL_COLUMN;

    /**
     * Values are text rather than numbers, and not to dodge anything this witnesses. The two faces
     * compare equality by different rules - the store coerces across its numeric types, the in-memory
     * evaluator asks Java whether the two objects are equal - so a number seeded as an int and filtered
     * as the long a JSON literal parses into would part the two faces for a reason that has nothing to
     * do with a dot. That disagreement is real and worth its own witness; this is not it.
     */
    private static final String HELD_UNDER_THE_LITERAL_NAME = "100.00";

    private static final String HELD_UNDER_THE_PATH = "200.00";

    private static final String LITERALLY_NAMED = "literally-named";
    private static final String NESTED = "nested";
    private static final String LITERALLY_NAMED_AGAIN = "literally-named-again";
    private static final String NESTED_AGAIN = "nested-again";

    /** Holds both readings at once, so every follow has to be sent it whichever spelling it asked with. */
    private static final String BOTH_READINGS = "both-readings";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void tellsTheColumnNamedWithADotFromThePathThroughOne() {
        String storeUri = SharedMongo.replicaSetUrl("e2e_dotted_field_store");
        String dataUri = SharedMongo.replicaSetUrl(DATABASE);

        try (ServerHandle server = InProcessServer.start(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            mongo.insert(EndpointAddress.uri(dataUri), COLLECTION,
                    new Document("_id", LITERALLY_NAMED).append(LITERAL_COLUMN, HELD_UNDER_THE_LITERAL_NAME));
            mongo.insert(EndpointAddress.uri(dataUri), COLLECTION,
                    new Document("_id", NESTED).append("price", new Document("usd", HELD_UNDER_THE_PATH)));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("src_mongo.tap.yml", sourceYaml(dataUri)));

            // (1) The escaped spelling names the column, and reaches the one document that has it. The
            // nested document is the control: an implementation reading the escape as a step would find
            // that one instead, and one reading it as a name would find this one either way.
            assertThat(idsOf(control.find(SOURCE_ID, COLLECTION,
                    Map.of("filter", term(ESCAPED, HELD_UNDER_THE_LITERAL_NAME)))))
                    .as("the rows a read for the column literally named `%s` is answered with", BARE)
                    .containsExactly(LITERALLY_NAMED);

            // (2) The bare spelling still steps, which is the half that must not be lost in making the
            // first one work. A change that reached the column by reading every dot literally would pass
            // (1) and fail here, and that implementation is the likelier of the two to be written.
            assertThat(idsOf(control.find(SOURCE_ID, COLLECTION,
                    Map.of("filter", term(BARE, HELD_UNDER_THE_PATH)))))
                    .as("the rows a read for a `usd` inside a `price` is answered with")
                    .containsExactly(NESTED);

            // (3) The same two filters on the followed face. They travel the same words and must come
            // back the same subset; the way this breaks is one of the two readings being changed and the
            // other not, which shows up nowhere else - a follow that disagrees with a read about what a
            // filter means reports nothing wrong, it just shows different rows.
            try (ControlPlane.Follow byName = control.follow(SOURCE_ID, COLLECTION,
                            term(ESCAPED, HELD_UNDER_THE_LITERAL_NAME));
                    ControlPlane.Follow byPath = control.follow(SOURCE_ID, COLLECTION,
                            term(BARE, HELD_UNDER_THE_PATH))) {

                awaitBothLive(mongo, dataUri,
                        Map.of("filtered on the column literally named `" + BARE + "`", byName,
                                "filtered on a `usd` inside a `price`", byPath));

                mongo.insert(EndpointAddress.uri(dataUri), COLLECTION, new Document("_id", LITERALLY_NAMED_AGAIN)
                        .append(LITERAL_COLUMN, HELD_UNDER_THE_LITERAL_NAME));
                mongo.insert(EndpointAddress.uri(dataUri), COLLECTION, new Document("_id", NESTED_AGAIN)
                        .append("price", new Document("usd", HELD_UNDER_THE_PATH)));
                // Last, and matching both filters, so each follow having been sent it proves the two
                // before it were already delivered or already filtered out. Without a change both
                // follows must see, "the other document never arrived" is only "it has not arrived yet".
                mongo.insert(EndpointAddress.uri(dataUri), COLLECTION, bothReadings(BOTH_READINGS));

                assertThat(subjectsUnderTest(byName))
                        .as("the changes a follow filtered on the column literally named `%s` was sent", BARE)
                        .containsExactly(LITERALLY_NAMED_AGAIN, BOTH_READINGS);
                assertThat(subjectsUnderTest(byPath))
                        .as("the changes a follow filtered on a `usd` inside a `price` was sent")
                        .containsExactly(NESTED_AGAIN, BOTH_READINGS);
            }
        }
    }

    /**
     * Puts documents every follow must match in front of them until each has been sent one.
     *
     * <p>A follow is opened by a handshake that completes before the store's stream behind it does, so
     * until something has come down it, a follow that is not yet running and one that is running and
     * matching nothing look identical from here. Every document this inserts is named apart from the
     * ones under test and ignored by the assertions, so however many it takes changes nothing but how
     * long it took.
     *
     * <p>The follows are named, and a timeout says which ones went silent - that is what makes the two
     * causes tell apart rather than only being admitted to. These documents hold both readings, so the
     * only thing that silences one follow and not its neighbour is that follow's own spelling no longer
     * reaching a column it names. All of them silent is the stream itself.
     */
    private static void awaitBothLive(
            MongoEndpoints mongo, String dataUri, Map<String, ControlPlane.Follow> follows) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        int attempt = 0;
        List<String> silent = List.of();
        while (System.nanoTime() - deadline < 0) {
            mongo.insert(EndpointAddress.uri(dataUri), COLLECTION, bothReadings("live-" + attempt++));
            silent = follows.entrySet().stream()
                    .filter(follow -> follow.getValue().frames().isEmpty())
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
            if (silent.isEmpty()) {
                return;
            }
            sleep();
        }
        throw new AssertionError("within " + TIMEOUT + " nothing at all was sent to the follow(s) " + silent
                + ", though every document put in front of them holds both readings"
                + (silent.size() == follows.size()
                        ? " - and none of the follows was sent anything, so the stream itself never ran"
                        : " - while " + (follows.size() - silent.size())
                                + " other follow(s) on the same collection were sent them"));
    }

    /** A document holding the column and the path at once, so either spelling reaches it. */
    private static Document bothReadings(String id) {
        return new Document("_id", id)
                .append(LITERAL_COLUMN, HELD_UNDER_THE_LITERAL_NAME)
                .append("price", new Document("usd", HELD_UNDER_THE_PATH));
    }

    /**
     * The ids of the documents under test a follow was sent, in arrival order, waiting for the last one
     * first. The documents that only proved the follow was running are left out; every id this
     * specification seeded on purpose is kept, including the ones it is asserting did <em>not</em>
     * arrive - dropping those would be dropping the assertion.
     */
    private static List<String> subjectsUnderTest(ControlPlane.Follow follow) {
        List<String> ids = new ArrayList<>();
        List<Map<String, Object>> delivered = follow.awaitFrame(
                frame -> BOTH_READINGS.equals(idOf(frame)), TIMEOUT, "the change both follows must be sent");
        for (Map<String, Object> frame : delivered) {
            String id = idOf(frame);
            if (List.of(LITERALLY_NAMED_AGAIN, NESTED_AGAIN, BOTH_READINGS).contains(id)) {
                ids.add(id);
            }
        }
        return ids;
    }

    /** The id of the row a change carries, from whichever side of it the store described. */
    @SuppressWarnings("unchecked")
    private static String idOf(Map<String, Object> frame) {
        Object row = frame.get("after") != null ? frame.get("after") : frame.get("before");
        return row instanceof Map<?, ?> map ? String.valueOf(((Map<String, Object>) map).get("_id")) : null;
    }

    private static Map<String, Object> term(String field, String value) {
        Map<String, Object> filter = new LinkedHashMap<>();
        filter.put("field", field);
        filter.put("op", "eq");
        filter.put("value", value);
        return filter;
    }

    @SuppressWarnings("unchecked")
    private static List<String> idsOf(Map<String, Object> answer) {
        assertThat(answer).containsKey("rows");
        List<String> ids = new ArrayList<>();
        for (Map<String, Object> row : (List<Map<String, Object>>) answer.get("rows")) {
            ids.add(String.valueOf(row.get("_id")));
        }
        return ids;
    }

    /**
     * The database is declared beside the address rather than left to be read out of it. Which database a
     * read may touch is assembled from the connection and sent with the request, and a connection that
     * names none sends none - so a source declared with a bare address is one this face cannot read at
     * all, whatever the connector would have inferred for its own purposes.
     */
    private static String sourceYaml(String dataUri) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mongo
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(dataUri, DATABASE);
    }

    private static void sleep() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for a follow to come up", e);
        }
    }
}
