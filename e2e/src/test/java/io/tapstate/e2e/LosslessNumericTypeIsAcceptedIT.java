package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that a column arithmetic survives is computed, runs and lands right - and that a column
 * arithmetic would not survive still travels through the same expression untouched.
 *
 * <p>The companion of {@link DecimalIsRefusedAtApplyIT} over the same real table. That one proves the
 * gate refuses; this one proves the gate is not simply refusing everything, which is the cheapest way
 * to pass a refusal test and the one a reader should be able to rule out.
 *
 * <p>Values, not counts. A count says rows arrived, which a transform quietly reduced to an identity
 * would also say; the doubled column says the expression ran, and it is asserted per row so a single
 * constant cannot satisfy it. The gate's whole claim is about what happens to values, so a witness
 * that only counted would be agreeing with it without checking.
 *
 * <p>The decimal is the other half of that claim, and the sharper half. Carrying one through an
 * expression is deliberately allowed, on the grounds that moving a value loses nothing - so this reads
 * it back and compares it digit for digit, scale included. It is compared by equality rather than by
 * numeric comparison on purpose: a value rounded through a binary float, or one whose stated scale was
 * dropped, is still numerically equal to what it came from, and only equality sees the difference. An
 * adaptation that normalised every number into one representation would leave every gate green, every
 * count right, and this the only assertion in the suite that noticed.
 *
 * <p>Gated on Docker and on a directory of real connector jars. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors
 * </pre>
 */
class LosslessNumericTypeIsAcceptedIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PIPELINE_ID = "lossless_pipeline";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void anIntegralColumnIsComputedAndTheDecimalBesideItArrivesUnchanged(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        // A database of this tier's own on the shared server: the pipeline reads the table's changes, and
        // only that server's user holds the replication grant such a read needs.
        Map<String, Object> config = SharedMySql.settings("lossless_" + suffix);
        NumericSource.seed(config);

        String targetUri = SharedMongo.replicaSetUrl("lossless_target_" + suffix);

        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl("lossless_store_" + suffix));
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = NumericSource.connected(server);

            control.discoverSchema(NumericSource.SOURCE_ID, "mysql", config);
            control.apply(NumericSource.workspace(
                    config,
                    targetUri,
                    PIPELINE_ID,
                    // One expression computes, the other only moves. Both are allowed, for reasons
                    // that are not the same reason, and the two assertions below are those reasons.
                    "{ doubled: \"=after.qty * 2\", moved: \"=after.amount\" }"));

            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            List<Document> landed = awaitRows(mongo, targetUri);

            assertThat(landed)
                    .as("the computed column, per row, read back out of the target")
                    .extracting(document -> document.get("doubled"))
                    .containsExactlyInAnyOrderElementsOf(
                            NumericSource.QUANTITIES.stream().map(qty -> qty * 2).toList());

            assertThat(landed)
                    .as("the decimal every row carried, digit for digit and scale included")
                    .allSatisfy(document -> assertThat(decimalOf(document, "moved"))
                            .isEqualTo(NumericSource.AMOUNT));
        }
    }

    /**
     * The value as the target states it, refused rather than coerced if it is not the exact kind the
     * column was. A decimal that arrived as a float would convert into something equal-looking, so
     * accepting whatever is there and converting it would hide the very loss this reads it to find.
     */
    private static BigDecimal decimalOf(Document document, String field) {
        Object value = document.get(field);
        if (!(value instanceof Decimal128 decimal)) {
            throw new AssertionError(
                    "the decimal column arrived as " + (value == null ? "nothing" : value.getClass().getName())
                            + " rather than an exact decimal: " + value);
        }
        return decimal.bigDecimalValue();
    }

    /** Reads the target from outside the product until every seeded row is there. */
    private static List<Document> awaitRows(MongoEndpoints mongo, String targetUri) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = mongo.documents(EndpointAddress.uri(targetUri), NumericSource.TABLE);
            if (last.size() == NumericSource.ROWS) {
                return last;
            }
            sleep();
        }
        assertThat(last)
                .as("rows in the Mongo target %s after %d real MySQL rows",
                        NumericSource.TABLE, NumericSource.ROWS)
                .hasSize((int) NumericSource.ROWS);
        return last;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the rows to reach Mongo", e);
        }
    }
}
