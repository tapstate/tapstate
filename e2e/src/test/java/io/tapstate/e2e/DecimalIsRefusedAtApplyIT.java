package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that computing on an exact fixed-point column is refused while the author is still
 * writing, against a real column whose type a real connector reported.
 *
 * <p>The type has to come from a real database. A synthetic source declares every column as text, so a
 * specification written against one cannot tell a refusal of decimal arithmetic from a refusal of
 * everything - the discriminating fact is that this column is a decimal and the connector said so.
 *
 * <p>Two assertions, because either alone is satisfied by an outcome this test exists to exclude.
 * Asserting only that the apply failed is satisfied by any failure at all, including a connector that
 * would not load; asserting only the code leaves "refused, having already filed the pipeline"
 * indistinguishable from a clean refusal, and a half-applied workspace is the state an author would
 * then have to unpick by hand. The listing is read over every artifact the batch submitted rather than
 * the pipeline alone: the source and target are the ones written first, so a batch that filed half of
 * itself is half-filed in exactly the artifacts a pipeline-only assertion never looks at.
 *
 * <p>Discovery runs before the apply, not after it, which is the order the obligation on reading a row
 * field imposes. So the refusal here cannot be the absent-schema refusal wearing a different name: the
 * schema is present, and the only thing left to object to is the type in it.
 *
 * <p>Its companion is {@link LosslessNumericTypeIsAcceptedIT}, and neither is complete without the
 * other. A product that refused every numeric expression would pass this one.
 *
 * <p>Gated on Docker and on a directory of real connector jars, the same as the other real-connector
 * witnesses. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors
 * </pre>
 */
class DecimalIsRefusedAtApplyIT {

    private static final String PIPELINE_ID = "decimal_pipeline";
    private static final String TYPE_UNSUPPORTED = "dsl.row-expression-type-unsupported";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void computingOnADecimalColumnIsRefusedAndTheWorkspaceKeepsNoneOfIt(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            NumericSource.seed(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
                ControlPlane control = NumericSource.connected(server, storage);
                Map<String, Object> config = NumericSource.config(mysql);

                // Discovered first: the schema this refusal is entitled to consult has to be there, or
                // the refusal would be the one about a source nobody discovered.
                control.discoverSchema(NumericSource.SOURCE_ID, "mysql", config);

                ControlPlane.Refusal refusal = control.applyExpectingRefusal(NumericSource.workspace(
                        config,
                        SharedMongo.replicaSetUrl("decimal_refused_target_" + suffix),
                        PIPELINE_ID,
                        // Arithmetic, not a move. Carrying a decimal through is allowed and loses
                        // nothing; computing on one is what cannot survive the type the expression
                        // language would have to borrow.
                        "{ scaled: \"=after.amount * 2\" }"));

                assertThat(refusal.code())
                        .as("the code refusing arithmetic over a column whose type the expression "
                                + "language has no lossless equivalent for")
                        .isEqualTo(TYPE_UNSUPPORTED);
                assertThat(control.artifactIds())
                        .as("what the server holds after refusing the batch - a refusal that filed any "
                                + "of it leaves the author a workspace to unpick by hand")
                        .doesNotContain(NumericSource.SOURCE_ID, NumericSource.TARGET_ID, PIPELINE_ID);
            }
        }
    }
}
