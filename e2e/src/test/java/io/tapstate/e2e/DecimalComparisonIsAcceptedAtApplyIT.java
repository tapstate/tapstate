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
 * The witness that exact fixed-point DECIMAL comparisons in filter predicates are accepted
 * at apply while decimal arithmetic remains refused, evaluated against a real discovered column
 * whose type a real connector reported.
 *
 * <p>Companions with {@link DecimalIsRefusedAtApplyIT} and {@link LosslessNumericTypeIsAcceptedIT}.
 * A product that refused every decimal expression would pass {@link DecimalIsRefusedAtApplyIT} but
 * fail here.
 */
class DecimalComparisonIsAcceptedAtApplyIT {

    private static final String ACCEPTED_PIPELINE = "decimal_filter_pipeline";
    private static final String REFUSED_PIPELINE = "decimal_arithmetic_pipeline";
    private static final String TYPE_UNSUPPORTED = "dsl.row-expression-type-unsupported";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void comparingOnADecimalColumnIsAcceptedAtApply(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            NumericSource.seed(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
                ControlPlane control = NumericSource.connected(server, storage);
                Map<String, Object> config = NumericSource.config(mysql);

                control.discoverSchema(NumericSource.SOURCE_ID, "mysql", config);

                // Comparing a DECIMAL column with a literal and mixed integral column is accepted:
                control.apply(NumericSource.filterWorkspace(
                        config,
                        SharedMongo.replicaSetUrl("decimal_accept_target_" + suffix),
                        ACCEPTED_PIPELINE,
                        "after.amount > 25 && after.qty > 5"));

                assertThat(control.artifactIds())
                        .as("the pipeline is accepted and stored because exact decimal comparisons are supported")
                        .contains(NumericSource.SOURCE_ID, NumericSource.TARGET_ID, ACCEPTED_PIPELINE);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void decimalArithmeticInFilterRemainsRefusedAtApply(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            NumericSource.seed(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
                ControlPlane control = NumericSource.connected(server, storage);
                Map<String, Object> config = NumericSource.config(mysql);

                control.discoverSchema(NumericSource.SOURCE_ID, "mysql", config);

                // Arithmetic on DECIMAL remains refused at apply:
                ControlPlane.Refusal refusal = control.applyExpectingRefusal(NumericSource.filterWorkspace(
                        config,
                        SharedMongo.replicaSetUrl("decimal_refuse_target_" + suffix),
                        REFUSED_PIPELINE,
                        "after.amount * 2 > 0"));

                assertThat(refusal.code())
                        .as("arithmetic on exact decimal column must be refused")
                        .isEqualTo(TYPE_UNSUPPORTED);
                assertThat(control.artifactIds())
                        .doesNotContain(REFUSED_PIPELINE);
            }
        }
    }
}
