package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class SrvConnectionWitnessTest {

    @ParameterizedTest
    @EnumSource(AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.Stage.class)
    void command_failure_retains_the_fixed_stage_and_numeric_code_only(
            AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.Stage stage) {
        var response = new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(13))
                .append("codeName", new BsonString("credential-secret"))
                .append("errmsg", new BsonString("mongodb+srv://user:credential-secret@identity.invalid/private"));
        var failure = new MongoCommandException(response, new ServerAddress("identity.invalid"));

        var reported = AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.failureAt(stage, failure);

        assertThat(reported).hasMessage("Controlled SRV connection failed at " + stage
                + ": MongoCommandException code=13").hasNoCause();
        assertThat(reported.getSuppressed()).isEmpty();
    }

    @Test
    void witness_database_names_fit_the_live_cluster_limit_without_truncating_the_identity() {
        var databases = AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.databases(new UUID(-1L, -1L));

        assertThat(databases.metadata().getBytes(StandardCharsets.UTF_8)).hasSizeLessThanOrEqualTo(38);
        assertThat(databases.views().getBytes(StandardCharsets.UTF_8)).hasSizeLessThanOrEqualTo(38);
        assertThat(databases.metadata()).isNotEqualTo(databases.views()).endsWith("ffffffffffffffffffffffffffffffff");
        assertThat(databases.views()).endsWith("ffffffffffffffffffffffffffffffff");
    }

    @ParameterizedTest
    @MethodSource("successfulPingValues")
    void a_successful_ping_accepts_each_numeric_bson_representation(Number ok) {
        AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.assertSuccessfulPing(new Document("ok", ok));
    }

    static Stream<Number> successfulPingValues() {
        return Stream.of(1, 1L, 1.0);
    }

    @ParameterizedTest
    @MethodSource("unsuccessfulPingValues")
    void a_non_successful_or_non_numeric_ping_is_never_accepted(Object ok) {
        assertThatThrownBy(() -> AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.assertSuccessfulPing(
                new Document("ok", ok))).isInstanceOf(AssertionError.class);
    }

    static Stream<Object> unsuccessfulPingValues() {
        return Stream.of(0, 0L, 0.0, 1.5, "1", true, null);
    }

    @Test
    void a_non_numeric_ping_never_renders_the_response() {
        var response = new Document("ok", "mongodb+srv://user:private-response@identity.invalid/private");

        assertThatThrownBy(() -> AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.assertSuccessfulPing(response))
                .isInstanceOf(AssertionError.class).hasMessageNotContaining("private-response")
                .hasMessageNotContaining("identity.invalid").hasNoCause();
    }

    @Test
    void invalid_configuration_keeps_the_stage_without_retaining_the_input() {
        var failure = new IllegalArgumentException("mongodb+srv://user:credential-secret@identity.invalid/private");

        var reported = AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.failureAt(
                AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT.Stage.VIEWS_URI, failure);

        assertThat(reported).hasMessage("Controlled SRV connection failed at VIEWS_URI: IllegalArgumentException")
                .hasNoCause();
        assertThat(reported.getSuppressed()).isEmpty();
    }
}
