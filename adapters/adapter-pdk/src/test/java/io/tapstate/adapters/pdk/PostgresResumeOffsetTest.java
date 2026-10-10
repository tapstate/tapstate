package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapstate.core.common.TapstateException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Audits reader-only adaptation against the real PostgreSQL offset bean, without opening a database. */
class PostgresResumeOffsetTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonParser PARSER = (JsonParser) Proxy.newProxyInstance(
            JsonParser.class.getClassLoader(), new Class<?>[] {JsonParser.class}, (proxy, method, args) -> {
                return switch (method.getName()) {
                    case "toJson" -> JSON.writeValueAsString(args[0]);
                    case "fromJson" -> JSON.readValue((String) args[0], (Class<?>) args[1]);
                    case "fromJsonObject" -> {
                        DataMap data = new DataMap();
                        data.putAll(JSON.readValue((String) args[0], Map.class));
                        yield data;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            });
    private static ConnectorClassLoader loader;
    private static Class<?> type;

    @BeforeAll
    static void realOffsetClass() throws Exception {
        String configured = System.getProperty("tapstate.pdk.it.postgres-jar");
        String directory = System.getProperty("tapstate.e2e.connectors-dir");
        assumeTrue(configured != null || directory != null, "no real PostgreSQL connector jar supplied");
        Path jar;
        if (configured != null) {
            jar = Path.of(configured);
        } else {
            try (var files = Files.list(Path.of(directory))) {
                List<Path> jars = files.filter(path -> path.getFileName().toString().startsWith("postgres"))
                        .filter(path -> path.getFileName().toString().endsWith(".jar")).toList();
                assertThat(jars).hasSize(1);
                jar = jars.getFirst();
            }
        }
        loader = ConnectorClassLoader.open(List.of(jar));
        type = loader.load("io.tapdata.connector.postgres.cdc.offset.PostgresOffset");
    }

    @AfterAll
    static void closeLoader() {
        if (loader != null) {
            loader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"postgres", "aliyun-rds-postgres", "polar-db-postgres", "tencent-db-postgres"})
    void aHeartbeatChangesOnlyTheReadersProcessedLsn(String connectorId) throws Exception {
        String source = """
                {"lsn_proc":9007199254740993,"lsn_commit":9007199254740993,"lsn":9007199254740993,
                 "ts_usec":1791367620623727,"futureCoordinate":{"opaque":"keep-me","large":9007199254740993}}
                """;
        Object original = offset(source);
        String token = ConnectorOffsetCodec.toToken(connectorId, original);

        Object reader = PostgresResumeOffset.forReader(connectorId, original, () -> PARSER);

        assertThat(reader).isNotSameAs(original).isInstanceOf(type);
        assertThat(coordinates(reader)).containsEntry("lsn_proc", 9007199254740992L)
                .containsEntry("lsn_commit", 9007199254740993L).containsEntry("lsn", 9007199254740993L)
                .containsEntry("ts_usec", 1791367620623727L)
                .containsEntry("futureCoordinate", Map.of("opaque", "keep-me", "large", 9007199254740993L));
        assertThat(type.getMethod("getSortString").invoke(reader)).isEqualTo("retained-order");
        assertThat(type.getMethod("getOffsetValue").invoke(reader)).isEqualTo(7L);
        assertThat(ConnectorOffsetCodec.toToken(connectorId, original)).isEqualTo(token);
        Object restored = ConnectorOffsetCodec.fromToken(connectorId, token, type.getClassLoader());
        assertThat(type.getMethod("getSourceOffset").invoke(restored)).isEqualTo(source);
    }

    @Test
    void transactionPositionsAndUnrecognizedCoordinatesRemainUnchanged() throws Exception {
        for (String source : List.of(
                "{\"lsn_proc\":30729648,\"lsn_commit\":30729648,\"txId\":739}",
                "{\"lsn_proc\":30729648,\"lsn_commit\":30729536}",
                "{\"lsn_proc\":30729648}",
                "{\"lsn_commit\":30729648}",
                "{\"lsn_proc\":0,\"lsn_commit\":0}",
                "{\"lsn_proc\":1.5,\"lsn_commit\":1.5}",
                "{\"lsn_proc\":\"30729648\",\"lsn_commit\":\"30729648\"}",
                "{}")) {
            Object original = offset(source);
            String token = ConnectorOffsetCodec.toToken("postgres", original);
            assertThat(PostgresResumeOffset.forReader("postgres", original, () -> PARSER)).isSameAs(original);
            assertThat(ConnectorOffsetCodec.toToken("postgres", original)).isEqualTo(token);
        }
    }

    @Test
    void anExplicitlyNullTransactionIdIsAlsoAPostCommitBoundary() throws Exception {
        Object original = offset("{\"lsn_proc\":30729648,\"lsn_commit\":30729648,\"txId\":null}");
        Object reader = PostgresResumeOffset.forReader("postgres", original, () -> PARSER);
        assertThat(coordinates(reader)).containsEntry("lsn_proc", 30729647)
                .containsEntry("lsn_commit", 30729648).containsEntry("txId", null);
    }

    @Test
    void unsignedLsnBitsArePreservedAcrossTheSignedLongBoundary() throws Exception {
        Object original = offset("{\"lsn_proc\":-9223372036854775808,\"lsn_commit\":-9223372036854775808}");
        Object reader = PostgresResumeOffset.forReader("postgres", original, () -> PARSER);
        assertThat(coordinates(reader)).containsEntry("lsn_proc", Long.MAX_VALUE)
                .containsEntry("lsn_commit", Long.MIN_VALUE);
    }

    @Test
    void freshNativeOffsetsKeepTheirOriginalRepresentation() throws Exception {
        for (String source : new String[] {null, "", " "}) {
            Object original = offset(source);
            assertThat(PostgresResumeOffset.forReader("postgres", original, () -> PARSER)).isSameAs(original);
        }
    }

    @Test
    void unrelatedOffsetTypesAndTimestampStartsDoNotResolveAParser() {
        for (Object original : List.of(123L, "opaque", Map.of("lsn_proc", 30729648, "lsn_commit", 30729648))) {
            assertThat(PostgresResumeOffset.forReader("postgres", original, unusedParser())).isSameAs(original);
        }
        assertThat(PostgresResumeOffset.forReader("postgres", null, unusedParser())).isNull();
    }

    @Test
    void parserFailuresUseTheExistingPositionRefusal() throws Exception {
        Object original = offset("{\"lsn_proc\":30729648,\"lsn_commit\":30729648}");
        assertThatThrownBy(() -> PostgresResumeOffset.forReader("postgres", original, () -> {
            throw new IllegalStateException("cannot read the native offset");
        })).isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException) error).code()).isEqualTo(ConnectorError.POSITION_UNREADABLE);
        TapstateException coded = new TapstateException(ConnectorError.STATE_UNREADABLE,
                Map.of("detail", "invalid stored bytes"), null);
        assertThatThrownBy(() -> PostgresResumeOffset.forReader("postgres", original, () -> {
            throw coded;
        })).isSameAs(coded);
    }

    private static Object offset(String source) throws Exception {
        Object value = type.getConstructor().newInstance();
        type.getMethod("setSourceOffset", String.class).invoke(value, source);
        type.getMethod("setSortString", String.class).invoke(value, "retained-order");
        type.getMethod("setOffsetValue", Long.class).invoke(value, 7L);
        return value;
    }

    private static Map<String, Object> coordinates(Object offset) throws Exception {
        return JSON.readValue((String) type.getMethod("getSourceOffset").invoke(offset), Map.class);
    }

    private static Supplier<JsonParser> unusedParser() {
        return () -> { throw new AssertionError("this offset must not resolve a JSON provider"); };
    }
}
