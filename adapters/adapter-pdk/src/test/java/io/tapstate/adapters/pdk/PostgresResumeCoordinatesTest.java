package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapstate.core.common.TapstateException;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The coordinate rule runs without a connector jar; the native-bean audit remains separate. */
class PostgresResumeCoordinatesTest {
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

    @Test
    void aHeartbeatChangesOnlyTheReadersProcessedLsn() throws Exception {
        String source = """
                {"lsn_proc":9007199254740993,"lsn_commit":9007199254740993,"lsn":9007199254740993,
                 "ts_usec":1791367620623727,"futureCoordinate":{"opaque":"keep-me","large":9007199254740993}}
                """;
        Map<String, Object> original = document(source);
        String stored = JSON.writeValueAsString(original);

        Map<String, Object> reader = prepare(original);

        assertThat(reader).isNotSameAs(original).isInstanceOf(original.getClass());
        assertThat(coordinates(reader)).containsEntry("lsn_proc", 9007199254740992L)
                .containsEntry("lsn_commit", 9007199254740993L).containsEntry("lsn", 9007199254740993L)
                .containsEntry("ts_usec", 1791367620623727L)
                .containsEntry("futureCoordinate", Map.of("opaque", "keep-me", "large", 9007199254740993L));
        Map<String, Object> expected = new LinkedHashMap<>(original);
        expected.put("sourceOffset", reader.get("sourceOffset"));
        assertThat(reader).containsExactlyInAnyOrderEntriesOf(expected);
        assertThat(JSON.writeValueAsString(original)).isEqualTo(stored);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"lsn_proc\":30729648,\"lsn_commit\":30729648,\"txId\":739}",
            "{\"lsn_proc\":30729648,\"lsn_commit\":30729536}",
            "{\"lsn_proc\":30729648}",
            "{\"lsn_commit\":30729648}",
            "{\"lsn_proc\":0,\"lsn_commit\":0}",
            "{\"lsn_proc\":1.5,\"lsn_commit\":1.5}",
            "{\"lsn_proc\":1,\"lsn_commit\":1.5}",
            "{\"lsn_proc\":\"30729648\",\"lsn_commit\":\"30729648\"}",
            "{\"lsn_proc\":30729648,\"lsn_commit\":\"30729648\"}",
            "{}"})
    void transactionPositionsAndUnrecognizedCoordinatesRetainTheirIdentity(String source) throws Exception {
        Map<String, Object> original = document(source);
        String stored = JSON.writeValueAsString(original);

        assertThat(prepare(original)).isSameAs(original);
        assertThat(JSON.writeValueAsString(original)).isEqualTo(stored);
    }

    @Test
    void anExplicitlyNullTransactionIdIsAlsoAPostCommitBoundary() throws Exception {
        Map<String, Object> original = document("{\"lsn_proc\":30729648,\"lsn_commit\":30729648,\"txId\":null}");

        assertThat(coordinates(prepare(original))).containsEntry("lsn_proc", 30729647)
                .containsEntry("lsn_commit", 30729648).containsEntry("txId", null);
        assertThat(original).containsEntry("sourceOffset",
                "{\"lsn_proc\":30729648,\"lsn_commit\":30729648,\"txId\":null}");
    }

    @Test
    void unsignedLsnBitsArePreservedAcrossTheSignedLongBoundary() throws Exception {
        Map<String, Object> original = document(
                "{\"lsn_proc\":-9223372036854775808,\"lsn_commit\":-9223372036854775808}");

        assertThat(coordinates(prepare(original))).containsEntry("lsn_proc", Long.MAX_VALUE)
                .containsEntry("lsn_commit", Long.MIN_VALUE);
    }

    @Test
    void freshOrUnrecognizedSourceOffsetsRetainTheirIdentity() {
        for (Object source : new Object[] {null, "", " ", 7, Map.of("lsn_proc", 1)}) {
            Map<String, Object> original = document(source);
            assertThat(prepare(original)).isSameAs(original);
        }
        assertThat(prepare(new LinkedHashMap<>())).isEmpty();
    }

    @Test
    void unrelatedOffsetTypesAndTimestampStartsDoNotResolveAParser() {
        for (Object original : List.of(123L, "opaque", document("{\"lsn_proc\":1,\"lsn_commit\":1}"))) {
            assertThat(PostgresResumeOffset.forReader("postgres", original, () -> {
                throw new AssertionError("this offset must not resolve a JSON provider");
            })).isSameAs(original);
        }
        assertThat(PostgresResumeOffset.forReader("postgres", null, () -> {
            throw new AssertionError("a fresh start must not resolve a JSON provider");
        })).isNull();
    }

    @Test
    void parserFailuresUseTheExistingPositionRefusalAndPreserveTheCause() {
        Map<String, Object> original = document("{\"lsn_proc\":1,\"lsn_commit\":1}");
        IllegalStateException failure = new IllegalStateException("cannot read the native offset");

        assertThatThrownBy(() -> PostgresResumeOffset.prepareReaderCopy("postgres", original, () -> {
            throw failure;
        })).isInstanceOf(TapstateException.class).hasCause(failure)
                .satisfies(error -> {
                    TapstateException coded = (TapstateException) error;
                    assertThat(coded.code()).isEqualTo(ConnectorError.POSITION_UNREADABLE);
                    assertThat(coded.args()).containsEntry("connector", "postgres")
                            .containsEntry("detail", "cannot prepare PostgreSQL resume offset: " + failure.getMessage());
                });
    }

    @Test
    void anAlreadyCodedParserFailureIsPropagatedUnchanged() {
        TapstateException coded = new TapstateException(ConnectorError.STATE_UNREADABLE,
                Map.of("detail", "invalid stored bytes"), null);

        assertThatThrownBy(() -> PostgresResumeOffset.prepareReaderCopy("postgres", document(null), () -> {
            throw coded;
        })).isSameAs(coded);
    }

    @Test
    void unreadableCoordinatesAreRefusedWithoutChangingTheStoredDocument() {
        Map<String, Object> original = document("not JSON");

        assertThatThrownBy(() -> prepare(original)).isInstanceOf(TapstateException.class)
                .satisfies(error -> assertThat(((TapstateException) error).code())
                        .isEqualTo(ConnectorError.POSITION_UNREADABLE));
        assertThat(original).containsEntry("sourceOffset", "not JSON");
    }

    private static Map<String, Object> document(Object source) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("sourceOffset", source);
        document.put("sortString", "retained-order");
        document.put("offsetValue", 7);
        document.put("futureField", Map.of("opaque", "keep-me"));
        return document;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> prepare(Map<String, Object> document) {
        return (Map<String, Object>) PostgresResumeOffset.prepareReaderCopy("postgres", document, () -> PARSER);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> coordinates(Map<String, Object> document) throws Exception {
        return JSON.readValue((String) document.get("sourceOffset"), Map.class);
    }
}
