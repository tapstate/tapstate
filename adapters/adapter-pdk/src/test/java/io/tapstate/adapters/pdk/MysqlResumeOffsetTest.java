package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.core.common.TapstateException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The offset belongs to the source log; its lookup namespace belongs to the reader opening it. */
class MysqlResumeOffsetTest {
    private static final String TYPE = "io.tapdata.connector.mysql.entity.MysqlStreamOffset";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static ConnectorClassLoader loader;
    private static Class<?> offsetType;
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

    @BeforeAll
    static void isolatedOffsetClass(@TempDir Path dir) throws Exception {
        Path jar = SyntheticJar.compileToJar(dir, TYPE, """
                package io.tapdata.connector.mysql.entity;
                public class MysqlStreamOffset {
                    public String name;
                    public java.util.Map<String, String> offset;
                }
                """);
        loader = ConnectorClassLoader.open(List.of(jar));
        offsetType = loader.load(TYPE);
    }

    @AfterAll
    static void closeLoader() throws Exception {
        if (loader != null) {
            loader.close();
        }
    }

    @Test
    void anotherReadersPositionKeepsEveryCoordinateAndUsesThisReadersNamespace() throws Exception {
        String coordinate = "{\"file\":\"binlog.000007\",\"pos\":9007199254740993,"
                + "\"row\":3,\"event\":2,\"server_id\":19,\"gtids\":\"recorded:1-99\",\"extra\":null}";
        String partition = "{\"server\":\"recorded\",\"scope\":\"keep-me\"}";
        Object original = offset("recorded", Map.of(partition, coordinate));
        String before = JSON.writeValueAsString(original);
        InMemoryStateMap state = new InMemoryStateMap();
        state.put("SERVER_NAME", "this-reader");

        Object rebound = adapt(original, state);

        assertThat(rebound.getClass()).isSameAs(offsetType);
        Map<?, ?> document = document(rebound);
        assertThat(document.get("name")).isEqualTo("this-reader");
        Map<?, ?> positions = (Map<?, ?>) document.get("offset");
        assertThat(positions).hasSize(1);
        assertThat(positions.values().stream().toList()).isEqualTo(List.of(coordinate));
        assertThat(JSON.readValue((String) positions.keySet().iterator().next(), Map.class))
                .containsExactlyInAnyOrderEntriesOf(Map.of("server", "this-reader", "scope", "keep-me"));
        assertThat(JSON.writeValueAsString(original)).isEqualTo(before);
        assertThat(state.get("SERVER_NAME")).isEqualTo("this-reader");
    }

    @Test
    void aMatchingReaderKeepsTheOriginalOffset() throws Exception {
        InMemoryStateMap state = new InMemoryStateMap();
        state.put("SERVER_NAME", "recorded");
        Object offset = offset("recorded", Map.of("{\"server\":\"recorded\"}", "opaque-coordinate"));
        assertThat(adapt(offset, state)).isSameAs(offset);
    }

    @Test
    void aNewNodeGetsItsOwnStableIdentityInsteadOfBorrowingTheOtherNodes() throws Exception {
        Object offset = offset("recorded", Map.of("{\"server\":\"recorded\"}", "coordinate"));
        InMemoryStateMap first = new InMemoryStateMap();
        InMemoryStateMap second = new InMemoryStateMap();

        Object initial = adapt(offset, first);
        Object again = adapt(offset, first);
        Object other = adapt(offset, second);

        assertThat(first.get("SERVER_NAME")).isInstanceOf(String.class).isNotEqualTo("recorded");
        assertThat(document(initial).get("name")).isEqualTo(first.get("SERVER_NAME"));
        assertThat(document(again).get("name")).isEqualTo(first.get("SERVER_NAME"));
        assertThat(document(other).get("name")).isNotEqualTo(first.get("SERVER_NAME"));
    }

    @Test
    void anOffsetWhosePartitionDisagreesWithItsNameIsRefusedInsteadOfGuessed() throws Exception {
        Object offset = offset("recorded", Map.of("{\"server\":\"someone-else\"}", "coordinate"));
        InMemoryStateMap state = new InMemoryStateMap();
        state.put("SERVER_NAME", "this-reader");
        assertThatThrownBy(() -> adapt(offset, state))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.POSITION_UNREADABLE);
    }

    @Test
    void multiplePartitionsCannotBeCollapsedOntoOneReader() throws Exception {
        Object offset = offset("recorded", Map.of(
                "{\"server\":\"recorded\"}", "first",
                "{ \"server\" : \"recorded\" }", "second"));
        InMemoryStateMap state = new InMemoryStateMap();
        state.put("SERVER_NAME", "this-reader");
        assertThatThrownBy(() -> adapt(offset, state)).isInstanceOf(TapstateException.class);
    }

    @Test
    void malformedOffsetsDoNotCreateAReaderIdentity() {
        for (Object offset : List.of(
                offset("", Map.of("{\"server\":\"recorded\"}", "coordinate")),
                offset("recorded", Map.of()),
                offset("recorded", Map.of("not-json", "coordinate")))) {
            InMemoryStateMap state = new InMemoryStateMap();
            assertThatThrownBy(() -> adapt(offset, state)).isInstanceOf(TapstateException.class);
            assertThat(state.get("SERVER_NAME")).isNull();
        }
    }

    @Test
    void anInvalidStoredIdentityIsNotOverwritten() {
        Object offset = offset("recorded", Map.of("{\"server\":\"recorded\"}", "coordinate"));
        for (Object invalid : List.of("", 12L)) {
            InMemoryStateMap state = new InMemoryStateMap();
            state.put("SERVER_NAME", invalid);
            assertThatThrownBy(() -> adapt(offset, state)).isInstanceOf(TapstateException.class)
                    .extracting(failure -> ((TapstateException) failure).code())
                    .isEqualTo(ConnectorError.STATE_UNREADABLE);
            assertThat(state.get("SERVER_NAME")).isEqualTo(invalid);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aCodedStateFailureKeepsItsOwnDiagnosis() {
        Object offset = offset("recorded", Map.of("{\"server\":\"recorded\"}", "coordinate"));
        TapstateException unreadableState = new TapstateException(ConnectorError.STATE_UNREADABLE,
                Map.of("detail", "invalid stored bytes"), null);
        KVMap<Object> state = (KVMap<Object>) Proxy.newProxyInstance(
                KVMap.class.getClassLoader(), new Class<?>[] {KVMap.class}, (proxy, method, args) -> {
                    throw unreadableState;
                });
        assertThatThrownBy(() -> MysqlResumeOffset.forReader("mysql", offset, state, () -> PARSER))
                .isSameAs(unreadableState);
    }

    @Test
    void otherConnectorOffsetsAndSnapshotSeamsAreNotInterpreted() {
        InMemoryStateMap state = new InMemoryStateMap();
        Map<String, Object> offset = Map.of("name", "other-reader", "offset", 12L);
        assertThat(MysqlResumeOffset.forReader("postgres", offset, state, () -> {
            throw new AssertionError("an unrelated offset must not resolve a JSON provider");
        })).isSameAs(offset);
        assertThat(MysqlResumeOffset.forReader("mysql", 123L, state, () -> {
            throw new AssertionError("a timestamp must not resolve a JSON provider");
        })).isEqualTo(123L);
        assertThat(MysqlResumeOffset.forReader("mysql", null, state, () -> {
            throw new AssertionError("an absent offset must not resolve a JSON provider");
        })).isNull();
        assertThat(state.get("SERVER_NAME")).isNull();
    }

    private static Object adapt(Object offset, InMemoryStateMap state) {
        return MysqlResumeOffset.forReader("mysql", offset, state, () -> PARSER);
    }

    private static Object offset(String name, Map<String, String> partitions) {
        return JSON.convertValue(Map.of("name", name, "offset", new LinkedHashMap<>(partitions)), offsetType);
    }

    private static Map<?, ?> document(Object offset) {
        return JSON.convertValue(offset, Map.class);
    }
}
