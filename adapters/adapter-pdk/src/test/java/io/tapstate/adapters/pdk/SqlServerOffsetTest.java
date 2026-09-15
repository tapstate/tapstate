package io.tapstate.adapters.pdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.utils.JsonParser;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlServerOffsetTest {
    private static final String TYPE = "io.tapdata.connector.mssql.cdc.CdcOffset";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String POSITION = """
            {"currentStartLSN":"0000002A000001B00003", "tablesOffset":{
              "dbo.orders":{"lsn":"00000029000000F00001","sequence":9007199254740993}},
              "ddlOffset":"AAH+/w==", "futureCoordinate":"keep-this"}
            """;
    private static final JsonParser PARSER = (JsonParser) Proxy.newProxyInstance(
            JsonParser.class.getClassLoader(), new Class<?>[]{JsonParser.class}, (proxy, method, args) -> {
                if (method.getName().equals("toJson")) return JSON.writeValueAsString(args[0]);
                if (method.getName().equals("fromJson")) return JSON.readValue((String) args[0], Map.class);
                throw new UnsupportedOperationException(method.getName());
            });
    private static ConnectorClassLoader loader;
    private static Class<?> type;

    @BeforeAll
    static void isolatedOffset(@TempDir Path directory) throws Exception {
        Path jar = SyntheticJar.compileToJar(directory, TYPE, """
                package io.tapdata.connector.mssql.cdc;
                public class CdcOffset {
                    public String currentStartLSN;
                    public java.util.Map<String, Object> tablesOffset;
                    public byte[] ddlOffset;
                    public String futureCoordinate;
                }
                """);
        loader = ConnectorClassLoader.open(List.of(jar));
        type = loader.load(TYPE);
    }

    @AfterAll
    static void closeLoader() throws Exception {
        if (loader != null) loader.close();
    }

    @Test
    void aNativeOffsetUsesTheConnectorsJsonFormWithoutDroppingCoordinates() throws Exception {
        Object original = JSON.readValue(POSITION, type);
        Object represented = SqlServerOffset.forStorage("sqlserver", original, () -> PARSER);
        assertThat(represented).isInstanceOf(String.class);
        String token = ConnectorOffsetCodec.toToken("sqlserver", represented);
        Object decoded = ConnectorOffsetCodec.fromToken("sqlserver", token, type.getClassLoader());
        Object nativeOffset = JSON.readValue((String) decoded, type);
        assertThat(JSON.readTree(JSON.writeValueAsString(nativeOffset))).isEqualTo(JSON.readTree(POSITION));
        assertThat((byte[]) type.getField("ddlOffset").get(nativeOffset))
                .containsExactly((byte) 0, (byte) 1, (byte) 254, (byte) 255);
        assertThat(JSON.readTree(JSON.writeValueAsString(original))).isEqualTo(JSON.readTree(POSITION));
        assertThat(token).isEqualTo(ConnectorOffsetCodec.toToken("sqlserver", JSON.writeValueAsString(original)));
    }

    @Test
    void aNativeOffsetWithoutAnInitialLsnCannotBeStored() throws Exception {
        for (String empty : new String[]{null, "", " "}) {
            Object original = type.getConstructor().newInstance();
            type.getField("currentStartLSN").set(original, empty);
            assertThatThrownBy(() -> SqlServerOffset.forStorage("sqlserver", original, () -> PARSER))
                    .isInstanceOf(TapstateException.class)
                    .hasMessageContaining("SQL Server CDC has not published an initial LSN")
                    .extracting(error -> ((TapstateException) error).code())
                    .isEqualTo(ConnectorError.POSITION_UNRENDERABLE);
        }
    }

    @Test
    void existingNativeStringsAndOtherSerializableOffsetsStayUntouched() {
        for (Object original : List.of(POSITION, 12L, Map.of("lsn", "retained"))) {
            assertThat(SqlServerOffset.forStorage("sqlserver", original, unusedParser())).isSameAs(original);
        }
        assertThat(SqlServerOffset.forStorage("sqlserver", null, unusedParser())).isNull();
        String token = ConnectorOffsetCodec.toToken("sqlserver", POSITION);
        assertThat(ConnectorOffsetCodec.fromToken("sqlserver", token, getClass().getClassLoader())).isEqualTo(POSITION);
    }

    @Test
    void theExactClassIsNotReinterpretedForAnotherConnector() throws Exception {
        Object original = JSON.readValue(POSITION, type);
        assertThat(SqlServerOffset.forStorage("postgres", original, unusedParser())).isSameAs(original);
    }

    @Test
    void unknownObjectsRetainTheExistingCodedRefusal() {
        Object unknown = new Object();
        Object represented = SqlServerOffset.forStorage("sqlserver", unknown, unusedParser());
        assertThat(represented).isSameAs(unknown);
        assertThatThrownBy(() -> ConnectorOffsetCodec.toToken("sqlserver", represented))
                .isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException) error).code())
                .isEqualTo(ConnectorError.POSITION_UNRENDERABLE);
    }

    @Test
    void jsonProviderFailuresRemainPositionRenderingFailures() throws Exception {
        Object original = JSON.readValue(POSITION, type);
        assertThatThrownBy(() -> SqlServerOffset.forStorage("sqlserver", original, () -> {
            throw new IllegalStateException("cannot serialize the native offset");
        })).isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException) error).code())
                .isEqualTo(ConnectorError.POSITION_UNRENDERABLE);
    }

    @Test
    void anEmptyJsonRepresentationCannotBecomeAValidPosition() throws Exception {
        Object original = JSON.readValue(POSITION, type);
        for (String empty : new String[]{null, "", " "}) {
            JsonParser broken = (JsonParser) Proxy.newProxyInstance(JsonParser.class.getClassLoader(),
                    new Class<?>[]{JsonParser.class}, (proxy, method, args) -> empty);
            assertThatThrownBy(() -> SqlServerOffset.forStorage("sqlserver", original, () -> broken))
                    .isInstanceOf(TapstateException.class)
                    .extracting(error -> ((TapstateException) error).code())
                    .isEqualTo(ConnectorError.POSITION_UNRENDERABLE);
        }
    }

    private static Supplier<JsonParser> unusedParser() {
        return () -> { throw new AssertionError("this offset must not request JSON conversion"); };
    }
}
