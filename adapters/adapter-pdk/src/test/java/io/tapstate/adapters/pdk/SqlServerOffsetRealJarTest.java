package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exercises the real connector's bean and frozen PDK JSON provider without opening a database. */
class SqlServerOffsetRealJarTest {
    @Test
    void nativeJsonPreservesTheRealOffsetsLogCoordinatesAndDdlBytes() throws Throwable {
        String configured = System.getProperty("tapstate.pdk.it.sqlserver-jar");
        assumeTrue(configured != null, "no SQL Server connector jar supplied for the native offset audit");
        Path jar = Path.of(configured);
        String api;
        String spec;
        try (JarFile file = new JarFile(jar.toFile())) {
            api = file.getManifest().getMainAttributes().getValue("PDK-API-Version");
            try (var input = file.getInputStream(file.getJarEntry("mssql-spec.json"))) {
                spec = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        ConnectorRef ref = new ConnectorRef(List.of(jar), "io.tapdata.connector.mssql.MssqlConnector", api, null, spec);
        try (PdkConnector connector = PdkConnector.open("sqlserver", ref, Map.of())) {
            ClassLoader caller = Thread.currentThread().getContextClassLoader();
            connector.underLoader(() -> {
                ClassLoader loader = connector.connector().getClass().getClassLoader();
                assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(loader);
                Class<?> type = loader.loadClass("io.tapdata.connector.mssql.cdc.CdcOffset");
                JsonParser parser = InstanceFactory.instance(JsonParser.class);
                Object original = parser.fromJson("""
                        {"currentStartLSN":"0000002A000001B00003", "tablesOffset":{
                          "dbo.orders":{"lsn":"00000029000000F00001","sequence":9007199254740993}},
                          "ddlOffset":"AAH+/w=="}
                        """, type);
                String before = parser.toJson(original);
                Object represented = SqlServerOffset.forStorage("sqlserver", original, () -> parser);
                String token = ConnectorOffsetCodec.toToken("sqlserver", represented);
                Object decoded = ConnectorOffsetCodec.fromToken("sqlserver", token, loader);
                assertThat(decoded).isInstanceOf(String.class).isEqualTo(before);
                // This is the same native parsing operation used by the connector's streamRead branch.
                Object restored = parser.fromJson((String) decoded, type);
                assertThat(type.getMethod("getCurrentStartLSN").invoke(restored)).isEqualTo("0000002A000001B00003");
                assertThat(type.getMethod("getTablesOffset").invoke(restored)).isEqualTo(Map.of(
                        "dbo.orders", Map.of("lsn", "00000029000000F00001", "sequence", 9007199254740993L)));
                assertThat((byte[]) type.getMethod("getDdlOffset").invoke(restored))
                        .containsExactly((byte) 0, (byte) 1, (byte) 254, (byte) 255);
                assertThat(parser.toJson(original)).isEqualTo(before);
                return null;
            });
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(caller);
        }
    }
}
