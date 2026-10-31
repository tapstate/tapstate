package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.TapConnector;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The same isolation contract witnessed against two REAL connector jars, gated on system properties
 * so it runs only when a connector dist is supplied and skips during normal builds (which carry no
 * connector jars). Point it at two different connector jars plus their entry class names:
 *
 * <pre>
 *   mvn -pl adapters/adapter-pdk test \
 *     -Dtapstate.pdk.it.jarA=/path/mysql-connector-x.jar   -Dtapstate.pdk.it.classA=io.tapdata.connector.mysql.MysqlConnector \
 *     -Dtapstate.pdk.it.jarB=/path/mongodb-connector-x.jar -Dtapstate.pdk.it.classB=io.tapdata.connector.mongodb.MongodbConnector
 * </pre>
 *
 * The deterministic isolation contract (same-name classes staying distinct, host masking, shared
 * contract, close) is covered by {@link ConnectorClassLoaderTest} with synthetic jars; this adds the
 * real-connector witness the plan calls for.
 */
class ConnectorClassLoaderRealJarTest {

    @Test
    void twoRealConnectorsLoadInIsolationOverOneSharedContract() throws Exception {
        String jarA = System.getProperty("tapstate.pdk.it.jarA");
        String classA = System.getProperty("tapstate.pdk.it.classA");
        String jarB = System.getProperty("tapstate.pdk.it.jarB");
        String classB = System.getProperty("tapstate.pdk.it.classB");
        assumeTrue(jarA != null && classA != null && jarB != null && classB != null,
                "no -Dtapstate.pdk.it.{jarA,classA,jarB,classB} — not a real-connector run, skipping");

        try (ConnectorClassLoader a = ConnectorClassLoader.open(List.of(Path.of(jarA)));
             ConnectorClassLoader b = ConnectorClassLoader.open(List.of(Path.of(jarB)))) {
            Class<? extends TapConnector> ca = a.loadConnectorClass(classA);
            Class<? extends TapConnector> cb = b.loadConnectorClass(classB);

            // Two different connectors, each on its own loader: no cross-pollution.
            assertThat(ca).isNotSameAs(cb);
            assertThat(ca.getClassLoader()).isNotSameAs(cb.getClassLoader());

            // Both bind to the one PDK contract shared from the host, not per-connector copies.
            Class<?> contractViaA = a.load("io.tapdata.pdk.apis.TapConnector");
            Class<?> contractViaB = b.load("io.tapdata.pdk.apis.TapConnector");
            assertThat(contractViaA).isSameAs(TapConnector.class).isSameAs(contractViaB);
            // New entity classes must resolve to the host even if a connector bundles another copy.
            assertThat(a.load("io.tapdata.entity.schema.type.TapFloat"))
                    .isSameAs(io.tapdata.entity.schema.type.TapFloat.class);
            assertThat(b.load("io.tapdata.entity.schema.type.TapDouble"))
                    .isSameAs(io.tapdata.entity.schema.type.TapDouble.class);
            assertThat(a.load("io.tapdata.entity.schema.value.TapFloatValue"))
                    .isSameAs(io.tapdata.entity.schema.value.TapFloatValue.class);
            assertThat(b.load("io.tapdata.entity.schema.value.TapDoubleValue"))
                    .isSameAs(io.tapdata.entity.schema.value.TapDoubleValue.class);
        }
    }

    @Test
    void realConnectorsInitializeAgainstTheHostRuntimeAndCommonLibrary() throws Exception {
        for (String suffix : new String[] {"A", "B"}) {
            String jar = System.getProperty("tapstate.pdk.it.jar" + suffix);
            String className = System.getProperty("tapstate.pdk.it.class" + suffix);
            assumeTrue(jar != null && className != null, "real connector artifacts were not supplied");
            String version;
            try (var archive = new java.util.jar.JarFile(jar)) {
                version = archive.getManifest().getMainAttributes().getValue("PDK-API-Version");
            }
            assertThat(version).as("the artifact's real declaration").isNotBlank();
            ConnectorRef ref = new ConnectorRef(List.of(Path.of(jar)), className, version, null);
            try (PdkConnector connector = PdkConnector.open("real-" + suffix, ref, Map.of())) {
                assertThat(connector.connector().getClass().getName()).isEqualTo(className);
                assertThat(connector.context()).isNotNull();
            }
        }
    }
}
