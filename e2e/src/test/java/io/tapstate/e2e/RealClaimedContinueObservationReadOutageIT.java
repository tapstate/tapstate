package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

/** Qualifies a real CONTINUE cold read outage separately from readable baseline absence. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.claimed-continue-read-io.jar", matches = ".+")
class RealClaimedContinueObservationReadOutageIT {
    private static final String PREFIX = "tapstate.e2e.claimed-continue-read-io.";
    @BeforeAll static void requireInputs() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Path.of(required("jar"))).isRegularFile();
        assertThat(Path.of(required("output")).isAbsolute()).isTrue();
        assertThat(required("sha256")).matches("[0-9a-f]{64}");
    }
    @Test @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aRealClaimedContinueColdReadOutageRetainsItsKnownFloorAndRecoversExactly() throws Exception {
        Map<String, Object> connectors = connectorInputs();
        BigDecimal cache = new BigDecimal(System.getProperty("tapstate.e2e.mongo.wired-tiger-cache-gb", "0.5"));
        assertThat(cache).as("the owned fault store preserves the configured half-gigabyte cache budget")
                .isEqualByComparingTo(new BigDecimal("0.5"));
        try (var store = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1",
                        "--wiredTigerCacheSizeGB=" + cache.stripTrailingZeros().toPlainString())) {
            store.start();
            RealClaimedRebuildHandoffCrashIT.qualifyContinueReadOutage(Path.of(required("jar")), required("sha256"),
                    Path.of(required("output")), database -> store.getReplicaSetUrl(database));
        }
        assertThat(connectorInputs()).as("all five actual connector artifacts remain immutable").isEqualTo(connectors);
    }
    private static Map<String, Object> connectorInputs() throws Exception {
        Path directory = ConnectorJars.pathFor("mysql").getParent(); Map<String, Object> actual = new LinkedHashMap<>();
        try (var files = Files.list(directory)) {
            List<Path> selected = files.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .sorted().limit(6).toList();
            assertThat(selected).as("the owned native directory supplies five immutable connector artifacts").hasSize(5);
            for (Path file : selected) { actual.put(file.getFileName().toString(), PipelineBenchmarkLiveRunIT.artifact(file)); }
        }
        return Map.copyOf(actual);
    }
    private static String required(String key) {
        String value = System.getProperty(PREFIX + key);
        if (value == null || value.isBlank()) { throw new AssertionError("missing native IO qualification input " + key); }
        return value;
    }
}
