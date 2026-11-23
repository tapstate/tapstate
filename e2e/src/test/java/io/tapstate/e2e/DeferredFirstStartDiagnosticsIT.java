package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparationInputs;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class DeferredFirstStartDiagnosticsIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @TempDir java.nio.file.Path work;

    @Test
    void replacementProcessRendersTheRetainedRejection() {
        String config = CONFIG.getReplicaSetUrl("first_start");
        String cache = CACHE.getReplicaSetUrl("first_start_cache");
        String cluster = "deferred-diagnostic-probe";
        NativeMongoStorageFixture.prepare(config,
                new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, cache, "first_start_cache", null),
                CACHE.getReplicaSetUrl("first_start_target"), cluster);
        try (var domain = MongoConfigurationDomain.open(new MongoConnectionSettings(config, null, Duration.ofSeconds(5)), cluster)) {
            var resource = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
            domain.artifacts().create(resource);
            var binding = domain.bindings().cachePreparation().orElseThrow().businessScope();
            var scope = new PipelineIntentScope(cluster, binding.bindingVersion(), binding.cacheIncarnation(), "flow", 1);
            String hash = CanonicalHash.of(resource);
            var intent = new DesiredState("flow", PipelineState.RUNNING, hash);
            var intents = domain.intents();
            assertThat(intents.compareAndSetStart(scope, 0, intent, new PipelinePreparationInputs(hash, Map.of(), Map.of(), List.of()))).isTrue();
            intents.failStart(intents.pendingStart("flow").orElseThrow(), IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", "doc-42", "field", "revision"));
        }
        var original = start(config, cluster);
        try (original) {
            verify(original);
        }
        assertThat(original.isAlive()).isFalse();
        try (var replacement = start(config, cluster)) {
            verify(replacement);
        }
    }

    private RealProcessServer start(String config, String cluster) {
        return RealProcessServer.startPrepared(config, "127.0.0.1", port -> List.of(
                "--tapstate.cluster.id=" + cluster, "--tapstate.cluster.node-id=deferred-probe",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds")));
    }

    private static void verify(RealProcessServer server) {
        var control = new ControlPlane(server.baseUrl());
        control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
        var status = control.pipelineStatus("flow");
        assertThat(status.get("failure")).isInstanceOfSatisfying(Map.class, failure -> {
            assertThat(failure.get("code")).isEqualTo("TS0480004");
            assertThat(failure.get("params")).isEqualTo(Map.of("id", "doc-42", "field", "revision"));
            assertThat(String.valueOf(failure.get("message"))).contains("doc-42", "revision");
        });
    }
}
