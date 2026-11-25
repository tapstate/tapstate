package io.tapstate.e2e;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot observe Mongo chunk reads during plugin cache staging. */
@RequiresDocker
class ASeedPopulatesThePluginCacheWithoutDownloadingItAgainIT {
    @Test
    void aRegisteredSeedIsLoadableThroughHttpWithoutASecondGridFsTransfer(@TempDir Path work) throws Exception {
        Path seed = E2eConnectorJar.buildInto(Files.createDirectories(work.resolve("fixture")));
        String hash;
        try (var input = Files.newInputStream(seed)) { hash = ContentHash.of(input); }
        Path seeds = Files.createDirectories(work.resolve("seeds"));
        Files.copy(seed, seeds.resolve(E2eConnectorJar.CONNECTOR_ID + "-connector.jar"));
        Path cache = work.resolve("plugins");
        Path source = Files.createDirectories(work.resolve("source"));
        Files.writeString(source.resolve("orders.csv"), "id,amount\n1,12.00\n");
        Logger logger = (Logger) LoggerFactory.getLogger("org.mongodb.driver.protocol.command");
        Level previous = logger.getLevel();
        boolean additive = logger.isAdditive();
        AtomicInteger chunkReads = new AtomicInteger();
        AtomicInteger chunkWrites = new AtomicInteger();
        var observer = new AppenderBase<ILoggingEvent>() {
            @Override protected void append(ILoggingEvent event) {
                String message = event.getFormattedMessage();
                if (message.contains("\"find\": \"connector_artifacts.chunks\"")) chunkReads.incrementAndGet();
                if (message.contains("\"insert\": \"connector_artifacts.chunks\"")) chunkWrites.incrementAndGet();
            }
        };
        observer.setName("seed-transfer-observer");
        try (var storage = PreparedNativeStore.create();
                var context = new org.springframework.boot.builder.SpringApplicationBuilder(io.tapstate.app.Bootstrap.class)
                        .initializers(application -> {
                            observer.start(); logger.addAppender(observer); logger.setLevel(Level.DEBUG); logger.setAdditive(false);
                        })
                        .properties("tapstate.store.mongo.enabled=true", ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID)
                        .run("--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                                "--tapstate.hz.jet.cooperative-thread-count=2", "--tapstate.cluster.id=" + storage.clusterId(),
                                "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                                "--tapstate.connectors.seed-dir=" + seeds,
                                "--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + cache,
                                "--logging.level.org.mongodb.driver.protocol.command=DEBUG")) {
            int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext) context).getWebServer().getPort();
            var control = new ControlPlane(java.net.URI.create("http://127.0.0.1:" + port));
            storage.loginAndAwaitBusiness(control);
            assertThat(control.testConnection("orders", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString())).outcome())
                    .isEqualTo("PASSED");
            Path cached = cache.resolve(hash + ".jar");
            assertThat(Files.isRegularFile(cached)).isTrue();
            try (var input = Files.newInputStream(cached)) { assertThat(ContentHash.of(input)).isEqualTo(hash); }
            assertThat(chunkWrites.get()).as("the observer witnessed the real GridFS upload").isPositive();
            assertThat(logger.getAppender(observer.getName())).isSameAs(observer);
            assertThat(chunkReads.get()).as("the original local seed supplies the plugin cache").isZero();
        } finally {
            logger.detachAppender(observer); observer.stop(); logger.setLevel(previous); logger.setAdditive(additive);
        }
    }
}
