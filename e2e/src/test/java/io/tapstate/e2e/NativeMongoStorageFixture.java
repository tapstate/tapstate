package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.app.BCryptPasswordHasher;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.User;
import java.time.Duration;
import java.util.Map;

/** Prepares native storage through actual initialization ports; this does not exercise the installation UI. */
final class NativeMongoStorageFixture {
    static final String USER = "admin", PASSWORD = "native-restart-password";

    private NativeMongoStorageFixture() {}

    static void prepare(String configUri, StorageBinding.Connection cacheBinding, String targetUri, String cluster) {
        prepare(configUri, cacheBinding, targetUri, cluster, USER, PASSWORD);
    }

    static void prepare(String configUri, StorageBinding.Connection cacheBinding, String targetUri, String cluster,
            String username, String password) {
        var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", targetUri), null, null, null, null);
        prepare(configUri, cacheBinding, target, cluster, username, password);
    }

    static void prepareCloud(String configUri, StorageBinding.Connection cacheBinding, String targetUri, String cluster) {
        var target = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.CLOUD), null, "mongodb", Map.of("isUri", true, "uri", targetUri), null, null, null, null);
        prepare(configUri, cacheBinding, target, cluster, null, null, StorageBinding.Mode.CLOUD);
    }

    static void prepare(String configUri, StorageBinding.Connection cacheBinding, SourceResource target, String cluster,
            String username, String password) {
        prepare(configUri, cacheBinding, target, cluster, username, password, StorageBinding.Mode.OP);
    }

    private static void prepare(String configUri, StorageBinding.Connection cacheBinding, SourceResource target, String cluster,
            String username, String password, StorageBinding.Mode mode) {
        var scope = new StorageSetupScope(cluster, "installation", "setup", 1);
        try (var configuration = MongoConfigurationBootstrap.prepare(
                new MongoConnectionSettings(configUri, null, Duration.ofSeconds(5)), cluster, "installation", "setup", 1)) {
            var committed = configuration.bindings().commitWithFirstCache(scope, mode, cacheBinding, target);
            try (var cache = MongoCacheDomain.connect(cacheBinding, Duration.ofSeconds(5), "fixture")) {
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(committed.preparation(), () -> {});
            }
            configuration.bindings().complete(scope);
            if (mode == StorageBinding.Mode.OP) configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                    new User(username, new BCryptPasswordHasher().hash(password), "admin"));
        }
    }
}
