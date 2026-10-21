package io.tapstate.app;

import com.mongodb.MongoCommandException;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** One opt-in local field classification for the explicitly selected startup test entry. */
final class CloudRegistryInsertRefusalClassifier {
    static final Entry REAL_MANAGED_STARTUP = new Entry("RealManagedAtlasStoreStartupIT",
            "cloudBootstrapAndRestartRecoverAtlasStorageAndChangeHistoryRetention");
    private final boolean enabled;
    private final Entry entry;
    private final AtomicBoolean classified = new AtomicBoolean();

    CloudRegistryInsertRefusalClassifier(boolean enabled, Entry entry) {
        this.enabled = enabled;
        this.entry = entry;
    }

    Optional<Category> classify(Scope scope, Supplier<String> errorField) {
        if (!enabled || !REAL_MANAGED_STARTUP.equals(entry) || scope == null || !scope.matches()
                || !classified.compareAndSet(false, true)) return Optional.empty();
        return Optional.of(CloudSpecPutRefusalClassifier.classifyField(errorField)
                == CloudSpecPutRefusalClassifier.Category.SPACE_QUOTA
                ? Category.SPACE_QUOTA : Category.UNCLASSIFIED);
    }

    record Entry(String testClass, String testMethod) {}

    record Scope(String connector, boolean seed, boolean pathMatch, String operation,
                 String command, Class<?> exceptionType, int driverCode) {
        boolean matches() {
            return "mysql".equals(connector) && seed && pathMatch
                    && "registry-register".equals(operation) && "insert".equals(command)
                    && exceptionType == MongoCommandException.class && driverCode == 8000;
        }
    }

    enum Category { SPACE_QUOTA, UNCLASSIFIED }
}
