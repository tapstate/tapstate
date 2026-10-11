package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.Map;
import org.bson.Document;
import io.tapstate.core.common.JsonReader;

/**
 * One replica set for every specification in the JVM.
 *
 * <p>A container per test class costs a start-up each time and buys nothing: runs stay independent
 * by taking a database of their own, not a daemon of their own. Ryuk reaps the container when the
 * JVM exits, so there is no stop to forget.
 */
final class SharedMongo {

    private static final DockerImageName IMAGE = DockerImageName.parse("mongo:7.0");
    private static final String CACHE_BUDGET_PROPERTY = "tapstate.e2e.mongo.wired-tiger-cache-gb";

    /** Kept as its own constant rather than reached for across modules: this module depends on neither. */
    static final String OPERATOR_STATE_DATABASE = "tapstate_nest";

    private static MongoDBContainer container;
    private static Optional<BigDecimal> cacheBudget;

    private SharedMongo() {
    }

    /**
     * The operator-state database configured for this test deployment. Every server here uses the
     * compatible default, while its control database takes a per-run name. The helper therefore has to
     * reset operator state explicitly between independent runs.
     *
     * <p>Two runs of one pipeline therefore meet in it. That is right in a deployment - a pipeline that
     * restarts is meant to resume what it had assembled - and wrong here, where two runs of one
     * specification share every id while sharing no data: the second inherits documents built from the
     * first one's rows and serves them as its own. Measured as a value appearing in a run whose source
     * never held it, whose store began empty, and whose change step had not executed.
     *
     * <p>Discarded per run rather than per specification: the id a run collides on is the pipeline's, and
     * every tier of every example writes one.
     */
    static synchronized void discardNestState() {
        if (container == null) {
            return;
        }
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            client.getDatabase(OPERATOR_STATE_DATABASE).drop();
        }
    }
    /**
     * The URL of the one database above, for a case whose subject is what lives in it.
     *
     * <p>Named here rather than spelled out at the call site because this is the value configured for the
     * test deployment. A case that wrote the default again would keep working after the harness changed
     * its configuration and would be asserting about a database nothing uses.
     */
    static synchronized String assemblyStateUrl() {
        return replicaSetUrl(OPERATOR_STATE_DATABASE);
    }


    /** The URL of a database on the shared replica set; the caller's name keeps its data its own. */
    static synchronized String replicaSetUrl(String database) {
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MONGO_URI, () -> {
        if (container == null) {
            BenchmarkSharedSupportSourceWork.dockerGate();
            MongoDBContainer starting = new MongoDBContainer(IMAGE);
            configuredCacheBudget().ifPresent(budget -> BenchmarkSharedSupportSourceWork.entered(
                    BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.Kind.MONGO_ARGUMENTS, () -> {
                // Preserve the replica-set arguments supplied by the container binding.
                String[] parts = starting.getCommandParts();
                BenchmarkSharedSupportSourceWork.commandParts(parts);
                var arguments = new ArrayList<>(Arrays.asList(parts));
                arguments.add(cacheArgument(budget));
                starting.withCommand(arguments.toArray(String[]::new));
                return null;
            }));
            starting.start();
            container = starting;
        }
        return container.getReplicaSetUrl(database);

                });
    }

    /** A startup input, never a total mongod memory limit or a late reconfiguration. */
    static synchronized Optional<BigDecimal> configuredCacheBudget() {
        return BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.MONGO_CONFIGURATION,
                () -> cacheBudget = configuredCacheBudgetUsing(cacheBudget, () -> System.getProperty(CACHE_BUDGET_PROPERTY)));
    }

    static Optional<BigDecimal> configuredCacheBudgetUsing(Optional<BigDecimal> cached,
            java.util.function.Supplier<String> property) {
        if (cached != null) {
            requireScopedCachePresence(cached);
            cached.ifPresent(SharedMongo::requireScopedCacheBudget);
            return cached;
        }
        String configured = property.get();
        if (configured == null) {
            Optional<BigDecimal> absent = Optional.empty();
            requireScopedCachePresence(absent);
            return absent;
        }
        return Optional.of(parseCacheBudget(configured));
    }

    private static void requireScopedCachePresence(Optional<BigDecimal> budget) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope != null && budget.isEmpty()) {
            BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    "MONGO_CACHE_BUDGET_MISSING_FROM_FROZEN_SOURCE_RECIPE");
            throw new AssertionError("active shared Mongo frozen cache budget is absent");
        }
    }

    static BigDecimal parseCacheBudget(String configured) {
        BenchmarkSharedSupportSourceWork.local(configured);
        BigDecimal budget = new BigDecimal(configured);
        if (budget.signum() <= 0) { throw new IllegalArgumentException("the shared Mongo cache budget must be positive"); }
        requireScopedCacheBudget(budget);
        return budget.stripTrailingZeros();
    }

    static String cacheArgument(BigDecimal budget) {
        requireScopedCacheBudget(budget);
        return "--wiredTigerCacheSizeGB=" + budget.toPlainString();
    }

    private static void requireScopedCacheBudget(BigDecimal budget) {
        var scope = BenchmarkReturnWorkBudget.current();
        // Equality checks both value and scale without expanding a scientific decimal into text.
        if (scope != null && !BigDecimal.valueOf(5, 1).equals(budget)) {
            BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    "MONGO_CACHE_BUDGET_DIFFERS_FROM_FROZEN_VALUE_OR_SCALE");
            throw new AssertionError("active shared Mongo cache budget differs from the frozen value or scale");
        }
    }

    /** Exact owned fixture identity, read only during diagnostic setup. */
    static synchronized Map<String, Object> diagnosticIdentity() {
        if (container == null) { return Map.of("status", "UNKNOWN", "reason", "OWNED_MONGO_NOT_STARTED"); }
        long before = System.nanoTime();
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            Document build = client.getDatabase("admin").runCommand(new Document("buildInfo", 1).append("maxTimeMS", 2000));
            Document fcv = client.getDatabase("admin").runCommand(new Document("getParameter", 1)
                    .append("featureCompatibilityVersion", 1).append("maxTimeMS", 2000));
            String version = build.getString("version"), gitVersion = build.getString("gitVersion");
            String canonical = fcv.toJson();
            var info = container.getContainerInfo();
            if (version == null || gitVersion == null || version.length() > 4096 || gitVersion.length() > 4096
                    || canonical.length() > 65536 || info.getImageId() == null || container.getContainerId() == null) {
                return Map.of("status", "UNKNOWN", "reason", "OWNED_MONGO_IDENTITY_INCOMPLETE");
            }
            return Map.of("status", "COMPLETE", "scope", "OWNED_FIXTURE_PRE_PIPELINE_CONFIGURATION",
                    "containerId", container.getContainerId(), "imageId", info.getImageId(),
                    "imageTag", container.getDockerImageName(), "version", version, "gitVersion", gitVersion,
                    "featureCompatibilityVersion", JsonReader.parse(canonical), "startedAtNanos", before,
                    "completedAtNanos", System.nanoTime());
        } catch (RuntimeException | AssertionError unavailable) {
            return Map.of("status", "UNKNOWN", "reason", "OWNED_MONGO_IDENTITY_READ_FAILED",
                    "failureType", unavailable.getClass().getName(), "startedAtNanos", before,
                    "completedAtNanos", System.nanoTime());
        }
    }
}
