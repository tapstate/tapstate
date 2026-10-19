package io.tapstate.app;

import com.mongodb.MongoException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** Test-only stage evidence before the startup boundary replaces a connector refusal. */
final class CloudStartupDerivationWitness implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    static final String REFUSAL_CLASSIFICATION_PROPERTY = "tapstate.test.mysql-seed-refusal-classification";
    private final Consumer<Map<String, Object>> output;
    private final Consumer<Map<String, Object>> storeOutput;
    private final Path expectedSeed;
    private final boolean refusalClassificationEnabled;
    private final AtomicBoolean refusalClassified = new AtomicBoolean();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger seedCalls = new AtomicInteger();
    private final ThreadLocal<Attempt> active = new ThreadLocal<>();
    private final ThreadLocal<StoreAttempt> storeActive = new ThreadLocal<>();

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output) {
        this(output, event -> {});
    }

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output,
            Consumer<Map<String, Object>> storeOutput) {
        this(output, storeOutput, null);
    }

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output,
            Consumer<Map<String, Object>> storeOutput, Path expectedSeed) {
        this(output, storeOutput, expectedSeed, false);
    }

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output,
            Consumer<Map<String, Object>> storeOutput, Path expectedSeed, boolean refusalClassificationEnabled) {
        this.output = output;
        this.storeOutput = storeOutput;
        this.expectedSeed = expectedSeed == null ? null : expectedSeed.toAbsolutePath().normalize();
        this.refusalClassificationEnabled = refusalClassificationEnabled;
    }

    static CloudStartupDerivationWitness initializer() {
        return new CloudStartupDerivationWitness(event ->
                System.out.println("CLOUD_MYSQL_DERIVATION " + JsonWriter.write(event)), event -> {
                    String prefix = "factory".equals(event.get("kind"))
                            ? "CLOUD_STORE_OBSERVER_FACTORY " : "seed".equals(event.get("stage"))
                            ? "CLOUD_MYSQL_SEED_STORE " : "CLOUD_MYSQL_STORE_READ ";
                    System.out.println(prefix + JsonWriter.write(event));
                }, CloudConnectorTestInputs.seedDirectory().resolve("mysql-connector.jar"),
                Boolean.getBoolean(REFUSAL_CLASSIFICATION_PROPERTY));
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (bean instanceof CapabilityDeriver deriver) return observe(deriver);
                if (bean instanceof ConnectorProvisioner provisioner) return observe(provisioner);
                if (bean instanceof ConnectorRegistry registry) return observe(registry);
                if (bean instanceof ConnectorSpecStore specs) return observe(specs);
                if (bean instanceof ConnectorCatalogStore catalog) return observe(catalog);
                if (bean instanceof ConnectorArtifactRegistrar registrar) return observe(registrar);
                return bean;
            }
        });
    }

    CapabilityDeriver observe(CapabilityDeriver delegate) {
        return id -> {
            if (!"mysql".equals(id)) return withoutStore(() -> delegate.derive(id));
            Attempt previous = active.get();
            Attempt attempt = new Attempt(calls.incrementAndGet(), System.nanoTime());
            active.set(attempt);
            try {
                var result = delegate.derive(id);
                Map<String, Object> event = base(attempt, "derive");
                event.put("outcome", "ok");
                event.put("capability_count", result.capabilityIds().size());
                output.accept(Map.copyOf(event));
                return result;
            } catch (TapstateException | UncheckedIOException failure) {
                if (attempt.failure == null) attempt.failure = failure(attempt, "derive", failure);
                output.accept(Map.copyOf(attempt.failure));
                throw failure;
            } finally {
                if (previous == null) active.remove();
                else active.set(previous);
            }
        };
    }

    ConnectorProvisioner observe(ConnectorProvisioner delegate) {
        return new ConnectorProvisioner() {
            @Override
            public ConnectorRef resolve(String id) {
                StoreAttempt previous = storeActive.get();
                Attempt current = active.get();
                if ("mysql".equals(id) && current != null) storeActive.set(new StoreAttempt(current));
                else storeActive.remove();
                try {
                    return delegate.resolve(id);
                } catch (TapstateException | UncheckedIOException failure) {
                    Attempt attempt = active.get();
                    if ("mysql".equals(id) && attempt != null && attempt.failure == null) {
                        attempt.failure = failure(attempt, "provision", failure);
                    }
                    throw failure;
                } finally {
                    if (previous == null) storeActive.remove();
                    else storeActive.set(previous);
                }
            }

            @Override
            public String cacheIdentity(String id) {
                return delegate.cacheIdentity(id);
            }
        };
    }

    ConnectorRegistry observe(ConnectorRegistry delegate) {
        return new ConnectorRegistry() {
            @Override public RegistrationOutcome register(String id, String pdk,
                    RegistrationSource source, byte[] artifact) {
                return seedOperation("registry-register", id, () -> delegate.register(id, pdk, source, artifact));
            }
            @Override public List<ConnectorRegistration> list() {
                return storeRead("registry-list", delegate::list);
            }
            @Override public List<ConnectorRegistration> findAll(String id) {
                return seedOperation("registry-find-all", id, () -> delegate.findAll(id));
            }
            @Override public Optional<byte[]> artifact(String hash) {
                return storeRead("artifact", () -> delegate.artifact(hash));
            }
            @Override public Optional<String> shareSafeCertification(String id, String hash) {
                return delegate.shareSafeCertification(id, hash);
            }
            @Override public boolean hasArtifact(String hash) {
                return delegate.hasArtifact(hash);
            }
        };
    }

    ConnectorSpecStore observe(ConnectorSpecStore delegate) {
        return new ConnectorSpecStore() {
            @Override public void put(String hash, byte[] spec) {
                seedOperation("spec-put", () -> { delegate.put(hash, spec); return null; });
            }
            @Override public Optional<byte[]> get(String hash) { return delegate.get(hash); }
            @Override public boolean has(String hash) {
                return seedOperation("spec-has", () -> delegate.has(hash));
            }
        };
    }

    ConnectorCatalogStore observe(ConnectorCatalogStore delegate) {
        return new ConnectorCatalogStore() {
            @Override public void upsert(ConnectorCatalogEntry entry) { delegate.upsert(entry); }
            @Override public Optional<ConnectorCatalogEntry> get(String id) {
                return seedOperation("catalog-get", id, () -> delegate.get(id));
            }
            @Override public List<ConnectorCatalogEntry> list() { return delegate.list(); }
        };
    }

    ConnectorArtifactRegistrar observe(ConnectorArtifactRegistrar delegate) {
        if (expectedSeed == null) return delegate;
        ConnectorArtifactRegistrar observed = spy(delegate);
        doAnswer(call -> {
            Path artifact = call.getArgument(0);
            StoreAttempt previous = storeActive.get();
            boolean selected = call.getArgument(1) == RegistrationSource.SEED
                    && artifact.toAbsolutePath().normalize().equals(expectedSeed);
            StoreAttempt attempt = selected ? new StoreAttempt(seedCalls.incrementAndGet()) : null;
            if (attempt == null) storeActive.remove();
            else storeActive.set(attempt);
            long started = System.nanoTime();
            try {
                Object result = call.callRealMethod();
                if (attempt != null) recordStoreOperation(attempt, started, null);
                return result;
            } catch (TapstateException | UncheckedIOException failure) {
                if (attempt != null) recordStoreOperation(attempt, started, failure);
                throw failure;
            } finally {
                if (previous == null) storeActive.remove();
                else storeActive.set(previous);
            }
        }).when(observed).register(any(Path.class), any(RegistrationSource.class));
        return observed;
    }

    private <T> T seedOperation(String operation, String id, Supplier<T> delegate) {
        return "mysql".equals(id) ? seedOperation(operation, delegate) : withoutStore(delegate);
    }

    private <T> T seedOperation(String operation, Supplier<T> delegate) {
        StoreAttempt attempt = storeActive.get();
        return attempt != null && attempt.seed ? storeRead(operation, delegate) : delegate.get();
    }

    private <T> T withoutStore(Supplier<T> delegate) {
        StoreAttempt previous = storeActive.get();
        storeActive.remove();
        try { return delegate.get(); }
        finally { if (previous != null) storeActive.set(previous); }
    }

    private <T> T storeRead(String operation, Supplier<T> delegate) {
        StoreAttempt attempt = storeActive.get();
        if (attempt == null) return delegate.get();
        String previous = attempt.operation;
        attempt.operation = operation;
        long started = System.nanoTime();
        try {
            T result = delegate.get();
            recordStoreOperation(attempt, started, null);
            return result;
        } catch (TapstateException | UncheckedIOException failure) {
            recordStoreOperation(attempt, started, failure);
            throw failure;
        } finally {
            attempt.operation = previous;
        }
    }

    private void recordStoreOperation(StoreAttempt attempt, long started, RuntimeException failure) {
        Map<String, Object> event = storeBase(attempt, "operation");
        event.put("outcome", failure == null ? "ok" : "mapped-refusal");
        event.put("elapsed_millis", (System.nanoTime() - started) / 1_000_000);
        if (failure != null) {
            event.put("exception_type", failure.getClass().getName());
            if (failure instanceof TapstateException coded) {
                String code = coded.code().code();
                if (code.matches("[a-z0-9][a-z0-9.-]{0,120}")) event.put("code", code);
            }
        }
        emitStore(event);
    }

    private static Map<String, Object> storeBase(StoreAttempt attempt, String kind) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("kind", kind);
        event.put("connector", "mysql");
        event.put("call", attempt.call);
        event.put("operation", attempt.operation);
        if (attempt.seed) {
            event.put("stage", "seed");
            event.put("path_match", true);
        }
        return event;
    }

    private void commandFailed(CommandFailedEvent failed) {
        StoreAttempt attempt = storeActive.get();
        if (attempt == null || attempt.operation == null) return;
        String command = failed.getCommandName();
        if (!("find".equals(command) || "getMore".equals(command)
                || attempt.seed && ("insert".equals(command) || "update".equals(command)))) return;
        Throwable failure = failed.getThrowable();
        if (failure == null) return;
        Map<String, Object> event = storeBase(attempt, "command-failed");
        event.put("outcome", "driver-refusal");
        event.put("command", command);
        event.put("exception_type", failure.getClass().getName());
        if (failure instanceof MongoException mongo) event.put("driver_code", mongo.getCode());
        if (refusalClassificationEnabled && attempt.seed && "spec-put".equals(attempt.operation)
                && "update".equals(command) && failure instanceof MongoCommandException mongo
                && mongo.getCode() == 8000 && refusalClassified.compareAndSet(false, true)) {
            // Read only this field once; arbitrary text never enters an event or a diagnostic exception.
            event.put("refusal_category", CloudSpecPutRefusalClassifier.classify(
                    true, true, attempt.operation, command, mongo.getCode(), mongo::getErrorMessage).name());
        }
        emitStore(event);
    }

    private void emitStore(Map<String, Object> event) {
        try {
            storeOutput.accept(Map.copyOf(event));
        } catch (RuntimeException unavailable) {
            // Missing diagnostic output must not change the original store call.
        }
    }

    MongoClientObservation observeClientCreation() {
        return new MongoClientObservation();
    }

    final class MongoClientObservation implements AutoCloseable {
        private final MockedStatic<MongoClients> clients;
        private final ThreadLocal<Boolean> creating = new ThreadLocal<>();
        private int intercepted;
        private int realCalls;
        private boolean closed;

        private MongoClientObservation() {
            clients = mockStatic(MongoClients.class, CALLS_REAL_METHODS);
            try {
                clients.when(() -> MongoClients.create(any(MongoClientSettings.class))).thenAnswer(call -> {
                    MongoClientSettings settings = call.getArgument(0);
                    if (settings == null) return call.callRealMethod();
                    if (Boolean.TRUE.equals(creating.get())) {
                        realCalls++;
                        return call.callRealMethod();
                    }
                    intercepted++;
                    MongoClientSettings observed = MongoClientSettings.builder(settings)
                            .addCommandListener(new CommandListener() {
                                @Override public void commandFailed(CommandFailedEvent failed) {
                                    CloudStartupDerivationWitness.this.commandFailed(failed);
                                }
                            }).build();
                    creating.set(true);
                    try {
                        return MongoClients.create(observed);
                    } finally {
                        creating.remove();
                    }
                });
            } catch (RuntimeException | Error failure) {
                clients.close();
                throw failure;
            }
        }

        MockedStatic<MongoClients> mockedFactory() {
            return clients;
        }

        @Override public void close() {
            if (closed) return;
            closed = true;
            clients.close();
            creating.remove();
            emitStore(Map.of("kind", "factory", "intercepted_calls", intercepted,
                    "real_factory_calls", realCalls));
        }
    }

    private static final class StoreAttempt {
        private final int call;
        private final boolean seed;
        private String operation;

        private StoreAttempt(Attempt derivation) {
            this.call = derivation.call;
            this.seed = false;
        }

        private StoreAttempt(int call) {
            this.call = call;
            this.seed = true;
            this.operation = "registration";
        }
    }

    private static Map<String, Object> base(Attempt attempt, String phase) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("connector", "mysql");
        event.put("call", attempt.call);
        event.put("phase", phase);
        event.put("elapsed_millis", (System.nanoTime() - attempt.started) / 1_000_000);
        return event;
    }

    private static Map<String, Object> failure(Attempt attempt, String phase, RuntimeException failure) {
        Map<String, Object> event = base(attempt, phase);
        event.put("outcome", "coded-or-io-refusal");
        event.put("exception_type", failure.getClass().getName());
        if (failure instanceof TapstateException coded) {
            String code = coded.code().code();
            if (code.matches("[a-z0-9][a-z0-9.-]{0,120}")) event.put("code", code);
        }
        var types = new ArrayList<String>();
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 8; depth++, cause = cause.getCause()) {
            types.add(cause.getClass().getName());
            if (cause instanceof MongoException mongo && !event.containsKey("driver_code")) {
                event.put("driver_code", mongo.getCode());
            }
        }
        event.put("cause_types", types);
        return event;
    }

    private static final class Attempt {
        private final int call;
        private final long started;
        private Map<String, Object> failure;

        private Attempt(int call, long started) {
            this.call = call;
            this.started = started;
        }
    }
}
