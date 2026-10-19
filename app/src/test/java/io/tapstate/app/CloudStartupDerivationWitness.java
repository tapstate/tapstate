package io.tapstate.app;

import com.mongodb.MongoException;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/** Test-only stage evidence before the startup boundary replaces a connector refusal. */
final class CloudStartupDerivationWitness implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private final Consumer<Map<String, Object>> output;
    private final Consumer<Map<String, Object>> storeOutput;
    private final AtomicInteger calls = new AtomicInteger();
    private final ThreadLocal<Attempt> active = new ThreadLocal<>();
    private final ThreadLocal<StoreAttempt> storeActive = new ThreadLocal<>();

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output) {
        this(output, event -> {});
    }

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output,
            Consumer<Map<String, Object>> storeOutput) {
        this.output = output;
        this.storeOutput = storeOutput;
    }

    static CloudStartupDerivationWitness initializer() {
        return new CloudStartupDerivationWitness(event ->
                System.out.println("CLOUD_MYSQL_DERIVATION " + JsonWriter.write(event)), event -> {
                    String prefix = "factory".equals(event.get("kind"))
                            ? "CLOUD_STORE_OBSERVER_FACTORY " : "CLOUD_MYSQL_STORE_READ ";
                    System.out.println(prefix + JsonWriter.write(event));
                });
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (bean instanceof CapabilityDeriver deriver) return observe(deriver);
                if (bean instanceof ConnectorProvisioner provisioner) return observe(provisioner);
                if (bean instanceof ConnectorRegistry registry) return observe(registry);
                return bean;
            }
        });
    }

    CapabilityDeriver observe(CapabilityDeriver delegate) {
        return id -> {
            if (!"mysql".equals(id)) return delegate.derive(id);
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
                return delegate.register(id, pdk, source, artifact);
            }
            @Override public List<ConnectorRegistration> list() {
                return storeRead("registry-list", delegate::list);
            }
            @Override public List<ConnectorRegistration> findAll(String id) {
                return delegate.findAll(id);
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

    private <T> T storeRead(String operation, Supplier<T> delegate) {
        StoreAttempt attempt = storeActive.get();
        if (attempt == null) return delegate.get();
        String previous = attempt.operation;
        attempt.operation = operation;
        long started = System.nanoTime();
        try {
            T result = delegate.get();
            Map<String, Object> event = storeBase(attempt, "operation");
            event.put("outcome", "ok");
            event.put("elapsed_millis", (System.nanoTime() - started) / 1_000_000);
            emitStore(event);
            return result;
        } catch (TapstateException | UncheckedIOException failure) {
            Map<String, Object> event = storeBase(attempt, "operation");
            event.put("outcome", "mapped-refusal");
            event.put("exception_type", failure.getClass().getName());
            event.put("elapsed_millis", (System.nanoTime() - started) / 1_000_000);
            if (failure instanceof TapstateException coded) {
                String code = coded.code().code();
                if (code.matches("[a-z0-9][a-z0-9.-]{0,120}")) event.put("code", code);
            }
            emitStore(event);
            throw failure;
        } finally {
            attempt.operation = previous;
        }
    }

    private static Map<String, Object> storeBase(StoreAttempt attempt, String kind) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("kind", kind);
        event.put("connector", "mysql");
        event.put("call", attempt.derivation.call);
        event.put("operation", attempt.operation);
        return event;
    }

    private void commandFailed(CommandFailedEvent failed) {
        StoreAttempt attempt = storeActive.get();
        if (attempt == null || attempt.operation == null
                || !("find".equals(failed.getCommandName()) || "getMore".equals(failed.getCommandName()))) return;
        Throwable failure = failed.getThrowable();
        if (failure == null) return;
        Map<String, Object> event = storeBase(attempt, "command-failed");
        event.put("outcome", "driver-refusal");
        event.put("command", failed.getCommandName());
        event.put("exception_type", failure.getClass().getName());
        if (failure instanceof MongoException mongo) event.put("driver_code", mongo.getCode());
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
        private final Attempt derivation;
        private String operation;

        private StoreAttempt(Attempt derivation) {
            this.derivation = derivation;
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
