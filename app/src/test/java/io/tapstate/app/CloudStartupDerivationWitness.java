package io.tapstate.app;

import com.mongodb.MongoException;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Test-only stage evidence before the startup boundary replaces a connector refusal. */
final class CloudStartupDerivationWitness implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private final Consumer<Map<String, Object>> output;
    private final AtomicInteger calls = new AtomicInteger();
    private final ThreadLocal<Attempt> active = new ThreadLocal<>();

    CloudStartupDerivationWitness(Consumer<Map<String, Object>> output) {
        this.output = output;
    }

    static CloudStartupDerivationWitness initializer() {
        return new CloudStartupDerivationWitness(event ->
                System.out.println("CLOUD_MYSQL_DERIVATION " + JsonWriter.write(event)));
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (bean instanceof CapabilityDeriver deriver) return observe(deriver);
                if (bean instanceof ConnectorProvisioner provisioner) return observe(provisioner);
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
                try {
                    return delegate.resolve(id);
                } catch (TapstateException | UncheckedIOException failure) {
                    Attempt attempt = active.get();
                    if ("mysql".equals(id) && attempt != null && attempt.failure == null) {
                        attempt.failure = failure(attempt, "provision", failure);
                    }
                    throw failure;
                }
            }

            @Override
            public String cacheIdentity(String id) {
                return delegate.cacheIdentity(id);
            }
        };
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
