package io.tapstate.app;

import com.mongodb.MongoException;
import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/** Test-only typed evidence from the one real seed sweep, before Cloud suppresses its refusal. */
final class CloudSeedOutcomeWitness implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private final Path expectedArtifact;
    private final String expectedHash;
    private final String expectedPdk;
    private final Consumer<Map<String, Object>> output;
    private final AtomicInteger calls = new AtomicInteger();

    CloudSeedOutcomeWitness(Path expectedArtifact, String expectedHash, String expectedPdk,
            Consumer<Map<String, Object>> output) {
        this.expectedArtifact = expectedArtifact.toAbsolutePath().normalize();
        this.expectedHash = expectedHash;
        this.expectedPdk = expectedPdk;
        this.output = output;
    }

    static CloudSeedOutcomeWitness initializer() {
        Path seeds = CloudConnectorTestInputs.seedDirectory();
        try {
            Map<?, ?> lock = (Map<?, ?>) JsonReader.parse(Files.readString(
                    seeds.getParent().resolve("release/connectors.lock.json")));
            List<?> entries = (List<?>) lock.get("connectors");
            List<?> mysql = entries.stream().filter(entry -> entry instanceof Map<?, ?> fields
                    && "mysql".equals(fields.get("id"))).toList();
            if (mysql.size() != 1) throw new AssertionError("the locked MySQL entry must be unique");
            Map<?, ?> fields = (Map<?, ?>) mysql.getFirst();
            return new CloudSeedOutcomeWitness(seeds.resolve("mysql-connector.jar"),
                    (String) fields.get("sha256"), (String) fields.get("pdkApiVersion"), event ->
                    System.out.println("CLOUD_MYSQL_SEED " + JsonWriter.write(event)));
        } catch (IOException unavailable) {
            throw new AssertionError("the locked seed evidence is unavailable");
        }
    }

    @Override public void initialize(ConfigurableApplicationContext context) {
        context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
            @Override public Object postProcessAfterInitialization(Object bean, String name) {
                return bean instanceof SeedConnectorSweep sweep ? observe(sweep) : bean;
            }
        });
    }

    @SuppressWarnings("unchecked")
    SeedConnectorSweep observe(SeedConnectorSweep delegate) {
        SeedConnectorSweep observed = spy(delegate);
        doAnswer(call -> record((List<SeedOutcome>) call.callRealMethod())).when(observed).sweep(any(Path.class));
        return observed;
    }

    List<SeedOutcome> record(List<SeedOutcome> outcomes) {
        List<SeedOutcome> selected = outcomes.stream().filter(outcome ->
                "mysql-connector.jar".equals(String.valueOf(outcome.artifact().getFileName()))
                        || outcome instanceof SeedOutcome.Seeded seeded
                        && "mysql".equals(seeded.outcome().registration().connectorId())).toList();
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("connector", "mysql");
        event.put("call", calls.incrementAndGet());
        event.put("matching_count", selected.size());
        if (selected.size() != 1) {
            event.put("outcome", selected.isEmpty() ? "absent" : "ambiguous");
        } else {
            SeedOutcome selectedOutcome = selected.getFirst();
            event.put("path_match", selectedOutcome.artifact().toAbsolutePath().normalize().equals(expectedArtifact));
            if (selectedOutcome instanceof SeedOutcome.Seeded seeded) {
                event.put("outcome", "seeded");
                var registration = seeded.outcome().registration();
                event.put("id_match", "mysql".equals(registration.connectorId()));
                event.put("hash_match", Objects.equals(expectedHash, registration.contentHash()));
                event.put("pdk_match", Objects.equals(expectedPdk, registration.pdkApiVersion()));
            } else {
                event.put("outcome", "failed");
                RuntimeException failure = ((SeedOutcome.Failed) selectedOutcome).cause();
                event.put("exception_type", failure.getClass().getName());
                if (failure instanceof TapstateException coded) {
                    String code = coded.code().code();
                    if (code.matches("[a-z0-9][a-z0-9.-]{0,120}")) event.put("code", code);
                }
                List<String> types = new ArrayList<>();
                Throwable cause = failure;
                for (int depth = 0; cause != null && depth < 8; depth++, cause = cause.getCause()) {
                    types.add(cause.getClass().getName());
                    if (cause instanceof MongoException mongo && !event.containsKey("driver_code")) {
                        event.put("driver_code", mongo.getCode());
                    }
                }
                event.put("cause_types", types);
            }
        }
        output.accept(Map.copyOf(event));
        return outcomes;
    }
}
