package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.BuiltinAlarmTypes;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RequiresDocker
class AlertShutdownTransactionIT {
    @Container static final MongoDBContainer MONGO = AlertEvaluationHostIT.MONGO
            .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"));

    @Test void closeEndsTheEvaluationTransactionBeforeConfigurationClientClosure() {
        AlertEvaluationHostIT.withFixture(f -> {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var interrupted = new CountDownLatch(1);
            var guards = new AtomicInteger();
            Runnable active = () -> {
                if (!Thread.currentThread().getName().startsWith("alert-evaluation-")) return;
                // The final guard follows the real evaluationRules reads, before their commit.
                if (guards.incrementAndGet() != 3) return;
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("evaluation barrier timed out");
                } catch (InterruptedException stopped) {
                    interrupted.countDown();
                    Thread.currentThread().interrupt();
                }
            };
            var original = f.storeHost();
            var host = new AlertStore.Host(original.scope(), original.setup(), original.deployment(), active);
            var store = f.configuration().alerts(host, BuiltinAlarmTypes.catalog(), f.clock());
            var evaluator = new AlertEvaluationHost(store, BuiltinAlarmTypes.catalog(), host.deployment(),
                    f.clock(), "shutdown-witness", active);
            var runtime = new PipelineAlertRuntime(() -> new PipelineAlertRuntime.Services(store, evaluator),
                    f.configuration().intents(), (scope, claim) -> Optional.of(active), () -> { }, f.clock());
            try (var observer = MongoClients.create(f.settings().uri());
                    var closer = Executors.newSingleThreadExecutor()) {
                var admin = observer.getDatabase("admin");
                runtime.tick();
                assertThat(entered.await(5, TimeUnit.SECONDS)).as("evaluation reached its open CONFIG transaction").isTrue();
                var transactions = transactions(admin);
                assertThat(transactions).as("real server transaction holding the fixture database lock").hasSize(1);
                Document session = transactions.getFirst().get("lsid", Document.class);
                Document transaction = transactions.getFirst().get("transaction", Document.class);
                var number = transaction.get("parameters", Document.class).get("txnNumber");
                var closed = closer.submit(runtime::close);
                try {
                    // Let a draining close finish naturally; an interrupting close releases the barrier immediately.
                    interrupted.await(250, TimeUnit.MILLISECONDS);
                } finally {
                    release.countDown();
                }
                closed.get(5, TimeUnit.SECONDS);
                await().atMost(Duration.ofSeconds(5)).until(() -> runtime.health().queuedInputs() == 0);
                f.configuration().close();
                var retained = transactions(admin).stream().filter(op -> session.equals(op.get("lsid")))
                        .filter(op -> number.equals(op.get("transaction", Document.class)
                                .get("parameters", Document.class).get("txnNumber"))).toList();
                assertThat(retained).as("CONFIG transaction must end before client closure; retained locks delay dropDatabase").isEmpty();
            } catch (Exception failure) {
                throw new AssertionError(failure);
            } finally {
                release.countDown();
                runtime.close();
            }
        });
    }

    private static List<Document> transactions(com.mongodb.client.MongoDatabase admin) {
        // This class owns its replica set; only the captured session/transaction is asserted below.
        return admin.aggregate(List.of(new Document("$currentOp", new Document("allUsers", true).append("idleSessions", true)),
                new Document("$match", new Document("transaction", new Document("$exists", true))
                        .append("locks.Database", new Document("$exists", true))))).into(new java.util.ArrayList<>());
    }
}
