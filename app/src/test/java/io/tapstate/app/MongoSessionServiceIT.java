package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.adapters.mongostore.MongoSessionStore;
import io.tapstate.control.core.AccessTokenService;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SessionService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class MongoSessionServiceIT {

    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Instant NOW = Instant.parse("2026-08-17T10:00:00Z");
    private static final String ISSUER = "urn:tapstate:cluster:01J5FIXTURE";

    @Container
    static final MongoDBContainer CONTAINER = MONGO;

    @Test
    void independentServicesConcurrentlyExchangeTheSameOpaqueSessionWithoutRotation() throws Exception {
        try (MongoClient client = MongoClients.create(CONTAINER.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("sessions");
            collection.drop();
            MongoSessionStore firstStore = new MongoSessionStore(collection);
            MongoSessionStore secondStore = new MongoSessionStore(collection);
            SessionService firstService = service(firstStore);
            SessionService secondService = service(secondStore);
            String sessionToken = firstService.create("admin", Scope.ADMIN, ISSUER).token();
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            try (var executor = Executors.newFixedThreadPool(2)) {
                List<Callable<Boolean>> exchanges = List.of(
                        exchangeAfterStart(firstService, sessionToken, ready, start),
                        exchangeAfterStart(secondService, sessionToken, ready, start));
                var futures = exchanges.stream().map(executor::submit).toList();
                ready.await();
                start.countDown();
                assertThat(futures).allSatisfy(future -> assertThat(future.get()).isTrue());
            }

            assertThat(sessionToken).isEqualTo("tss_s01.session-secret");
            assertThat(secondService.exchange(sessionToken, ISSUER)).isPresent();
            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(firstStore.find("s01")).get().satisfies(record -> {
                assertThat(record.sessionId()).isEqualTo("s01");
                assertThat(record.secretHash()).isEqualTo("digest-fixture");
            });
        }
    }

    private static Callable<Boolean> exchangeAfterStart(
            SessionService service, String sessionToken, CountDownLatch ready, CountDownLatch start) {
        return () -> {
            ready.countDown();
            start.await();
            return service.exchange(sessionToken, ISSUER).isPresent();
        };
    }

    private static SessionService service(MongoSessionStore store) {
        TokenSecrets secrets = new TokenSecrets() {
            @Override
            public GeneratedSecret generate() {
                return new GeneratedSecret("s01", "session-secret", "digest-fixture");
            }

            @Override
            public String hash(String presentedSecret) {
                return "session-secret".equals(presentedSecret) ? "digest-fixture" : "digest-wrong";
            }

            @Override
            public boolean matches(String presentedSecret, String storedHash) {
                return storedHash.equals(hash(presentedSecret));
            }
        };
        TokenSigner signer = new TokenSigner() {
            @Override
            public String issue(String subject, Scope scope) {
                return subject + '|' + scope;
            }

            @Override
            public Optional<VerifiedToken> verify(String token) {
                return Optional.empty();
            }
        };
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new SessionService(store, secrets, new AccessTokenService(signer, clock), clock);
    }
}
