package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.adapters.mongostore.MongoConnectorCatalogStore;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.SchemaDerivation;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.SinkCapability;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.WriteMode;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.SchemaStore;
import java.nio.file.Path;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Upgrade coverage across registration, persisted catalog reads and the on-prem apply gate. */
class ALegacyDb2CatalogRowCannotKeepDb2WritableTest {

    @Test
    void reRegisteringTheSameJarAfterAnUpgradeStillRefusesASyncIntoDb2(@TempDir Path dir) {
        Path jar = Synthetic.seedableConnector(dir, "db2");
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        InMemoryConnectorSpecStore specs = new InMemoryConnectorSpecStore();
        MongoConnectorCatalogStore rows = catalogStore();
        ConnectorArtifactRegistrar current = registrar(registry, specs, rows);
        RegistrationOutcome first = current.register(jar, RegistrationSource.SEED);
        ConnectorCatalogEntry fresh = rows.get("db2").orElseThrow();
        ConnectorCatalogView view = new ConnectorCatalogView(TapstateCatalog.load(), rows, specs, registry);
        Map<String, Resource> installed = new LinkedHashMap<>();
        ApplyService apply = new ApplyService(view::merged, artifacts(installed),
                new AuditGate(record -> { }, Clock.systemUTC()), mock(SchemaStore.class),
                PlanAdvisories.none(), SchemaDerivation.none());
        List<ArtifactDraft> batch = List.of(
                new ArtifactDraft("src_orders.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: src_orders
                        connector: mysql
                        config: { host: 10.10.0.5, database: ods, username: u, password: p }
                        mode: cdc
                        tables: [ orders ]
                        """),
                new ArtifactDraft("tgt_db2.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: tgt_db2
                        connector: db2
                        config: { host: 10.30.0.9, port: 50000, database: SAMPLE, schema: APP }
                        """),
                new ArtifactDraft("orders_into_db2.tap.yml", """
                        version: tapstate/v1
                        kind: pipeline
                        id: orders_into_db2
                        source: src_orders
                        serve:
                          from: orders
                          sync: [ { id: out, source: tgt_db2, write_mode: upsert } ]
                        """));

        // Control: deriving this artifact under the running release withdraws its write capability.
        assertThat(first.newlyRegistered()).isTrue();
        assertThat(fresh.sink().capable()).isFalse();
        DslException freshRefusal = catchThrowableOfType(DslException.class,
                () -> apply.apply("tester", batch));
        assertThat(freshRefusal).isNotNull();
        assertThat(freshRefusal.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(installed).isEmpty();

        // Before the source-only rule, the same inputs persisted the jar's write_record capability.
        // Leave its bytes and provenance intact, replacing only the sink field the old release derived.
        rows.upsert(new ConnectorCatalogEntry(fresh.id(), fresh.name(), fresh.displayName(), fresh.icon(),
                fresh.group(), fresh.modes(), fresh.discovery(),
                new SinkCapability(true, List.of(WriteMode.UPSERT, WriteMode.APPEND)),
                fresh.pushOut(), fresh.config(), fresh.provenance()));
        RegistrationOutcome again = registrar(registry, specs, rows).register(jar, RegistrationSource.SEED);
        assertThat(again.newlyRegistered()).isFalse();
        assertThat(again.registration().contentHash()).isEqualTo(first.registration().contentHash());

        DslException refusal = catchThrowableOfType(DslException.class, () -> apply.apply("tester", batch));

        assertThat(refusal)
                .as("an upgraded deployment must refuse a Db2 sync after re-registering the same jar; installed: %s",
                        installed.keySet())
                .isNotNull();
        assertThat(refusal.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(refusal.args()).containsEntry("connector", "db2").containsEntry("source", "tgt_db2");
        assertThat(installed).isEmpty();
    }

    private static ConnectorArtifactRegistrar registrar(InMemoryConnectorRegistry registry,
            InMemoryConnectorSpecStore specs, MongoConnectorCatalogStore rows) {
        return new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function", "write_record")),
                rows, specs, List.of("db2"));
    }

    /** Only the database transport is stubbed; row serialization and read-back policy are production code. */
    @SuppressWarnings("unchecked")
    private static MongoConnectorCatalogStore catalogStore() {
        AtomicReference<Document> stored = new AtomicReference<>();
        MongoCollection<Document> collection = mock(MongoCollection.class);
        FindIterable<Document> found = mock(FindIterable.class);
        when(collection.replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class)))
                .thenAnswer(call -> {
                    stored.set(call.getArgument(1));
                    return UpdateResult.acknowledged(1L, 1L, null);
                });
        when(collection.find(any(Bson.class))).thenReturn(found);
        when(collection.find()).thenReturn(found);
        when(found.first()).thenAnswer(call -> stored.get());
        when(found.iterator()).thenAnswer(call -> {
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(cursor.hasNext()).thenReturn(stored.get() != null, false);
            when(cursor.next()).thenReturn(stored.get());
            return cursor;
        });
        return new MongoConnectorCatalogStore(collection);
    }

    private static ArtifactStore artifacts(Map<String, Resource> installed) {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> resources) {
                resources.forEach(resource -> installed.put(resource.id(), resource));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(installed.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(installed.values());
            }
        };
    }
}
