package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.ViewResource;
import io.tapstate.runtime.probe.DataBrowserCollectionsProbe;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * What the listing says about each collection beyond its name: which class of collection it is, the
 * fields something discovered on it, and the text whoever declared it wrote.
 *
 * <p>All three have a source that may simply not exist — nothing has been discovered on this
 * connection, nobody declared this collection — and the cases here are mostly about that: an answer
 * nobody can give is left out, never rendered as an empty or a made-up one. The difference matters to
 * the caller these are for. An empty field list reads as "this collection has no fields", which is a
 * different statement from "nobody has looked", and only one of them is true.
 *
 * <p>The listing covers every collection the database holds, and a database holds more than a
 * workspace authored — so "this is a view" is an answer about some of them and about none of the
 * rest. Saying it of all of them would be this layer deciding, on a caller's behalf, that a
 * collection somebody made by hand is a thing a pipeline materialized.
 */
class DataBrowserCollectionsTest {

    private static final SourceResource VIEWS = new SourceResource(
            "views", null, "mongodb",
            Map.of("uri", "mongodb://db.local", "database", "shop"),
            null, null, null, null);

    private static final DataBrowserPreview NOTHING = new DataBrowserPreview(List.of(), null, false);

    @Test
    void namesEveryCollectionTheDatabaseHolds() {
        DataBrowserService service = service(
                store(VIEWS), config -> List.of("order_state", "customers"), schemas());

        assertThat(service.collections("views"))
                .extracting(DataBrowserCollection::name)
                .containsExactly("order_state", "customers");
    }

    @Test
    void callsACollectionAViewWhenSomeViewDeclaresIt() {
        DataBrowserService service = service(
                store(VIEWS, view("v_order_state", "order_state", "One row per order")),
                config -> List.of("order_state"),
                schemas());

        assertThat(service.collections("views")).singleElement()
                .extracting(DataBrowserCollection::kind)
                .isEqualTo("view");
    }

    @Test
    void leavesTheKindAbsentForACollectionNoViewDeclares() {
        // The collection somebody made by hand, in the same database. Calling it a view would tell a
        // caller a pipeline materializes it — which is a statement about this product, made up here,
        // about a collection this product has never touched.
        DataBrowserService service = service(
                store(VIEWS, view("v_order_state", "order_state", "One row per order")),
                config -> List.of("order_state", "audit_log"),
                schemas());

        assertThat(service.collections("views"))
                .extracting(DataBrowserCollection::name, DataBrowserCollection::kind)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("order_state", "view"),
                        org.assertj.core.groups.Tuple.tuple("audit_log", null));
    }

    @Test
    void callsItAViewEvenWhenTheDeclarationSaidNothingAboutIt() {
        // What separates the two answers: `kind` is about whether it was declared at all, `description`
        // about whether the declaration bothered to say anything. Deriving one from the other would
        // make an undescribed view indistinguishable from a collection nobody declared.
        DataBrowserService service = service(
                store(VIEWS, view("v_order_state", "order_state", null)),
                config -> List.of("order_state"),
                schemas());

        assertThat(service.collections("views")).singleElement()
                .extracting(DataBrowserCollection::kind, DataBrowserCollection::description)
                .containsExactly("view", null);
    }

    @Test
    void carriesTheFieldsDiscoveryFoundOnThatCollection() {
        // Top-level fields, an array one among them: what tells the caller `shipments` is there at all.
        DataBrowserService service = service(
                store(VIEWS),
                config -> List.of("order_state"),
                schemas(discovered("order_state", "id", "status", "shipments")));

        assertThat(service.collections("views"))
                .singleElement()
                .extracting(DataBrowserCollection::fields)
                .isEqualTo(List.of("id", "status", "shipments"));
    }

    @Test
    void leavesFieldsAbsentWhenNothingHasBeenDiscoveredOnTheConnection() {
        DataBrowserService service =
                service(store(VIEWS), config -> List.of("order_state"), schemas());

        assertThat(service.collections("views")).singleElement()
                .extracting(DataBrowserCollection::fields)
                .isNull();
    }

    @Test
    void leavesFieldsAbsentForACollectionTheDiscoveredModelNeverNamed() {
        // Discovery ran, and said nothing about this collection. That is the same thing as not having
        // run for the purpose of this answer, and the opposite of "it has no fields".
        DataBrowserService service = service(
                store(VIEWS),
                config -> List.of("order_state", "audit_log"),
                schemas(discovered("order_state", "id")));

        assertThat(service.collections("views"))
                .extracting(DataBrowserCollection::name, DataBrowserCollection::fields)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("order_state", List.of("id")),
                        org.assertj.core.groups.Tuple.tuple("audit_log", null));
    }

    @Test
    void carriesTheDescriptionOfACollectionSomeViewDeclares() {
        DataBrowserService service = service(
                store(VIEWS, view("v_order_state", "order_state", "One row per order, shipments inlined")),
                config -> List.of("order_state"),
                schemas());

        assertThat(service.collections("views")).singleElement()
                .extracting(DataBrowserCollection::description)
                .isEqualTo("One row per order, shipments inlined");
    }

    @Test
    void leavesTheDescriptionAbsentForACollectionNoViewDeclares() {
        // The common case once the browsable range is any declared source: most collections in a
        // database were never authored here, so there is no text to take.
        DataBrowserService service = service(
                store(VIEWS, view("v_order_state", "order_state", "One row per order")),
                config -> List.of("order_state", "audit_log"),
                schemas());

        assertThat(service.collections("views"))
                .extracting(DataBrowserCollection::name, DataBrowserCollection::description)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("order_state", "One row per order"),
                        org.assertj.core.groups.Tuple.tuple("audit_log", null));
    }

    @Test
    void leavesTheDescriptionAbsentWhenTheDeclaringViewWroteNone() {
        // Declared, but with nothing said about it. Being declared is not itself a description.
        DataBrowserService service = service(
                store(VIEWS, view("v_order_state", "order_state", null)),
                config -> List.of("order_state"),
                schemas());

        assertThat(service.collections("views")).singleElement()
                .extracting(DataBrowserCollection::description)
                .isNull();
    }

    // ---- fixtures --------------------------------------------------------------------------------

    /** A view declaring where it materializes, and optionally what it is. */
    private static ViewResource view(String id, String collection, String description) {
        return new ViewResource(
                id,
                description == null ? null : new Metadata(null, description),
                null,
                new Storage(null, new Storage.Warm(collection, null), null),
                null,
                null);
    }

    /** One connection's discovery, holding a single table with the named fields. */
    private static DiscoveredSourceModel discovered(String table, String... fields) {
        List<SourceField> columns = new ArrayList<>(fields.length);
        for (String field : fields) {
            columns.add(new SourceField(field, "string"));
        }
        return new DiscoveredSourceModel(
                "views", "mongodb", 1L,
                new SourceModel(List.of(new SourceTable(table, columns, List.of(), List.of()))));
    }

    private static SchemaStore schemas(DiscoveredSourceModel... stored) {
        Map<String, DiscoveredSourceModel> byConnection = new LinkedHashMap<>();
        for (DiscoveredSourceModel discovery : stored) {
            byConnection.put(discovery.connectionId(), discovery);
        }
        return new SchemaStore() {
            @Override
            public void save(DiscoveredSourceModel discovered) {
                byConnection.put(discovered.connectionId(), discovered);
            }

            @Override
            public Optional<DiscoveredSourceModel> get(String connectionId) {
                return Optional.ofNullable(byConnection.get(connectionId));
            }
        };
    }

    private static DataBrowserService service(
            ArtifactStore store, DataBrowserCollectionsProbe listing, SchemaStore schemas) {
        return new DataBrowserService(
                store, schemas, listing, (config, collection) -> null, (config, query) -> NOTHING,
                (config, request, listener) -> {
                    throw new AssertionError("a listing must not open a follow");
                });
    }

    private static ArtifactStore store(Resource... stored) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : stored) {
            byId.put(resource.id(), resource);
        }
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(artifact -> byId.put(artifact.id(), artifact));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(byId.get(id));
            }

            @Override
            public List<Resource> list() {
                return new ArrayList<>(byId.values());
            }
        };
    }
}
