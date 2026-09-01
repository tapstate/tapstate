package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.control.core.DataBrowserCriteria.All;
import io.tapstate.control.core.DataBrowserCriteria.Any;
import io.tapstate.control.core.DataBrowserCriteria.Match;
import io.tapstate.control.core.DataBrowserCriteria.Operator;
import io.tapstate.runtime.probe.DataBrowserCollectionsProbe;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserFilter;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DataBrowserQuery;
import io.tapstate.spi.store.DataBrowserSort;
import io.tapstate.spi.store.DataBrowserTableInfo;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The control plane's three data-browser verbs: resolving a declared source to the connection its
 * reads run on, refusing a name that is not one, and driving the whitelisted probes.
 */
class DataBrowserServiceTest {

    /**
     * A follow probe that is never driven. Every case here is about the bounded reads, and a probe that
     * answered would let one of them pass by following instead of reading.
     */
    private static final io.tapstate.runtime.probe.DataBrowserTailProbe NO_FOLLOWS =
            (config, request, listener) -> {
                throw new AssertionError("a bounded read must not open a follow");
            };


    /** What a probe answers when a test does not care what came back. */
    private static final DataBrowserPreview NOTHING = new DataBrowserPreview(List.of(), null, false);

    private static final SourceResource VIEWS = new SourceResource(
            "views", null, "mongodb",
            Map.of("uri", "mongodb://db.local", "database", "shop"),
            null, null, null, null);

    @Test
    void listsThroughTheProbeOnTheSourcesOwnConnection() {
        AtomicReference<ConnectionConfig> driven = new AtomicReference<>();
        DataBrowserService service = service(store(VIEWS), config -> {
            driven.set(config);
            return List.of("order_state", "customers");
        });

        assertThat(service.collections("views"))
                .extracting(DataBrowserCollection::name)
                .containsExactly("order_state", "customers");
        assertThat(driven.get().id()).isEqualTo("views");
        assertThat(driven.get().connectorId()).isEqualTo("mongodb");
        assertThat(driven.get().settings()).containsEntry("database", "shop");
    }

    @Test
    void refusesAnOrderFieldWhoseSpellingIsMalformedWithACode() {
        DataBrowserService service = service(store(VIEWS), config -> List.of("order_state"));

        // Below this ring the spelling refuses bare, and nothing maps a bare refusal: unturned it is a
        // 500 for something the caller typed.
        assertThatThrownBy(() -> service.find("views", "order_state", null,
                new DataBrowserSortOrder("price\\usd", DataBrowserSortOrder.Direction.ASC), 10))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }

    @Test
    void refusesASourceIdThatIsNotStored() {
        DataBrowserService service = service(store(VIEWS), config -> List.of());

        assertThatThrownBy(() -> service.collections("absent"))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("source.not-found");
    }

    @Test
    void refusesAStoredArtifactThatIsNotASource() {
        // A pipeline has an id like a source's and no connection at all; resolving one would otherwise
        // fall through to a null connector and fail somewhere far from the name the user typed.
        Resource pipeline =
                new PipelineResource("orders", null, List.of("views"), null, null, null, null, null);
        DataBrowserService service = service(store(pipeline), config -> List.of());

        assertThatThrownBy(() -> service.collections("orders"))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("source.not-found");
    }

    @Test
    void refusesACollectionTheSourcesDatabaseDoesNotHold() {
        DataBrowserService service = service(store(VIEWS), config -> List.of("order_state"));

        assertThatThrownBy(() -> service.stats("views", "absent"))
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> {
                    TapstateException coded = (TapstateException) failure;
                    assertThat(coded.code().code()).isEqualTo("data-browser.unknown-collection");
                    assertThat(coded.args())
                            .containsEntry("source", "views")
                            .containsEntry("collection", "absent");
                });
    }

    @Test
    void doesNotDriveTheStatsProbeForACollectionThatIsNotThere() {
        // The refusal has to happen before the read, not be mapped out of its failure: a connector
        // reports nothing distinguishable for a collection that is simply absent.
        AtomicReference<String> read = new AtomicReference<>();
        DataBrowserService service = new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> List.of("order_state"),
                (config, collection) -> {
                    read.set(collection);
                    return new DataBrowserTableInfo(0L, 0L, 0L);
                },
                (config, query) -> NOTHING,
                NO_FOLLOWS);

        assertThatThrownBy(() -> service.stats("views", "absent")).isInstanceOf(TapstateException.class);
        assertThat(read.get()).isNull();
    }

    @Test
    void doesNotDriveTheFindProbeForACollectionThatIsNotThere() {
        // The same refusal on the read that would look most complete if it slipped through: a query
        // against a collection that does not exist comes back empty, which reads exactly like a
        // collection that holds nothing.
        AtomicReference<DataBrowserQuery> read = new AtomicReference<>();
        DataBrowserService service = new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> List.of("order_state"),
                (config, collection) -> null,
                (config, query) -> {
                    read.set(query);
                    return NOTHING;
                },
                NO_FOLLOWS);

        assertThatThrownBy(() -> service.find("views", "absent", null, null, 10))
                .isInstanceOf(TapstateException.class);
        assertThat(read.get()).isNull();
    }

    @Test
    void reportsWhatTheStatsProbeAnswersForACollectionThatIsThere() {
        AtomicReference<String> read = new AtomicReference<>();
        DataBrowserService service = new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> List.of("order_state"),
                (config, collection) -> {
                    read.set(collection);
                    return new DataBrowserTableInfo(512L, 40960L, 80L);
                },
                (config, query) -> NOTHING,
                NO_FOLLOWS);

        DataBrowserStatsReport report = service.stats("views", "order_state");

        assertThat(report.numOfRows()).isEqualTo(512L);
        assertThat(report.storageSize()).isEqualTo(40960L);
        assertThat(report.avgObjSize()).isEqualTo(80L);
        assertThat(read.get()).isEqualTo("order_state");
    }

    @Test
    void reportsASizeTheConnectorDidNotGiveAsUnreportedRatherThanZero() {
        // Null means the connector reported nothing, and the surfaces read it that way. Filling in a zero
        // here would state as fact - to every face at once - the one thing nobody worked out.
        DataBrowserService service = new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> List.of("order_state"),
                (config, collection) -> new DataBrowserTableInfo(null, null, null),
                (config, query) -> NOTHING,
                NO_FOLLOWS);

        DataBrowserStatsReport report = service.stats("views", "order_state");

        assertThat(report.numOfRows()).isNull();
        assertThat(report.storageSize()).isNull();
        assertThat(report.avgObjSize()).isNull();
    }

    @Test
    void carriesTheCollectionFilterAndLimitIntoTheQueryTheFindProbeIsDrivenWith() {
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserPreview expected =
                new DataBrowserPreview(List.of(Map.of("order_id", "ord_123")), 512L, true);
        DataBrowserService service = new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> List.of("order_state"),
                (config, collection) -> null,
                (config, query) -> {
                    driven.set(query);
                    return expected;
                },
                NO_FOLLOWS);

        DataBrowserPreviewReport preview = service.find(
                "views", "order_state", new Match("status", Operator.EQ, "paid"), null, 25);

        // The report is a projection of what the probe answered, carried whole rather than re-derived:
        // every surface renders this, so a field lost here is lost on all four at once.
        assertThat(preview.rows()).isEqualTo(expected.rows());
        assertThat(preview.approximateTotal()).isEqualTo(512L);
        assertThat(preview.moreAvailable()).isTrue();
        assertThat(driven.get().collection()).isEqualTo("order_state");
        assertThat(driven.get().filter()).isEqualTo(
                new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "paid"));
        assertThat(driven.get().limit()).isEqualTo(25);
    }

    @Test
    void carriesACombinationIntoTheQueryTermForTerm() {
        // A combination is where a translation can lose a term and still look like it worked: the read
        // comes back with rows, just more of them than were asked for, and nothing says a term was
        // dropped.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", new All(List.of(
                new Match("status", Operator.EQ, "Paid"),
                new Match("total", Operator.GT, 100))), null, 10);

        assertThat(driven.get().filter()).isEqualTo(new DataBrowserFilter.All(List.of(
                new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "Paid"),
                new DataBrowserFilter.Match("total", DataBrowserFilter.Operator.GT, 100))));
    }

    @Test
    void carriesAnAlternativeThroughAsAnAlternativeRatherThanAConjunction() {
        // The other connective, so a translation that answered "all" to both cannot pass — and that one
        // reads as a working filter, only stricter than the caller asked for.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", new Any(List.of(
                new Match("status", Operator.EQ, "Paid"))), null, 10);

        assertThat(driven.get().filter()).isEqualTo(new DataBrowserFilter.Any(List.of(
                new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "Paid"))));
    }

    @Test
    void asksForEveryRowWhenTheCallerGivesNoCriteria() {
        // Absent has to reach the port as absent: a filter invented here would narrow a read the caller
        // asked to be whole, and the total the report carries is offered only for an unfiltered one.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", null, null, 10);

        assertThat(driven.get().filter()).isNull();
    }

    @Test
    void translatesEveryOperatorInTheVocabularyOntoItsPortTwin() {
        // The two enums are declared apart, so a value added to one and not the other is a compile error
        // in the translation — but a value mapped to the wrong twin is not, and it reads as a working
        // read that answers a different question.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        for (Operator operator : Operator.values()) {
            service.find("views", "order_state",
                    new Match("f", operator, sampleValueFor(operator)), null, 10);

            assertThat(((DataBrowserFilter.Match) driven.get().filter()).operator())
                    .as("%s must reach the port as its own twin", operator)
                    .hasToString(operator.name());
        }
    }

    /** A value the operator accepts, so the translation is what the loop above is testing. */
    private static Object sampleValueFor(Operator operator) {
        return switch (operator) {
            case IN -> List.of("a");
            case EXISTS -> true;
            case CONTAINS -> "a";
            default -> 1;
        };
    }

    @Test
    void refusesToOrderOnAColumnWhoseNameHoldsADotRatherThanOrderingByNothing() {
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        // A filter can name such a column, because a query may carry an expression. An order cannot: the
        // backend takes a sort key as a path and offers no second form, so the request travels as a path
        // that resolves nowhere, every row sorts equal, and rows come back in no particular order with
        // nothing reported. Refusing is the only way the reader learns the order they asked for was not
        // applied.
        assertThatThrownBy(() -> service.find("views", "order_state", null,
                new DataBrowserSortOrder("price\\.usd", DataBrowserSortOrder.Direction.DESC), 10))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException coded = (TapstateException) thrown;
                    assertThat(coded.code().code()).isEqualTo("data-browser.unorderable-field");
                    assertThat(coded.args())
                            .as("named as the reader wrote it -- told about `price.usd` they would go "
                                    + "looking for a different column")
                            .containsEntry("field", "price\\.usd");
                });
        assertThat(driven.get())
                .as("refused before the read, so the cost of a read that cannot be ordered is never paid")
                .isNull();
    }

    @Test
    void ordersOnANestedFieldAsBefore() {
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        // The half that keeps the refusal from being a ban on dots. A path through nested documents is
        // exactly what a sort key already is, and refusing every field with a dot in it would take that
        // away to fix something else.
        service.find("views", "order_state", null,
                new DataBrowserSortOrder("shipping.city", DataBrowserSortOrder.Direction.ASC), 10);

        assertThat(driven.get().sort().field()).isEqualTo("shipping.city");
    }

    @Test
    void carriesTheRequestedOrderIntoTheQueryTheFindProbeIsDrivenWith() {
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", null,
                new DataBrowserSortOrder("status", DataBrowserSortOrder.Direction.DESC), 10);

        // The order the surfaces express in control-ring terms reaches the port as the same order; a
        // direction dropped in translation would return rows sorted the other way, silently.
        assertThat(driven.get().sort().field()).isEqualTo("status");
        assertThat(driven.get().sort().direction()).isEqualTo(DataBrowserSort.Direction.DESC);
    }

    @Test
    void carriesAnAscendingOrderThroughAsAscending() {
        // The other half of the translation, so a switch that answered DESC to everything cannot pass.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", null,
                new DataBrowserSortOrder("status", DataBrowserSortOrder.Direction.ASC), 10);

        assertThat(driven.get().sort().direction()).isEqualTo(DataBrowserSort.Direction.ASC);
    }

    @Test
    void leavesTheOrderUnsetWhenTheCallerAsksForNone() {
        // No order asked for means the database's own, and that is the request rather than a gap in it.
        // Filling in a default here - any default - is the one thing this face promised not to do, and
        // it would be invisible: the rows would come back in some order and look deliberate.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", null, null, 10);

        assertThat(driven.get().sort()).isNull();
    }

    @Test
    void appliesTheDefaultSizeWhenTheCallerAsksForNoParticularOne() {
        // The default lives here, once, because four surfaces reach this verb and a default per surface
        // is four defaults that drift apart - and this face's whole claim is that they are one request.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", null, null, null);

        assertThat(driven.get().limit()).isEqualTo(DataBrowserService.DEFAULT_LIMIT);
        assertThat(DataBrowserService.DEFAULT_LIMIT).isEqualTo(10);
    }

    @Test
    void refusesASizePastTheCap() {
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        assertThatThrownBy(() -> service.find("views", "order_state", null, null, 201))
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> {
                    TapstateException coded = (TapstateException) failure;
                    assertThat(coded.code().code()).isEqualTo("data-browser.invalid-limit");
                    assertThat(coded.args()).containsEntry("limit", "201").containsEntry("max", "200");
                });
        assertThat(driven.get()).isNull();
    }

    @Test
    void acceptsASizeThatLandsExactlyOnTheCap() {
        // The boundary the refusal is written against: a cap that refuses the value it names would make
        // the number in the message a lie, and nothing else would report it.
        AtomicReference<DataBrowserQuery> driven = new AtomicReference<>();
        DataBrowserService service = finding(driven);

        service.find("views", "order_state", null, null, DataBrowserService.MAX_LIMIT);

        assertThat(driven.get().limit()).isEqualTo(200);
    }

    @Test
    void refusesASizeThatWouldReadNothing() {
        // Zero and below are not small reads, they are unanswerable ones: the answer would be no rows
        // plus "there is more", which reads as an empty collection with a contradiction attached.
        DataBrowserService service = finding(new AtomicReference<>());

        for (int unreadable : new int[] {0, -1}) {
            assertThatThrownBy(() -> service.find("views", "order_state", null, null, unreadable))
                    .isInstanceOf(TapstateException.class)
                    .extracting(failure -> ((TapstateException) failure).code().code())
                    .isEqualTo("data-browser.invalid-limit");
        }
    }

    @Test
    void refusesASizeBeforeItCostsAListingToCheckTheCollection() {
        // Resolving the collection is a round trip to the connector. A request that cannot be served
        // whatever comes back should not pay for it - and should not reach a connector at all.
        AtomicReference<ConnectionConfig> listed = new AtomicReference<>();
        DataBrowserService service = new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> {
                    listed.set(config);
                    return List.of("order_state");
                },
                (config, collection) -> null,
                (config, query) -> NOTHING,
                NO_FOLLOWS);

        assertThatThrownBy(() -> service.find("views", "order_state", null, null, 500))
                .isInstanceOf(TapstateException.class);

        assertThat(listed.get()).isNull();
    }

    @Test
    void refusesRowsFromAConnectorThisFaceCannotAskIn() {
        // Refused on the connector's name, before a listing and before a read - so the request never
        // reaches a driver that would fail somewhere inside itself over a query nobody wrote.
        AtomicReference<ConnectionConfig> listed = new AtomicReference<>();
        AtomicReference<DataBrowserQuery> read = new AtomicReference<>();
        DataBrowserService service = new DataBrowserService(
                store(ON_A_CONNECTOR_THAT_IS_NOT_BROWSABLE),
                new EmptySchemaStore(),
                config -> {
                    listed.set(config);
                    return List.of("orders");
                },
                (config, collection) -> null,
                (config, query) -> {
                    read.set(query);
                    return NOTHING;
                },
                NO_FOLLOWS);

        assertThatThrownBy(() -> service.find("rows_in_sql", "orders", null, null, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> {
                    TapstateException coded = (TapstateException) failure;
                    assertThat(coded.code().code()).isEqualTo("data-browser.connector-not-browsable");
                    // Both parameters carry their weight: one says which source cannot be read, the
                    // other says what can, which is the only thing a reader can act on.
                    assertThat(coded.args())
                            .containsEntry("connector", "mysql")
                            .containsEntry("browsable", "mongodb");
                });
        assertThat(read.get()).as("a read that was refused must not have been sent").isNull();
        assertThat(listed.get()).as("nor cost a round trip to find out").isNull();
    }

    @Test
    void refusesToFollowTheSameConnectorItRefusesToRead() {
        // A follow carries rows too. Gating only the bounded read would leave the same request
        // reachable by asking for it continuously instead of once.
        DataBrowserService service = service(
                store(ON_A_CONNECTOR_THAT_IS_NOT_BROWSABLE), config -> List.of("orders"));

        assertThatThrownBy(() -> service.tail("rows_in_sql", "orders", null, change -> { }))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("data-browser.connector-not-browsable");
    }

    @Test
    void stillListsAndSizesAConnectorItWillNotReadRowsFrom() {
        // Deliberate, and the reason the refusal above is worth having: these two ask nothing shaped -
        // they are the connector's own table names and table info, and they answer for any connector
        // here. Refusing them as well would hide a source a reader can perfectly well see the outline
        // of, and would make the refusal read as "this source is broken" rather than "not this verb".
        // The size probe answers what the real one answers when a connector reports nothing about a
        // collection: an info carrying no numbers. Never null - the port has no such answer, and the
        // stub the other cases share returns one only because none of them asks for a size.
        DataBrowserService service = new DataBrowserService(
                store(ON_A_CONNECTOR_THAT_IS_NOT_BROWSABLE),
                new EmptySchemaStore(),
                config -> List.of("orders"),
                (config, collection) -> new DataBrowserTableInfo(null, null, null),
                (config, query) -> {
                    throw new AssertionError("this case must not read rows");
                },
                NO_FOLLOWS);

        assertThat(service.collections("rows_in_sql"))
                .extracting(DataBrowserCollection::name)
                .containsExactly("orders");
        assertThat(service.stats("rows_in_sql", "orders")).isNotNull();
    }

    // ---- fixtures --------------------------------------------------------------------------------

    /** A declared source whose connector reads perfectly well, in a shape this face does not ask in. */
    private static final SourceResource ON_A_CONNECTOR_THAT_IS_NOT_BROWSABLE = new SourceResource(
            "rows_in_sql", null, "mysql",
            Map.of("host", "db.local", "database", "shop"),
            null, null, null, null);

    /** A service over a source holding one collection, recording the query its find probe is driven with. */
    private static DataBrowserService finding(AtomicReference<DataBrowserQuery> driven) {
        return new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> List.of("order_state"),
                (config, collection) -> null,
                (config, query) -> {
                    driven.set(query);
                    return NOTHING;
                },
                NO_FOLLOWS);
    }

    private static DataBrowserService service(ArtifactStore store, DataBrowserCollectionsProbe listing) {
        return new DataBrowserService(
                store, new EmptySchemaStore(), listing, (config, collection) -> null,
                (config, query) -> NOTHING,
                NO_FOLLOWS);
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
