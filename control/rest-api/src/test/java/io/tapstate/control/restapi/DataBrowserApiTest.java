package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.DataBrowserPreviewReport;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.DataBrowserStatsReport;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.ViewResource;
import io.tapstate.runtime.probe.DataBrowserCollectionsProbe;
import io.tapstate.runtime.probe.DataBrowserFindProbe;
import io.tapstate.runtime.probe.DataBrowserStatsProbe;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserFilter;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DataBrowserQuery;
import io.tapstate.spi.store.DataBrowserTableInfo;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three data-browser verbs projected onto HTTP, exercised end to end through a real embedded server:
 * an authenticated caller lists a declared source's collections, reads one collection's rows, and reads
 * one collection's size. The probes and the artifact store are in-memory fakes so the test needs no
 * connector or PDK; the authentication stack, the control-core service and the controller wiring are real.
 *
 * <p>What this face must not grow is asserted alongside what it must do: the read is one-shot, so no
 * continuation state travels in either direction, and the request's defaults are the control plane's own
 * rather than a second set invented here.
 */
class DataBrowserApiTest {

    /** A follow probe that is never driven: nothing here streams, and one that answered
     * would let a case pass by following instead of reading. */
    private static final io.tapstate.runtime.probe.DataBrowserTailProbe NO_FOLLOWS =
            (config, request, listener) -> {
                throw new AssertionError("no case here opens a follow");
            };

    private static final Instant NOW = Instant.parse("2026-08-13T12:00:00Z");

    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class).properties("server.port=0").run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    @BeforeEach
    void resetFakes() {
        context.getBean(FakeCollectionsProbe.class).reset();
        context.getBean(FakeFindProbe.class).reset();
        context.getBean(FakeStatsProbe.class).reset();
        context.getBean(FakeSchemaStore.class).reset();
        context.getBean(FakeTokenStore.class).clear();
    }

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    private String token(Scope scope) {
        return context.getBean(TokenService.class).create(scope);
    }

    // ---- listing a source's collections ----

    @Test
    void listsWhatTheSourcesOwnDatabaseHolds() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state", "customers");

        CollectionList body = client().get().uri("/api/sources/views/collections")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .retrieve().toEntity(CollectionList.class).getBody();

        assertThat(body.collections())
                .extracting(CollectionList.Entry::name)
                .containsExactly("order_state", "customers");
        // The read ran on the source's own connection, resolved from the declaration rather than the request.
        assertThat(context.getBean(FakeCollectionsProbe.class).lastConfig().connectorId()).isEqualTo("mongodb");
    }

    @Test
    void carriesTheKindTheFieldsAndTheAuthoredDescriptionOfEachCollection() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeSchemaStore.class).found("order_state", "id", "status", "shipments");

        Map<String, Object> listed = firstListedCollection();

        assertThat(listed).containsEntry("name", "order_state")
                .containsEntry("kind", "view")
                .containsEntry("fields", List.of("id", "status", "shipments"))
                .containsEntry("description", "One row per order, shipments inlined");
    }

    @Test
    void leavesOutTheKeyForEverythingNobodyAnswered() {
        // The whole point of the shape, asserted on the wire rather than on the record: a caller reading
        // `fields: []` is told the collection has no fields, reading `description: ""` is told somebody
        // described it as nothing, and reading `kind: "view"` on a collection made by hand is told a
        // pipeline materializes it. None is true, and only an absent key says so.
        context.getBean(FakeCollectionsProbe.class).answer("customers");

        Map<String, Object> listed = firstListedCollection();

        assertThat(listed).containsEntry("name", "customers");
        assertThat(listed)
                .doesNotContainKey("kind")
                .doesNotContainKey("fields")
                .doesNotContainKey("description");
    }

    /** The first entry of the listing body, read as raw JSON so an absent key stays distinguishable. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> firstListedCollection() {
        Map<String, Object> body = client().get().uri("/api/sources/views/collections")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .retrieve().toEntity(Map.class).getBody();
        return (Map<String, Object>) ((List<Object>) body.get("collections")).get(0);
    }

    // ---- reading one collection's rows ----

    @Test
    void carriesTheRequestedFilterOrderAndSizeThroughToTheRead() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeFindProbe.class).answer(new DataBrowserPreview(
                List.of(Map.of("order_id", "ord_123")), 512L, true));

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("filter", Map.of("field", "status", "op", "eq", "value", "paid"));
        request.put("sort", Map.of("field", "total", "dir", "desc"));
        request.put("limit", 25);

        DataBrowserPreviewReport body = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve().toEntity(DataBrowserPreviewReport.class).getBody();

        assertThat(body.rows()).containsExactly(Map.of("order_id", "ord_123"));

        DataBrowserQuery driven = context.getBean(FakeFindProbe.class).lastQuery();
        assertThat(driven.collection()).isEqualTo("order_state");
        assertThat(driven.filter()).isEqualTo(
                new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "paid"));
        assertThat(driven.limit()).isEqualTo(25);
        assertThat(driven.sort().field()).isEqualTo("total");
        assertThat(driven.sort().direction().name()).isEqualTo("DESC");
    }

    @Test
    void readsTheOtherDirectionWordAsTheOtherDirection() {
        // `asc` and `desc` are the two words this face publishes, so both are pinned: a mapping that read
        // either as the other would return rows in reverse and look entirely deliberate doing it.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        find(Map.of("sort", Map.of("field", "total", "dir", "asc")));

        assertThat(context.getBean(FakeFindProbe.class).lastQuery().sort().direction().name())
                .isEqualTo("ASC");
    }

    @Test
    void refusesADirectionThatIsNeitherOfTheTwoWords() {
        // Client-attributable input. Reading it as one of the two would serve rows in an order the caller
        // did not ask for and cannot tell apart from the one they did.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        ApiError body = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("sort", Map.of("field", "total", "dir", "sideways")))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.params()).containsKey("reason");
        // Refused before the read, not mapped out of its outcome.
        assertThat(context.getBean(FakeFindProbe.class).lastQuery()).isNull();
    }

    @Test
    void carriesACombinationThroughTermForTerm() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        find(Map.of("filter", Map.of("all", List.of(
                Map.of("field", "status", "op", "eq", "value", "Paid"),
                Map.of("field", "total", "op", "gt", "value", 100)))));

        assertThat(context.getBean(FakeFindProbe.class).lastQuery().filter())
                .isEqualTo(new DataBrowserFilter.All(List.of(
                        new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "Paid"),
                        new DataBrowserFilter.Match("total", DataBrowserFilter.Operator.GT, 100))));
    }

    @Test
    void refusesAFilterWrittenInTheStoresOwnQueryLanguage() {
        // The one shape that must not work. A face that forwarded a backend query document would forward
        // everything that language can express, including the operators that run code in the database —
        // and would hand an agent a value with no shape to it. This is what the vocabulary replaced.
        //
        // What refuses it is the strict mapper this surface is configured with, not the vocabulary check
        // below: none of `{"status": "paid"}`'s keys are ours, so binding fails before any of them is
        // read. Said plainly because the two are different guards and only one of them is exercised here.
        assertThat(refusedFilter(Map.of("status", "paid")).code()).isEqualTo("control.malformed-request");
    }

    @Test
    void refusesAFilterThatNamesNeitherATermNorACombination() {
        // The vocabulary check itself, reached by the one body that gets past the mapper without saying
        // anything: an empty object binds cleanly and every field comes out null. It is neither shape,
        // and read as either it is a request to match nothing or a request to match everything — the
        // difference between an empty answer and a whole collection, guessed at.
        assertThat(refusedFilter(Map.of()).code()).isEqualTo("control.malformed-request");
    }

    @Test
    void refusesATermWhoseOperatorIsNotOneOfTheNine() {
        // Including the backend's own spellings: `$where` is not a word here, and the refusal has to say
        // so rather than pass it down as a field name nobody will ever match.
        assertThat(refusedFilter(Map.of("field", "status", "op", "$where", "value", "1"))
                .params()).containsKey("reason");
    }

    @Test
    void carriesAnAlternativeNestedInsideAConjunction() {
        // The shape a reader types most: several conditions, one of them a choice. It is the one nesting
        // the vocabulary allows, so the boundary has to let it through rather than refuse it as depth.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        find(Map.of("filter", Map.of("all", List.of(
                Map.of("field", "id", "op", "eq", "value", 1),
                Map.of("any", List.of(
                        Map.of("field", "status", "op", "eq", "value", "0"),
                        Map.of("field", "status", "op", "eq", "value", "1")))))));

        assertThat(context.getBean(FakeFindProbe.class).lastQuery().filter())
                .isEqualTo(new DataBrowserFilter.All(List.of(
                        new DataBrowserFilter.Match("id", DataBrowserFilter.Operator.EQ, 1),
                        new DataBrowserFilter.Any(List.of(
                                new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "0"),
                                new DataBrowserFilter.Match("status", DataBrowserFilter.Operator.EQ, "1"))))));
    }

    @Test
    void refusesACombinationHoldingAnotherCombination() {
        // The nesting bound, at the one boundary where a deeper one is expressible at all. A conjunction
        // may hold an alternative -- that is the shape above -- but an alternative holds terms, so a
        // choice between groups of conditions has no vocabulary term and is refused with a reason rather
        // than crashing the bind.
        assertThat(refusedFilter(Map.of("any", List.of(
                Map.of("all", List.of(Map.of("field", "a", "op", "eq", "value", 1))))))
                .code()).isEqualTo("control.malformed-request");
    }

    @Test
    void refusesATermThatIsAlsoACombination() {
        // Two requests in one body, and no reading of it that is not a guess about which was meant.
        assertThat(refusedFilter(Map.of(
                "field", "status", "op", "eq", "value", "Paid",
                "all", List.of(Map.of("field", "total", "op", "gt", "value", 100))))
                .code()).isEqualTo("control.malformed-request");
    }

    @Test
    void refusesAMembershipTermWhoseValueIsNotASet() {
        // The vocabulary's own rule reaching the wire: over JSON the value's type is whatever was typed,
        // and this mistake reads back as an empty collection rather than as a mistake.
        assertThat(refusedFilter(Map.of("field", "status", "op", "in", "value", "Paid"))
                .code()).isEqualTo("control.malformed-request");
    }

    /** The coded refusal a malformed {@code filter} produces, asserting it never reached the probe. */
    private ApiError refusedFilter(Map<String, Object> filter) {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        ApiError body = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("filter", filter))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(context.getBean(FakeFindProbe.class).lastQuery()).isNull();
        return body;
    }

    @Test
    void saysHowManyThereAreAndThatMoreRemain() {
        // The whole defence against a preview being read as a complete answer. The read is one-shot, so
        // nothing else in the response distinguishes ten rows from the first ten of a million.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeFindProbe.class).answer(new DataBrowserPreview(
                List.of(Map.of("order_id", "ord_123")), 512L, true));

        DataBrowserPreviewReport body = find(Map.of());

        assertThat(body.approximateTotal()).isEqualTo(512L);
        assertThat(body.moreAvailable()).isTrue();
    }

    @Test
    void reportsNoTotalRatherThanZeroWhenNoneCouldBeTold() {
        // Null means not reported, and a filtered read leaves it so rather than paying a scan. Rendering
        // that as 0 would state as fact the one thing the read declined to work out.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeFindProbe.class).answer(new DataBrowserPreview(List.of(), null, false));

        DataBrowserPreviewReport body = find(Map.of("filter", Map.of("field", "status", "op", "eq", "value", "paid")));

        assertThat(body.approximateTotal()).isNull();
        assertThat(body.moreAvailable()).isFalse();
    }

    @Test
    void leavesTheOrderToTheDatabaseWhenTheRequestAsksForNone() {
        // An absent sort is the request, not a gap in it: this face imposes no order of its own.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        find(Map.of("limit", 5));

        assertThat(context.getBean(FakeFindProbe.class).lastQuery().sort()).isNull();
    }

    @Test
    void servesTheControlPlanesOwnDefaultSizeRatherThanOneOfItsOwn() {
        // Four surfaces reach this verb; a default invented here would be one of four drifting apart,
        // under a face whose claim is that they are the same request.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        find(Map.of());

        assertThat(context.getBean(FakeFindProbe.class).lastQuery().limit())
                .isEqualTo(DataBrowserService.DEFAULT_LIMIT);
    }

    // ---- no continuation state travels in either direction ----

    @Test
    void refusesARequestCarryingContinuationState() {
        // The read is one-shot. A caller that sends a cursor must be told, not quietly served an answer
        // that ignored it - which is exactly what a tolerated unknown field would produce.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        HttpStatusCode status = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("cursor", "ord_123"))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void answersWithNothingBeyondTheRowsTheTotalAndWhetherMoreRemain() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeFindProbe.class).answer(new DataBrowserPreview(List.of(), 4L, false));

        @SuppressWarnings("unchecked")
        Map<String, Object> raw = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .retrieve().toEntity(Map.class).getBody();

        assertThat(raw).containsOnlyKeys("rows", "approximateTotal", "moreAvailable");
    }

    // ---- reading one collection's size ----

    @Test
    void reportsWhatTheConnectorKnowsAboutOneCollection() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeStatsProbe.class).answer(new DataBrowserTableInfo(512L, 40960L, 80L));

        DataBrowserStatsReport body = client().get().uri("/api/sources/views/collections/order_state/stats")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .retrieve().toEntity(DataBrowserStatsReport.class).getBody();

        assertThat(body.numOfRows()).isEqualTo(512L);
        assertThat(body.storageSize()).isEqualTo(40960L);
        assertThat(body.avgObjSize()).isEqualTo(80L);
    }

    @Test
    void reportsNothingRatherThanZeroForSizesTheConnectorDidNotGive() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeStatsProbe.class).answer(new DataBrowserTableInfo(null, null, null));

        DataBrowserStatsReport body = client().get().uri("/api/sources/views/collections/order_state/stats")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .retrieve().toEntity(DataBrowserStatsReport.class).getBody();

        assertThat(body.numOfRows()).isNull();
        assertThat(body.storageSize()).isNull();
        assertThat(body.avgObjSize()).isNull();
    }

    // ---- coded refusals reach the caller as their own status, not as a server fault ----

    @Test
    void refusesACollectionTheSourceDoesNotHoldAsACodedNotFound() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        ApiError body = client().get().uri("/api/sources/views/collections/absent/stats")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("data-browser.unknown-collection");
        assertThat(body.params()).containsEntry("source", "views").containsEntry("collection", "absent");
    }

    @Test
    void refusesAnOrderByANameHoldingADotAsACodedBadRequest() {
        // Ordering by such a field cannot be served at all, so the request is refused rather than answered
        // in an order nobody applied. A refusal is the caller's to act on: served as a 500 it would be
        // indistinguishable from the product having fallen over, and retried forever.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        ApiError body = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("sort", Map.of("field", "price\\.usd", "dir", "asc")))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("data-browser.unorderable-field");
        // Named as the caller wrote it: parsed, they would go looking for a different field.
        assertThat(body.params()).containsEntry("field", "price\\.usd");
    }

    @Test
    void refusesASourceWhoseConnectorCannotAnswerTheReadAsACodedBadRequest() {
        // A connector that does not implement the read is a fact about the source this request named, not
        // a server-side failure: no retry changes it, and the caller acts by naming another source. The
        // code table keeps every connector code at 500, because the same code on a resolve path really is
        // the server's; this verb has the context, so it attributes it here.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");
        context.getBean(FakeFindProbe.class).refuseWith(new TapstateException(
                new StubConnectorCode(),
                Map.of("connector", "mongodb", "capability", "executeQuery"),
                null));

        ApiError body = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("connector.capability-missing");
        assertThat(body.params()).containsEntry("capability", "executeQuery");
    }

    @Test
    void refusesASizePastTheCapAsACodedBadRequest() {
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        ApiError body = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("limit", DataBrowserService.MAX_LIMIT + 1))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("data-browser.invalid-limit");
        assertThat(body.params()).containsEntry("max", "200");
    }

    @Test
    void refusesASourceThatIsNotDeclaredAsACodedNotFound() {
        ApiError body = client().get().uri("/api/sources/absent/collections")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("source.not-found");
    }

    // ---- the three verbs are guarded like every other ----

    @Test
    void anUnauthenticatedCallerCannotReachAnyOfTheThreeVerbs() {
        // All three, because the guard engages per handler: one of them left off the annotated surface
        // would be reachable by anyone, and the other two passing would say nothing about it.
        HttpStatusCode collections = client().get().uri("/api/sources/views/collections")
                .exchange((request, response) -> response.getStatusCode());
        HttpStatusCode stats = client().get().uri("/api/sources/views/collections/order_state/stats")
                .exchange((request, response) -> response.getStatusCode());
        HttpStatusCode find = client().post().uri("/api/sources/views/collections/order_state:find")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .exchange((request, response) -> response.getStatusCode());

        assertThat(collections).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(stats).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(find).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void aReadCredentialSufficesForTheReadThatLooksLikeAWrite() {
        // find is posted because it carries a request body, not because it changes anything: these verbs
        // read through to the connector and persist nothing, so the grade they require is read.
        context.getBean(FakeCollectionsProbe.class).answer("order_state");

        HttpStatusCode status = client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.OK);
    }

    // ---- helpers ----

    private DataBrowserPreviewReport find(Map<String, Object> body) {
        return client().post().uri("/api/sources/views/collections/order_state:find")
                .header("Authorization", "Bearer " + token(Scope.READ))
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve().toEntity(DataBrowserPreviewReport.class).getBody();
    }

    /**
     * A minimal boot config: the path prefix + security chains, the data-browser controller and the
     * coded-error advice, with the real control-core service composed over fake probes and an in-memory
     * artifact store. The JSON contract mirrors the production face, so a body carrying a field the request
     * shape has no room for is refused rather than ignored.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, DataBrowserController.class,
            ApiExceptionHandler.class})
    static class TestApp {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        JsonMapperBuilderCustomizer strictRequestShape() {
            // The production customizer itself, not a copy of it: what makes an unknown field a refusal
            // rather than a shrug is a policy this face inherits, so the test has to be standing on the
            // real one for its refusal to mean anything about the running server.
            return new ControlHttpFace().sourceJsonContract();
        }

        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        FakeTokenStore tokenStore() {
            return new FakeTokenStore();
        }

        @Bean
        TokenSecrets tokenSecrets() {
            return new FakeTokenSecrets();
        }

        @Bean
        TokenSigner tokenSigner() {
            return new FakeSigner();
        }

        @Bean
        TokenService tokenService(TokenStore store, TokenSecrets secrets, Clock clock) {
            return new TokenService(store, secrets, clock);
        }

        @Bean
        ArtifactStore artifactStore() {
            // One declared source and one view declaring where it materializes: enough for a listing to
            // have both a collection somebody described and one nobody did.
            return new DeclaredWorkspaceStore(
                    new SourceResource(
                            "views", null, "mongodb",
                            Map.of("uri", "mongodb://db.local", "database", "shop"),
                            null, null, null, null),
                    new ViewResource(
                            "v_order_state",
                            new Metadata(null, "One row per order, shipments inlined"),
                            null,
                            new Storage(null, new Storage.Warm("order_state", null), null),
                            null,
                            null));
        }

        @Bean
        FakeSchemaStore schemaStore() {
            return new FakeSchemaStore();
        }

        @Bean
        FakeCollectionsProbe collectionsProbe() {
            return new FakeCollectionsProbe();
        }

        @Bean
        FakeStatsProbe statsProbe() {
            return new FakeStatsProbe();
        }

        @Bean
        FakeFindProbe findProbe() {
            return new FakeFindProbe();
        }

        @Bean
        DataBrowserService dataBrowserService(
                ArtifactStore store,
                SchemaStore schemas,
                DataBrowserCollectionsProbe collections,
                DataBrowserStatsProbe stats,
                DataBrowserFindProbe find) {
            return new DataBrowserService(store, schemas, collections, stats, find, NO_FOLLOWS);
        }
    }

    // ---- fakes ----

    /** A store holding the declared workspace, so resolution is real and everything else is absent. */
    private static final class DeclaredWorkspaceStore implements ArtifactStore {
        private final SourceResource source;
        private final List<Resource> declared;

        private DeclaredWorkspaceStore(SourceResource source, Resource... alsoDeclared) {
            this.source = source;
            List<Resource> all = new ArrayList<>();
            all.add(source);
            all.addAll(List.of(alsoDeclared));
            this.declared = List.copyOf(all);
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            throw new UnsupportedOperationException("the browse face never writes");
        }

        @Override
        public Optional<Resource> get(String id) {
            return source.id().equals(id) ? Optional.of(source) : Optional.empty();
        }

        @Override
        public List<Resource> list() {
            return declared;
        }
    }

    /** A schema store a case seeds to say what discovery found, or leaves empty to say it never ran. */
    private static final class FakeSchemaStore implements SchemaStore {
        private DiscoveredSourceModel discovered;

        void reset() {
            discovered = null;
        }

        void found(String collection, String... fields) {
            List<SourceField> columns = new ArrayList<>(fields.length);
            for (String field : fields) {
                columns.add(new SourceField(field, "string"));
            }
            discovered = new DiscoveredSourceModel("views", "mongodb", 1L,
                    new SourceModel(List.of(new SourceTable(collection, columns, List.of(), List.of()))));
        }

        @Override
        public void save(DiscoveredSourceModel model) {
            throw new UnsupportedOperationException("the browse face never writes");
        }

        @Override
        public Optional<DiscoveredSourceModel> get(String connectionId) {
            return Optional.ofNullable(discovered)
                    .filter(model -> model.connectionId().equals(connectionId));
        }
    }

    /** Answers a canned collection list and remembers the connection it was driven on. */
    private static final class FakeCollectionsProbe implements DataBrowserCollectionsProbe {
        private List<String> answer = List.of();
        private ConnectionConfig lastConfig;

        void reset() {
            answer = List.of();
            lastConfig = null;
        }

        void answer(String... collections) {
            answer = List.of(collections);
        }

        ConnectionConfig lastConfig() {
            return lastConfig;
        }

        @Override
        public List<String> collections(ConnectionConfig config) {
            lastConfig = config;
            return answer;
        }
    }

    /** Answers a canned preview and remembers the query it was driven with. */
    private static final class FakeFindProbe implements DataBrowserFindProbe {
        private DataBrowserPreview answer = new DataBrowserPreview(List.of(), null, false);
        private DataBrowserQuery lastQuery;
        private RuntimeException refusal;

        void reset() {
            answer = new DataBrowserPreview(List.of(), null, false);
            lastQuery = null;
            refusal = null;
        }

        void answer(DataBrowserPreview preview) {
            answer = preview;
        }

        /** Drives a refusal raised behind the probe, where a connector's own failures surface. */
        void refuseWith(RuntimeException coded) {
            refusal = coded;
        }

        DataBrowserQuery lastQuery() {
            return lastQuery;
        }

        @Override
        public DataBrowserPreview find(ConnectionConfig config, DataBrowserQuery query) {
            lastQuery = query;
            if (refusal != null) {
                throw refusal;
            }
            return answer;
        }
    }

    /** Answers canned table info. */
    private static final class FakeStatsProbe implements DataBrowserStatsProbe {
        private DataBrowserTableInfo answer = new DataBrowserTableInfo(null, null, null);

        void reset() {
            answer = new DataBrowserTableInfo(null, null, null);
        }

        void answer(DataBrowserTableInfo info) {
            answer = info;
        }

        @Override
        public DataBrowserTableInfo stats(ConnectionConfig config, String collection) {
            return answer;
        }
    }

    /** An in-memory token store keyed by token id. */
    private static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(TokenRecord record) {
            byId.put(record.tokenId(), record);
        }

        @Override
        public Optional<TokenRecord> find(String tokenId) {
            return Optional.ofNullable(byId.get(tokenId));
        }

        @Override
        public void revoke(String tokenId) {
            TokenRecord existing = byId.get(tokenId);
            if (existing != null) {
                byId.put(tokenId, new TokenRecord(existing.tokenId(), existing.scope(),
                        existing.secretHash(), true, existing.createdAt()));
            }
        }

        @Override
        public List<TokenRecord> list() {
            return new ArrayList<>(byId.values());
        }
    }

    /** A deterministic secret minter: tok-N / sec-N with a reversible hash. */
    private static final class FakeTokenSecrets implements TokenSecrets {
        private int counter;

        @Override
        public GeneratedSecret generate() {
            counter++;
            return new GeneratedSecret("tok-" + counter, "sec-" + counter, "hash:sec-" + counter);
        }

        @Override
        public boolean matches(String presentedSecret, String storedHash) {
            return storedHash.equals("hash:" + presentedSecret);
        }
    }

    /** A signer whose token is a reversible {@code subject|SCOPE} encoding (unused here but required to wire). */
    private static final class FakeSigner implements TokenSigner {
        @Override
        public String issue(String subject, Scope scope) {
            return subject + "|" + scope.name();
        }

        @Override
        public Optional<VerifiedToken> verify(String token) {
            int bar = token.indexOf('|');
            if (bar < 0) {
                return Optional.empty();
            }
            return Optional.of(new VerifiedToken(token.substring(0, bar), Scope.valueOf(token.substring(bar + 1))));
        }
    }

    /**
     * The capability refusal as it reaches this face. Stubbed rather than imported: the connector ring is
     * not on this module's classpath, and what is under test is the status a connector code gets here, for
     * which the canonical string and its params are the whole of it.
     */
    private record StubConnectorCode() implements TapstateErrorCode {
        @Override
        public String code() {
            return "connector.capability-missing";
        }

        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of("connector", "capability");
        }
    }
}
