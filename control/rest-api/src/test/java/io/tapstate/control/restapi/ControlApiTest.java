package io.tapstate.control.restapi;

import io.tapstate.control.core.DataBrowserFollows;
import io.tapstate.control.core.ApplyResult;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.ArtifactOutcome;
import io.tapstate.control.core.ArtifactValidationResult;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ArtifactListEntry;
import io.tapstate.control.core.AuditGate;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.Frontend;
import io.tapstate.control.core.Operation;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.StoredArtifact;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.ValidationDiagnostic;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.client.RestClient;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The HTTP face over the control verbs: each endpoint is a thin projection of a registered operation
 * onto {@code POST/GET /api/...}, and the endpoint table is a derivation of the registry — no endpoint
 * invents a verb. The apply / get / list verbs round-trip through the (fake-store-backed) control-core
 * services; the connection-test verb is routed onto its (fake-backed) service. The context is booted programmatically so
 * the module stays on the reactor's JUnit line.
 */
class ControlApiTest {

    private static final String STAMPED_PRINCIPAL = "alice";
    private static final Instant AUDIT_INSTANT = Instant.parse("2026-07-15T10:15:30Z");

    private static ConfigurableApplicationContext context;
    private static int port;

    /** An audit store that captures every record it is asked to write. */
    static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

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
    void resetStore() {
        ((InMemoryArtifactStore) context.getBean(ArtifactStore.class)).clear();
        context.getBean(RecordingAuditStore.class).records.clear();
    }

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    private ApplyResult applyDrafts(String... drafts) {
        List<Map<String, String>> body = new ArrayList<>();
        for (String draft : drafts) {
            body.add(Map.of("content", draft));
        }
        return client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", body))
                .retrieve().toEntity(ApplyResult.class).getBody();
    }

    // ---- the verbs project onto HTTP ----

    @Test
    void applyAuditsTheRequestPrincipalForEachChangedArtifact() {
        // artifact.apply is an audited write, so the endpoint must reach the store through the audit gate
        // and carry the request's own principal into the record — not a constant the controller invents.
        // The interceptor's job (deriving that principal from the credential) is asserted in AuthTest; the
        // request-attribute-to-audit chain for this same mechanism is asserted in PipelineApiTest.
        applyDrafts(TGT_MY, SRC_ORA);

        assertThat(context.getBean(RecordingAuditStore.class).records).extracting(
                        AuditRecord::operationId, AuditRecord::principal, AuditRecord::resourceId)
                .containsExactly(
                        tuple("artifact.apply", STAMPED_PRINCIPAL, "tgt_my"),
                        tuple("artifact.apply", STAMPED_PRINCIPAL, "src_ora"));
    }

    @Test
    void applyUpsertsAndReturnsTheOutcomes() {
        ApplyResult result = applyDrafts(TGT_MY);

        assertThat(result.outcomes()).singleElement().satisfies(o -> {
            assertThat(o.id()).isEqualTo("tgt_my");
            assertThat(o.kind()).isEqualTo("source");
            assertThat(o.change()).isEqualTo(ArtifactOutcome.Change.CREATED);
            assertThat(o.contentHash()).matches("[0-9a-f]{64}");
        });
    }

    @Test
    void validateReportsChangesAndDiagnosticsWithoutWritingOrAuditing() {
        ArtifactValidationResult valid = client().post().uri("/api/artifacts:validate")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", TGT_MY))))
                .retrieve().toEntity(ArtifactValidationResult.class).getBody();

        assertThat(valid.valid()).isTrue();
        assertThat(valid.diagnostics()).isEmpty();
        assertThat(valid.outcomes()).singleElement()
                .extracting(ArtifactOutcome::change).isEqualTo(ArtifactOutcome.Change.CREATED);
        assertThat(context.getBean(ArtifactStore.class).list()).isEmpty();
        assertThat(context.getBean(RecordingAuditStore.class).records).isEmpty();

        ArtifactValidationResult invalid = client().post().uri("/api/artifacts:validate")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", UNKNOWN_FIELD_DRAFT))))
                .retrieve().toEntity(ArtifactValidationResult.class).getBody();

        assertThat(invalid.valid()).isFalse();
        assertThat(invalid.diagnostics()).singleElement()
                .extracting("code").isEqualTo("dsl.unknown-field");
        assertThat(context.getBean(ArtifactStore.class).list()).isEmpty();
        assertThat(context.getBean(RecordingAuditStore.class).records).isEmpty();
    }

    @Test
    void applyAndValidateCarryAWarningsArrayApartFromTheDiagnostics() {
        // The advisory findings are their own column on the wire: a client must be able to tell "the batch
        // applied, and here is something to know" from "the batch was refused" without reading a severity
        // field. An empty run still carries the array, so an absent one is a broken contract, not a clean batch.
        String applied = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", TGT_MY))))
                .retrieve().body(String.class);
        assertThat(applied).contains("\"warnings\":[]").contains("\"outcomes\":[");

        String validated = client().post().uri("/api/artifacts:validate")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", TGT_MY))))
                .retrieve().body(String.class);
        assertThat(validated).contains("\"warnings\":[]").contains("\"diagnostics\":[]");
    }

    @Test
    void anAdvisoryFindingReachesTheClientWithItsCodeAndParams() {
        // The stub rule in TestApp reports on any artifact named `warned_*`, so this exercises the whole
        // channel — rule -> plan -> result -> JSON — rather than only the shape of an empty column.
        ApplyResult result = applyDrafts(WARNED_SRC);

        assertThat(result.outcomes()).extracting(ArtifactOutcome::id).containsExactly("warned_src");
        assertThat(result.warnings()).singleElement().satisfies(warning -> {
            assertThat(warning.code()).isEqualTo(STUB_ADVISORY_CODE);
            assertThat(warning.params()).containsEntry("id", "warned_src");
        });

        ArtifactValidationResult validated = client().post().uri("/api/artifacts:validate")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", WARNED_SRC))))
                .retrieve().toEntity(ArtifactValidationResult.class).getBody();

        assertThat(validated.valid()).as("an advisory finding is not a refusal").isTrue();
        assertThat(validated.diagnostics()).isEmpty();
        assertThat(validated.warnings()).extracting(ValidationDiagnostic::code)
                .containsExactly(STUB_ADVISORY_CODE);
    }

    @Test
    void getReadsBackTheAppliedArtifactAsItsCanonicalForm() {
        applyDrafts(TGT_MY);

        ResponseEntity<StoredArtifact> got = client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class);

        assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(got.getBody().id()).isEqualTo("tgt_my");
        assertThat(got.getBody().canonicalForm()).isEqualTo(offlineCanonical(TGT_MY));
    }

    @Test
    void anApplyCarryingTheCurrentPreconditionIsAcceptedAndReplacesTheVersion() {
        applyDrafts(TGT_MY);
        String current = client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class).getBody().contentHash();

        ApplyResult result = applyDraftsWithPrecondition(TGT_MY_CHANGED, current);

        assertThat(result.outcomes()).extracting(ArtifactOutcome::id).containsExactly("tgt_my");
        assertThat(client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class).getBody().canonicalForm())
                .isEqualTo(offlineCanonical(TGT_MY_CHANGED));
    }

    @Test
    void anApplyCarryingAStalePreconditionIsRefusedAndLeavesTheStoredBytesUnchanged() {
        applyDrafts(TGT_MY);
        String before = client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class).getBody().canonicalForm();

        ApiError body = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(
                        Map.of("content", TGT_MY_CHANGED, "expectedContentHash", "0".repeat(64)))))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("artifact.version-conflict");
        // Not merely "an error came back": the refusal has to precede the write, so the stored bytes are
        // the ones from before the call. An implementation that upserts then checks would pass a test
        // that only looked at the status.
        assertThat(client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class).getBody().canonicalForm())
                .isEqualTo(before);
    }

    @Test
    void anApplyCarryingAPreconditionForAnIdThatIsNotStoredIsNotFound() {
        // A precondition says "I am editing a version I read". If the id is not there at all, the author's
        // target was removed, and saying "version conflict" would send them to re-read something that no
        // longer exists.
        ApiError body = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(
                        Map.of("content", TGT_MY, "expectedContentHash", "0".repeat(64)))))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("artifact.not-found");
    }

    @Test
    void oneStaleDraftRefusesTheWholeBatchSoNoOtherDraftInItIsWritten() {
        // The batch guarantee, on the face where a partial write would be visible as a partial response.
        // The first draft is new and perfectly valid; if the refusal were per-draft rather than per-batch
        // it would land, and the caller would be left with half an edit it never asked to be split.
        //
        // The stale draft is deliberately second. That also discriminates an implementation that judges
        // preconditions for only the first draft it is handed — which passes a batch whose stale draft is
        // anywhere else, and would look correct in any test that put the bad one in front.
        applyDrafts(TGT_MY);

        HttpStatusCode refusal = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(
                        Map.of("content", SRC_ORA),
                        Map.of("content", TGT_MY_CHANGED, "expectedContentHash", "0".repeat(64)))))
                .exchange((request, response) -> response.getStatusCode());

        // The refusal's own status is pinned, not merely the absence of a write: any other refusal —
        // a parse error, an unrelated validation failure — would also leave src_ora unwritten, and
        // would pass an assertion that only looked at the store.
        assertThat(refusal).isEqualTo(HttpStatus.PRECONDITION_FAILED);
        HttpStatusCode secondDraft = client().get().uri("/api/artifacts/src_ora")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(secondDraft).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void anApplyWithNoPreconditionKeepsOverwritingAsItAlwaysHas() {
        // The backward-compatibility half: the field is optional, and a caller that never sends it is
        // never refused by a check it did not ask for.
        applyDrafts(TGT_MY);

        applyDrafts(TGT_MY_CHANGED);

        assertThat(client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class).getBody().canonicalForm())
                .isEqualTo(offlineCanonical(TGT_MY_CHANGED));
    }

    private ApplyResult applyDraftsWithPrecondition(String draft, String expectedContentHash) {
        return client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(
                        Map.of("content", draft, "expectedContentHash", expectedContentHash))))
                .retrieve().toEntity(ApplyResult.class).getBody();
    }

    @Test
    void whatAReadReturnsAsItsHashIsAcceptedVerbatimAsTheRemovalPrecondition() {
        // The round-trip that makes the removal usable by a caller that cannot hash for itself: read the
        // artifact, hand the hash straight back as If-Match, and the removal is accepted. Asserting only
        // that the field is present and 64 characters long would pass for a hash over the id, the raw
        // draft, or a constant — every one of which answers 412 here.
        applyDrafts(TGT_MY);

        StoredArtifact got = client().get().uri("/api/artifacts/tgt_my")
                .retrieve().toEntity(StoredArtifact.class).getBody();

        HttpStatusCode status = client().method(HttpMethod.DELETE).uri("/api/artifacts/tgt_my")
                .header(HttpHeaders.IF_MATCH, "\"" + got.contentHash() + "\"")
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    void aListedArtifactCarriesTheSameHashItsOwnReadReturns() {
        // A caller that lists and then removes must not need a second round trip to re-read each one.
        applyDrafts(SRC_ORA, TGT_MY, PIPELINE);

        ArtifactList listed = client().get().uri("/api/artifacts")
                .retrieve().toEntity(ArtifactList.class).getBody();

        // The size is asserted first because the per-element check below passes on an empty list: a
        // regression that listed nothing at all would otherwise read as every element agreeing.
        assertThat(listed.artifacts()).hasSize(3);
        assertThat(listed.artifacts()).allSatisfy(a -> assertThat(a.contentHash())
                .isEqualTo(client().get().uri("/api/artifacts/" + a.id())
                        .retrieve().toEntity(StoredArtifact.class).getBody().contentHash()));
    }

    @Test
    void getAnUnknownIdIsNotFound() {
        HttpStatusCode status = client().get().uri("/api/artifacts/no_such_id")
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void listReturnsEveryStoredArtifact() {
        applyDrafts(SRC_ORA, TGT_MY, PIPELINE);

        ArtifactList listed = client().get().uri("/api/artifacts")
                .retrieve().toEntity(ArtifactList.class).getBody();

        assertThat(listed.artifacts()).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my", "ora2my_ods");
    }

    @Test
    void listReturnsReadableSiblingsWhenOneStoredDocumentIsUnreadable() {
        applyDrafts(TGT_MY);
        InMemoryArtifactStore store = (InMemoryArtifactStore) context.getBean(ArtifactStore.class);
        store.putUnreadable("p1", "pipeline", "not: [valid");

        ArtifactList listed = client().get().uri("/api/artifacts")
                .retrieve().toEntity(ArtifactList.class).getBody();

        assertThat(listed.artifacts()).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("tgt_my", "p1");
        assertThat(listed.artifacts()).filteredOn(a -> a.id().equals("tgt_my")).singleElement()
                .satisfies(a -> assertThat(a.readable()).isTrue());
        assertThat(listed.artifacts()).filteredOn(a -> a.id().equals("p1")).singleElement()
                .satisfies(a -> {
                    assertThat(a.kind()).isEqualTo("pipeline");
                    assertThat(a.canonicalForm()).isEqualTo("not: [valid");
                    assertThat(a.readable()).isFalse();
                });
    }

    @Test
    void listByKindFiltersToThatKind() {
        applyDrafts(SRC_ORA, TGT_MY, PIPELINE);

        ArtifactList sources = client().get().uri("/api/artifacts?kind=source")
                .retrieve().toEntity(ArtifactList.class).getBody();

        assertThat(sources.artifacts()).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my");
    }

    // ---- the removal verb ----

    @Test
    void deleteRemovesTheArtifactAndAnswersNoContent() {
        String hash = applyDrafts(TGT_MY).outcomes().get(0).contentHash();

        HttpStatusCode status = client().method(HttpMethod.DELETE).uri("/api/artifacts/tgt_my")
                .header(HttpHeaders.IF_MATCH, "\"" + hash + "\"")
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.NO_CONTENT);
        // The removal is real: the read path answers 404 without having learned to filter anything, and
        // the listing is short by exactly that row. A tombstone would keep both of these green.
        HttpStatusCode afterwards = client().get().uri("/api/artifacts/tgt_my")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(afterwards).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(context.getBean(ArtifactStore.class).list()).isEmpty();
        assertThat(context.getBean(RecordingAuditStore.class).records)
                .extracting(AuditRecord::operationId, AuditRecord::resourceId)
                .contains(tuple("artifact.delete", "tgt_my"));
    }

    @Test
    void deleteWithNoIfMatchIsPreconditionRequiredAndKeepsTheArtifact() {
        applyDrafts(TGT_MY);

        ApiError body = client().method(HttpMethod.DELETE).uri("/api/artifacts/tgt_my")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.PRECONDITION_REQUIRED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("artifact.precondition-required");
        assertThat(body.params()).containsEntry("id", "tgt_my");
        assertThat(context.getBean(ArtifactStore.class).get("tgt_my")).isPresent();
    }

    /**
     * A malformed precondition is the same refusal as a missing one, not a mismatched one. Letting the raw
     * header value through would reach the store as a hash that happens not to match, answering "someone
     * else changed it" for a request that never carried a version at all.
     */
    @Test
    void deleteWithAnIfMatchThatIsNotAQuotedHashIsPreconditionRequired() {
        applyDrafts(TGT_MY);

        for (String malformed : List.of("*", "not-a-hash", "\"deadbeef\"")) {
            ApiError body = client().method(HttpMethod.DELETE).uri("/api/artifacts/tgt_my")
                    .header(HttpHeaders.IF_MATCH, malformed)
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode()).as(malformed)
                                .isEqualTo(HttpStatus.PRECONDITION_REQUIRED);
                        return response.bodyTo(ApiError.class);
                    });
            assertThat(body.code()).as(malformed).isEqualTo("artifact.precondition-required");
        }
        assertThat(context.getBean(ArtifactStore.class).get("tgt_my")).isPresent();
    }

    @Test
    void deleteWithAStaleIfMatchIsPreconditionFailedAndLeavesTheStoredBytesUntouched() {
        applyDrafts(TGT_MY);
        String stale = "0".repeat(64);
        String before = canonicalOf("tgt_my");

        ApiError body = client().method(HttpMethod.DELETE).uri("/api/artifacts/tgt_my")
                .header(HttpHeaders.IF_MATCH, "\"" + stale + "\"")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.PRECONDITION_FAILED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("artifact.version-conflict");
        assertThat(canonicalOf("tgt_my")).isEqualTo(before);
    }

    @Test
    void deleteOfAReferencedArtifactIsAConflictNamingTheReferrersWithNothingRemoved() {
        applyDrafts(SRC_ORA, TGT_MY, PIPELINE);
        // The precondition is derived from the canonical form the read face returns: the stored content
        // hash is the hash of exactly those bytes, so a reader never has to be told it separately.
        String hash = CanonicalHash.of(client().get().uri("/api/artifacts/src_ora")
                .retrieve().toEntity(StoredArtifact.class).getBody().canonicalForm());

        ApiError body = client().method(HttpMethod.DELETE).uri("/api/artifacts/src_ora")
                .header(HttpHeaders.IF_MATCH, "\"" + hash + "\"")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("artifact.in-use");
        // The caller is told who to deal with, so it needs no second query to act on the refusal.
        assertThat(body.params().get("referrers").toString()).contains("ora2my_ods");
        // Nothing cascaded and nothing was removed first: all three are still stored.
        assertThat(context.getBean(ArtifactStore.class).list()).extracting(Resource::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my", "ora2my_ods");
    }

    /** The stored canonical form of an artifact, for asserting the bytes did not move. */
    private static String canonicalOf(String id) {
        return new CanonicalWriter().write(context.getBean(ArtifactStore.class).get(id).orElseThrow());
    }

    // ---- coded errors project onto structured HTTP responses ----

    @Test
    void applyingAnInvalidDraftIsABadRequestWithACodedBody() {
        // A validation failure (an unknown field) surfaces the dsl.* coded diagnostic before any store
        // write; the advice projects it onto a 400 with the {code, params, message} body — no 500.
        ApiError body = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", UNKNOWN_FIELD_DRAFT))))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("dsl.unknown-field");
        assertThat(body.message()).isEqualTo("Unknown field 'snapshot_mode' at options.snapshot_mode.");
        assertThat(body.params()).containsEntry("field", "snapshot_mode");
    }

    @Test
    void applyingWithNullDraftsIsABadRequestWithACodedBody() {
        // A structurally malformed request — an apply body with no drafts array ({} deserializes to a null
        // drafts field) — is a client-attributable, diagnosable error. It is refused at the HTTP boundary
        // with a coded control.malformed-request (400) carrying a human reason, not a bare invariant crash
        // (a 500) deeper in the service.
        ApiError body = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.params()).containsKey("reason");
        // the message is rendered from the catalog with the reason substituted, not the bare code
        assertThat(body.message()).isNotBlank().isNotEqualTo("control.malformed-request").contains("drafts");
    }

    @Test
    void applyAndValidateRejectNullDraftEntriesAtTheBoundary() {
        for (String endpoint : List.of("/api/artifacts:apply", "/api/artifacts:validate")) {
            ApiError body = client().post().uri(endpoint)
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .body("{\"drafts\":[null]}")
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                        return response.bodyTo(ApiError.class);
                    });

            assertThat(body.code()).isEqualTo("control.malformed-request");
            assertThat(body.params()).containsKey("reason");
        }
    }

    @Test
    void applyingAnEmptyDraftsArrayIsAValidNoOp() {
        // The boundary guard refuses only a missing drafts array (null), never an empty one: applying zero
        // drafts is a legitimate no-op, not a malformed request. It answers 200 with no outcomes and writes
        // nothing — the accept half of the request-validation boundary, so the guard cannot over-reach into
        // rejecting a well-formed empty batch.
        ApplyResult result = client().post().uri("/api/artifacts:apply")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of()))
                .retrieve().toEntity(ApplyResult.class).getBody();

        assertThat(result.outcomes()).isEmpty();
    }

    @Test
    void anUncodedProgrammerErrorStaysABareServerErrorNotACodedBody() {
        // The discipline the coded-4xx request-validation boundary must not erode: a genuine uncoded
        // throwable (a programmer error / invariant violation) is never laundered into a pretty coded body.
        // The advice catches only TapstateException, so a bare RuntimeException stays a bare 500 with no {code}
        // envelope. Exercised through a test-only fault endpoint mounted outside the /api verb surface.
        Map<String, Object> body = client().get().uri("/boom")
                .accept(MediaType.APPLICATION_JSON)
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().is5xxServerError()).isTrue();
                    return response.bodyTo(new ParameterizedTypeReference<Map<String, Object>>() {});
                });

        assertThat(body).doesNotContainKey("code");
    }

    // ---- the anonymous probe lives outside the verb surface ----

    @Test
    void healthzAnswersAtTheRootOutsideTheApiPrefix() {
        // The load-balancer probe is pure HTTP: anonymous, at the root, not a registry verb. It must
        // not be swept under the /api prefix — that root is exactly what the plain-@Controller carve-out
        // in the path-prefix rule exists for.
        ResponseEntity<String> probe = client().get().uri("/healthz").retrieve().toEntity(String.class);

        assertThat(probe.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(probe.getBody()).isEqualTo("ok");

        HttpStatusCode underApi = client().get().uri("/api/healthz")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(underApi).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void versionAnswersAtTheRootWithWhatTheBuildStampedIn() {
        // The server's own version must be askable before anyone logs in: the CLI reads it while
        // connecting, and connecting is decoupled from authenticating. So this is the second anonymous
        // root endpoint, and like the probe it is a plain @Controller outside the /api prefix.
        // The expected value comes from the build, not from a constant in this module -- a version the
        // code carries can only be compared with itself, which pins nothing.
        String projectVersion = System.getProperty("tapstate.project.version");
        assertThat(projectVersion)
                .as("the build must pass -Dtapstate.project.version so this guard can run at all")
                .isNotBlank();

        ResponseEntity<Map<String, Object>> answer = client().get().uri("/version")
                .retrieve().toEntity(new ParameterizedTypeReference<>() { });

        assertThat(answer.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(answer.getBody()).containsEntry("version", projectVersion);
        // Both reserved fields are in the shape from the first release on, so a client that learns to
        // read them never has to tell "this server is too old" from "this server left them out". They
        // stay empty until what fills them lands. None of the three numbers derives from another: the
        // product version here, the DSL grammar version, and the system-data version are independent.
        assertThat(answer.getBody()).containsKeys("dslVersions", "dataVersion");
        assertThat((List<?>) answer.getBody().get("dslVersions")).isEmpty();

        HttpStatusCode versionUnderApi = client().get().uri("/api/version")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(versionUnderApi).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void clusterMembersIsRoutedButNotYetImplemented() {
        // Topology must never leak anonymously: until the authentication interceptor and the member
        // listing land, the endpoint is reserved and answers 501 — it exposes nothing.
        HttpStatusCode status = client().get().uri("/api/cluster/members")
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
    }

    // ---- the endpoint table is a derivation of the registry ----

    @Test
    void everyApiEndpointProjectsARegisteredCliExposedVerb() {
        Set<String> cliExposed = ControlOperations.registry()
                .exposedOn(Frontend.CLI).stream()
                .map(Operation::id).collect(Collectors.toSet());

        RequestMappingHandlerMapping mapping =
                context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);

        List<String> handlersMissingVerb = new ArrayList<>();
        List<String> verbsNotDerived = new ArrayList<>();
        List<String> projected = new ArrayList<>();
        mapping.getHandlerMethods().forEach((info, handler) -> {
            boolean underApi = info.getPathPatternsCondition() != null
                    && info.getPathPatternsCondition().getPatternValues().stream()
                            .anyMatch(p -> p.startsWith("/api"));
            if (!underApi) {
                return;
            }
            Verb verb = handler.getMethodAnnotation(Verb.class);
            if (verb == null) {
                handlersMissingVerb.add(describe(handler));
            } else {
                projected.add(verb.value());
                if (!cliExposed.contains(verb.value())) {
                    verbsNotDerived.add(verb.value());
                }
            }
        });

        assertThat(handlersMissingVerb)
                .as("every /api endpoint must project a control verb (carry @Verb) — a face composes "
                        + "registered operations, it never invents an endpoint")
                .isEmpty();
        assertThat(verbsNotDerived)
                .as("every projected verb must be a registered, CLI-exposed operation")
                .isEmpty();
        assertThat(projected)
                .as("the artifact verbs, the two whitelisted connection verbs with their read-backs, and the "
                        + "topology verb are projected onto HTTP")
                .contains("artifact.apply", "artifact.get", "artifact.list", "connection.test",
                        "connection.test-result", "connection.discover-schema", "connection.schema",
                        "source.draft",
                        "cluster.members");
    }

    private static String describe(HandlerMethod handler) {
        return handler.getBeanType().getSimpleName() + "#" + handler.getMethod().getName();
    }

    /** The offline canonical contract for a draft: the exact bytes the authoring corpus golden locks. */
    private static String offlineCanonical(String draft) {
        return new CanonicalWriter().write(new DslParser().parse(draft));
    }

    /**
     * A minimal boot config: auto-configures Web MVC + the embedded servlet container, imports the path
     * prefix configuration and the verb controllers, and constructs the control-core services over an
     * in-memory artifact store (the store-backed wiring lands at the assembly root, not here).
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, SourceDraftTestConfiguration.class, ArtifactController.class,
            SourceDraftController.class, ConnectionController.class,
            ClusterController.class, HealthController.class, VersionController.class,
            ApiExceptionHandler.class, FaultController.class})
    static class TestApp {

        @Bean
        ArtifactStore artifactStore() {
            return new InMemoryArtifactStore();
        }

        @Bean
        ApplyService applyService(ArtifactStore store, AuditGate auditGate) {
            return new ApplyService(TapstateCatalog::load, store, auditGate, new EmptySchemaStore(),
                    ControlApiTest::adviseOnWarnedArtifacts);
        }

        @Bean
        RecordingAuditStore auditStore() {
            return new RecordingAuditStore();
        }

        @Bean
        AuditGate auditGate(RecordingAuditStore store) {
            return new AuditGate(store, Clock.fixed(AUDIT_INSTANT, ZoneOffset.UTC));
        }

        /**
         * Stamps a fixed principal onto every {@code /api} request. This context omits the production
         * security configuration on purpose — it asserts verb mechanics, not the guard (which AuthTest
         * asserts) — but an audited verb still needs a principal to attribute its record to, so one is
         * supplied here through the same security context controllers use in production.
         */
        @Bean
        WebMvcConfigurer principalStamp() {
            return new WebMvcConfigurer() {
                @Override
                public void addInterceptors(InterceptorRegistry registry) {
                    registry.addInterceptor(new HandlerInterceptor() {
                        @Override
                        public boolean preHandle(
                                HttpServletRequest request, HttpServletResponse response, Object handler) {
                            SecurityContext context = SecurityContextHolder.createEmptyContext();
                            context.setAuthentication(new TapstateAuthentication(
                                    TapstatePrincipal.humanJwt(new VerifiedToken(STAMPED_PRINCIPAL, Scope.ADMIN))));
                            SecurityContextHolder.setContext(context);
                            return true;
                        }

                        @Override
                        public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler, Exception failure) {
                            SecurityContextHolder.clearContext();
                        }
                    }).addPathPatterns("/api/**");
                }
            };
        }

        @Bean
        SecurityFilterChain permitAllSecurity(HttpSecurity http) throws Exception {
            http.csrf(AbstractHttpConfigurer::disable)
                    .formLogin(AbstractHttpConfigurer::disable)
                    .httpBasic(AbstractHttpConfigurer::disable)
                    .authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll());
            return http.build();
        }

        @Bean
        ArtifactQueryService artifactQueryService(ArtifactStore store) {
            return new ArtifactQueryService(store);
        }

        /**
         * The removal service over the same store the apply verb writes through. The four dependent
         * stores a removed pipeline's reclaim would touch are refusing stubs rather than silent no-ops:
         * this test deletes sources only, so reaching any of them means the reclaim ran for something
         * that owns no bookkeeping — a no-op stub would let that pass as green.
         */
        @Bean
        ArtifactMutationService artifactMutationService(ArtifactStore store, AuditGate auditGate) {
            return new ArtifactMutationService(
                    store, NoReclaimStores.desired(), NoReclaimStores.state(),
                    NoReclaimStores.observations(), NoReclaimStores.srsMeta(),
                    NoReclaimStores.derivedSchemas(), auditGate, DataBrowserFollows.NONE);
        }

        // The connection-test controller is imported, so its service must be present for the context to
        // stand up. Its behaviour is proven in ConnectionApiTest; here it only needs to construct, so the
        // probe and stores are inert.
        @Bean
        ConnectionTestService connectionTestService() {
            ConnectionProbe probe = config -> {
                throw new UnsupportedOperationException("connection.test is not exercised in this test");
            };
            ConnectionTestResultStore resultStore = new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.empty();
                }
            };
            AuditGate auditGate = new AuditGate(record -> {
            }, Clock.systemUTC());
            return new ConnectionTestService(probe, resultStore, auditGate);
        }

        // The read-back controller is imported too, so its query service must be present for the context to
        // stand up; its behaviour is proven in ConnectionApiTest, so here it only needs to construct (empty store).
        @Bean
        ConnectionTestResultQueryService connectionTestResultQueryService() {
            return new ConnectionTestResultQueryService(new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.empty();
                }
            });
        }

        // The discover-schema controller methods are bundled with the same controller, so their services
        // must be present for the context to stand up; their behaviour is proven in ConnectionApiTest, so
        // here they only need to construct (inert probe, empty store).
        @Bean
        SchemaDiscoveryService schemaDiscoveryService() {
            SchemaDiscoveryProbe probe = config -> {
                throw new UnsupportedOperationException("connection.discover-schema is not exercised in this test");
            };
            return new SchemaDiscoveryService(probe, new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.empty();
                }
            }, new AuditGate(record -> {
            }, Clock.systemUTC()), Clock.systemUTC());
        }

        @Bean
        SchemaQueryService schemaQueryService() {
            return new SchemaQueryService(new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.empty();
                }
            });
        }
    }

    /**
     * A test-only endpoint that throws a bare, uncoded programmer error. Like the liveness probe it is a
     * plain {@code @Controller}, so it stays at the root — outside the {@code /api} prefix and the
     * endpoint-derivation gate (which inspects only {@code /api} handlers). It proves the advice never
     * launders an uncoded throwable into a coded body: the boundary that request validation must not erode.
     */
    @Controller
    static class FaultController {

        @GetMapping("/boom")
        ResponseEntity<String> boom() {
            throw new IllegalStateException("a simulated programmer error");
        }
    }

    // ---- fixtures ----

    /** A source draft carrying a field outside the tapstate/v1 schema — rejected as dsl.unknown-field. */
    private static final String UNKNOWN_FIELD_DRAFT = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config: { host: 10.20.0.15 }
            mode: cdc
            tables: [ ORDERS ]
            options: { snapshot_mode: initial, include_ddl: true }
            """;

    private static final String TGT_MY = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.5, username: writer, password: My_2026 }
            """;

    // The same source under an id the stub advisory rule reports on, so one batch exercises the channel.
    private static final String WARNED_SRC = TGT_MY.replace("id: tgt_my", "id: warned_src");

    private static final String STUB_ADVISORY_CODE = "nest.resident-demand-over-budget";

    /**
     * A stand-in advisory rule: it reports one finding per artifact named {@code warned_*}. A real rule
     * judges capacity; what this one stands in for is the shape — a coded finding, with named params,
     * over a batch that validated.
     */
    private static List<ValidationDiagnostic> adviseOnWarnedArtifacts(
            List<Resource> resources, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<ValidationDiagnostic> findings = new ArrayList<>();
        for (Resource resource : resources) {
            if (resource.id().startsWith("warned")) {
                findings.add(new ValidationDiagnostic(STUB_ADVISORY_CODE, Map.of("id", resource.id())));
            }
        }
        return findings;
    }

    /** The same id with different content, so an edit of TGT_MY changes the stored bytes and its hash. */
    private static final String TGT_MY_CHANGED = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.5, username: writer, password: Changed_2026 }
            """;

    private static final String SRC_ORA = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config: { host: 10.20.0.15, port: 1521, service_name: ORCL,
                      username: cdc_user, password: Ora_2026 }
            mode: cdc
            tables: [ ORDERS, ORDER_ITEMS, CUSTOMERS ]
            options: { include_ddl: true }
            """;

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: ora2my_ods
            source: src_ora
            settings: { read_mode: snapshot_and_cdc }
            serve:
              from: /.*/
              sync:
                - id: my_ods
                  source: tgt_my
                  write_mode: upsert
                  ddl: apply
            """;

    /**
     * An in-memory {@link ArtifactStore} that mirrors the Mongo store's canonical round-trip — it holds
     * each artifact as its canonical text and reconstructs it on read through the parser — so a read
     * exercises the same write-then-parse the real store does. Clearable so each test seeds a clean store.
     */
    private static final class InMemoryArtifactStore implements ArtifactStore {

        private final CanonicalWriter writer = new CanonicalWriter();
        private final DslParser parser = new DslParser();
        private final Map<String, String> byId = new LinkedHashMap<>();
        private final Map<String, String> kindById = new LinkedHashMap<>();

        void putUnreadable(String id, String kind, String canonical) {
            byId.put(id, canonical);
            kindById.put(id, kind);
        }

        void clear() {
            byId.clear();
            kindById.clear();
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            saveAll(artifacts, Map.of());
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            // The declared versions are compared as part of the write, the way the real store compares
            // them inside its transaction, so the boundary tests see the same refusal a real one gives.
            for (Map.Entry<String, String> expected : expectedContentHashes.entrySet()) {
                String canonical = byId.get(expected.getKey());
                if (canonical == null || !CanonicalHash.of(canonical).equals(expected.getValue())) {
                    return Optional.of(expected.getKey());
                }
            }
            Map<String, String> staged = new LinkedHashMap<>();
            for (Resource artifact : artifacts) {
                staged.put(artifact.id(), writer.write(artifact));
            }
            byId.putAll(staged);
            for (Resource artifact : artifacts) {
                kindById.put(artifact.id(), artifact.kind());
            }
            return Optional.empty();
        }

        @Override
        public Optional<Resource> get(String id) {
            String canonical = byId.get(id);
            return canonical == null ? Optional.empty() : Optional.of(parser.parse(canonical));
        }

        @Override
        public List<Resource> list() {
            List<Resource> resources = new ArrayList<>();
            for (String canonical : byId.values()) {
                resources.add(parser.parse(canonical));
            }
            return resources;
        }

        @Override
        public List<StoredArtifactRecord> listStored() {
            List<StoredArtifactRecord> rows = new ArrayList<>();
            for (Map.Entry<String, String> entry : byId.entrySet()) {
                try {
                    rows.add(StoredArtifactRecord.of(parser.parse(entry.getValue())));
                } catch (RuntimeException unreadable) {
                    rows.add(new StoredArtifactRecord(
                            entry.getKey(), kindById.getOrDefault(entry.getKey(), "unknown"),
                            entry.getValue(), null, false));
                }
            }
            return rows;
        }

        @Override
        public ArtifactMutation delete(String id, String expectedContentHash) {
            String canonical = byId.get(id);
            if (canonical == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!CanonicalHash.of(canonical).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            byId.remove(id);
            kindById.remove(id);
            return ArtifactMutation.DELETED;
        }
    }
}
