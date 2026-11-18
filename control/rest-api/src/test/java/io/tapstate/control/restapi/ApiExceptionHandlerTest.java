package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.ClusterError;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.CloudAuthenticationObserver;
import io.tapstate.control.core.MonitorError;
import io.tapstate.control.core.ConnectorCatalogError;
import io.tapstate.control.core.DataBrowserError;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineError;
import io.tapstate.control.core.PositionError;
import io.tapstate.control.core.SourceError;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.Severity;
import io.tapstate.core.dsl.DslError;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The coded-error to HTTP mapping in isolation (no container): a {@link TapstateException} becomes a
 * structured {@code {code, params, message}} body at a status chosen by the code's domain — a client
 * input error ({@code dsl.*}) is a 400, and a coded server-side failure keeps the structured body but
 * answers 500 (distinct from an uncoded programmer bug, which the advice never catches at all).
 */
class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler(MessageCatalog.bundled());

    @Test
    void migratedIoRefusalsKeepTheirExplicitStatuses() {
        assertThat(ApiExceptionHandler.statusFor(new StubCode(io.tapstate.core.common.IoErrorDefinitions.STORE_QUOTA_EXCEEDED.code())))
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        for (var definition : List.of(io.tapstate.core.common.IoErrorDefinitions.CONFIGURATION_IMPORT_REFUSED,
                io.tapstate.core.common.IoErrorDefinitions.PIPELINE_STATE_REBUILD_REQUIRED,
                io.tapstate.core.common.IoErrorDefinitions.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED))
            assertThat(ApiExceptionHandler.statusFor(new StubCode(definition.code()))).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void migratedStoreRefusalsKeepTheirExplicitStatuses() {
        for (var definition : List.of(io.tapstate.core.common.StoreErrorDefinitions.INITIALIZATION_REFUSED,
                io.tapstate.core.common.StoreErrorDefinitions.AI_PROVIDER_SCHEMA_UNSUPPORTED))
            assertThat(ApiExceptionHandler.statusFor(new StubCode(definition.code()))).isEqualTo(HttpStatus.CONFLICT);
        for (var definition : List.of(io.tapstate.core.common.StoreErrorDefinitions.INVALID_URI,
                io.tapstate.core.common.StoreErrorDefinitions.STORAGE_CONNECTION_UNSUPPORTED,
                io.tapstate.core.common.StoreErrorDefinitions.TLS_TRUST_INVALID))
            assertThat(ApiExceptionHandler.statusFor(new StubCode(definition.code()))).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void publishedHttpStatusesFollowTheirDefinitionsAcrossIdentityChanges() throws Exception {
        Map<String, TapstateErrorCode> definitions = new HashMap<>();
        for (Class<? extends Enum<?>> owner : List.of(AlertError.class, ControlError.class,
                ArtifactError.class, ClusterError.class, ConnectorCatalogError.class, DataBrowserError.class,
                MonitorError.class, PipelineDraftError.class, PipelineError.class, PositionError.class,
                SourceError.class, LifecycleError.class)) {
            for (Enum<?> value : owner.getEnumConstants())
                definitions.put(owner.getSimpleName() + "." + value.name(), (TapstateErrorCode) value);
        }
        try (var input = getClass().getResourceAsStream("/http-error-status-baseline.tsv")) {
            assertThat(input).isNotNull();
            var rows = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
                    .lines().filter(line -> !line.startsWith("#") && !line.isBlank()).toList();
            assertThat(rows).hasSize(91);
            for (String row : rows) {
                String[] fields = row.split("\t");
                TapstateErrorCode code = fields[0].startsWith("literal:")
                        ? new StubCode(fields[0].substring("literal:".length())) : definitions.get(fields[0]);
                assertThat(code).as(fields[0]).isNotNull();
                assertThat(ApiExceptionHandler.statusFor(code)).as(fields[0])
                        .isEqualTo(HttpStatus.valueOf(fields[1]));
            }
        }
        for (DslError code : DslError.values())
            assertThat(ApiExceptionHandler.statusFor(code)).as(code.name()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void migratedSourceSchemaRefusalKeepsItsConflictStatus() {
        assertThat(ApiExceptionHandler.statusFor(
                new StubCode(io.tapstate.core.common.ActuationErrorDefinitions.SOURCE_SCHEMA_NOT_DISCOVERED.code())))
                .isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void unknownIdentitiesKeepServerFailureUnlessTheVerbAttributesTheRefusal() {
        var failure = new TapstateException(new StubCode("TS9999999"), Map.of("detail", "safe detail"), null);
        assertThat(handler.handle(failure).getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        var request = handler.handle(new BadRequestCodedException(failure));
        var unavailable = handler.handle(new UnavailableCodedException(failure));
        assertThat(request.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(unavailable.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        for (var response : List.of(request, unavailable)) {
            assertThat(response.getBody().code()).isEqualTo("TS9999999");
            assertThat(response.getBody().params()).containsEntry("detail", "safe detail");
            assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
        }
        assertThat(ApiExceptionHandler.statusFor(new StubCode("dsl.external-validation")))
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test void defaultSourceTimeoutAndBusyAreExplicitRetryDiagnostics() {
        assertThat(ApiExceptionHandler.statusFor(new StubCode("TS0060025"))).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
        assertThat(ApiExceptionHandler.statusFor(new StubCode("TS0060032"))).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        var busy = new TapstateException(new StubCode("TS0060032"), Map.of("connector", "mongodb", "timeout", "0s"), null);
        assertThat(handler.handle(new UnavailableCodedException(busy)).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test void dispatcherQuotaSanitizationReachesTheHttpResponseWithoutPrivateMetadata() {
        var scope = new io.tapstate.spi.store.StorageSetupScope("cluster", "installation", "setup", 1);
        var caller = io.tapstate.control.core.StorageControlAccess.ordinary(io.tapstate.spi.store.StorageBinding.Mode.OP, scope,
                new io.tapstate.spi.store.CacheAuthScope("cluster", 1, io.tapstate.spi.store.StorageBinding.Mode.OP, "incarnation", 1),
                io.tapstate.control.core.TapstatePrincipal.humanJwt(new io.tapstate.control.core.VerifiedToken("operator", io.tapstate.control.core.Scope.ADMIN)), null, () -> {});
        var dispatcher = io.tapstate.control.core.StorageControlDispatcher.staged(io.tapstate.control.core.ControlOperations.registry(),
                () -> null, () -> null, () -> null, (selected, active) -> {
                    throw new TapstateException(IoError.STORE_QUOTA_EXCEEDED, Map.of("detail", "private-value-sentinel"), new IllegalArgumentException("private-cause"));
                });
        var failure = org.assertj.core.api.Assertions.catchThrowable(() -> dispatcher.dispatch("storage.status", caller, Map.of()));
        assertThat(failure).isInstanceOf(TapstateException.class);
        var response = handler.handle((TapstateException) failure);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().code()).isEqualTo(ControlError.STORAGE_QUOTA_EXCEEDED.code());
        assertThat(response.getBody().params()).isEmpty();
        assertThat(response.getBody().toString()).doesNotContain("private-value-sentinel", "private-cause", "operator", "installation");
    }

    @Test void quotaResponsesAreCoarse503DiagnosticsWithoutDriverParametersOrCauses() throws Exception {
        for (var code : List.of(IoError.STORE_QUOTA_EXCEEDED, ControlError.STORAGE_QUOTA_EXCEEDED)) {
            var failure = new TapstateException(code, Map.of("detail", "mongodb://u:secret@host/private"),
                    new IllegalArgumentException("private-cause"));
            var response = handler.handle(failure);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(response.getBody().code()).isEqualTo(ControlError.STORAGE_QUOTA_EXCEEDED.code());
            assertThat(response.getBody().params()).isEmpty();
            assertThat(response.getBody().message()).doesNotContain("secret", "private-cause", "host");
        }
    }

    @Test void aKnownPipelineRequiringAnExplicitStateRebuildIsAStateConflict() {
        var response=handler.handle(new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline","flow"),null));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo(IoError.PIPELINE_STATE_REBUILD_REQUIRED.code());
        assertThat(response.getBody().params()).containsEntry("pipeline","flow");
    }

    @Test
    void addedTablesExplainTheRequiredClearStateFullLoadAsAStateConflict() {
        var response = handler.handle(new TapstateException(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED,
                Map.of("pipeline", "flow", "tables", "input.quiet"), null));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED.code());
        assertThat(response.getBody().params()).containsEntry("pipeline", "flow").containsEntry("tables", "input.quiet");
        assertThat(response.getBody().message()).contains("input.quiet", "Clear all state", "full load");
    }

    @Test
    void aStartMissingItsRequiredSourceModelIsAStateConflict() {
        assertThat(ApiExceptionHandler.statusFor(new StubCode("actuation.source-schema-not-discovered")))
                .isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void mvcAndSecurityExposeOnlyTheExistingErrorWhileCloudDiagnosticsKeepItsCode() throws Exception {
        RequestAttributes previous = RequestContextHolder.getRequestAttributes();
        try {
            MockHttpServletRequest request = new MockHttpServletRequest();
            request.setAttribute(CloudAuthenticationObserver.REQUEST_ID_CONTEXT_KEY, "controlled-request");
            RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
            ResponseEntity<ApiError> mvc = handler.handle(
                    new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null));
            assertThat(mvc.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
            assertThat(mvc.getBody().code()).isEqualTo(ControlError.UNAUTHENTICATED.code());
            assertThat(request.getAttribute(CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY))
                    .isEqualTo(ControlError.UNAUTHENTICATED.code());

            request.removeAttribute(CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY);
            MockHttpServletResponse response = new MockHttpServletResponse();
            new ApiSecurityErrorWriter(MessageCatalog.bundled(), new ObjectMapper()).unauthenticated(request, response);
            assertThat(response.getStatus()).isEqualTo(401);
            assertThat(response.getContentAsString()).contains(ControlError.UNAUTHENTICATED.code());
            assertThat(request.getAttribute(CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY))
                    .isEqualTo(ControlError.UNAUTHENTICATED.code());

            request.removeAttribute(CloudAuthenticationObserver.REQUEST_ID_CONTEXT_KEY);
            request.removeAttribute(CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY);
            handler.handle(new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null));
            assertThat(request.getAttribute(CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY)).isNull();
        } finally {
            if (previous == null) RequestContextHolder.resetRequestAttributes();
            else RequestContextHolder.setRequestAttributes(previous);
        }
    }

    @Test
    void aClientInputErrorIsABadRequestWithACodedRenderedBody() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("field", "snapshot_mode");
        args.put("path", "options.snapshot_mode");
        TapstateException e = new TapstateException(DslError.UNKNOWN_FIELD, args, null);

        ResponseEntity<ApiError> response = handler.handle(e);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        ApiError body = response.getBody();
        assertThat(body.code()).isEqualTo("TS0010002");
        // the message is rendered from the catalog with the named args substituted, not the bare code
        assertThat(body.message()).isEqualTo("Unknown field 'snapshot_mode' at options.snapshot_mode.");
        assertThat(body.params()).containsEntry("field", "snapshot_mode").containsEntry("path", "options.snapshot_mode");
    }

    @Test
    void aMalformedRequestIsABadRequestWithACodedRenderedBody() {
        // A request refused at the boundary as structurally malformed is a client input error like dsl.*: a 400
        // with a coded body whose reason is substituted into the catalog template, not left as the bare code.
        TapstateException e = new TapstateException(
                ControlError.MALFORMED_REQUEST, Map.of("reason", "a `username` is required"), null);

        ResponseEntity<ApiError> response = handler.handle(e);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        ApiError body = response.getBody();
        assertThat(body.code()).isEqualTo("TS0210001");
        assertThat(body.message()).contains("a `username` is required").isNotEqualTo("TS0210001");
        assertThat(body.params()).containsEntry("reason", "a `username` is required");
    }

    @Test
    void aCodedServerSideFailureKeepsTheCodedBodyButAnswersServerError() {
        TapstateException e = new TapstateException(ControlError.AUDIT_BLOCKED, Map.of("op", "artifact.apply"), null);

        ResponseEntity<ApiError> response = handler.handle(e);

        // a coded error the surface has no client-attributable mapping for is a server-side failure (500),
        // yet still carries the structured coded body — it is not a bare, uncoded crash
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody().code()).isEqualTo("TS0210015");
        assertThat(response.getBody().message()).isNotBlank().isNotEqualTo("TS0210015");
    }

    @Test
    void anOverloadedPreviewAnswersServiceUnavailable() {
        ResponseEntity<ApiError> response = handler.handle(
                new TapstateException(ControlError.PREVIEW_OVERLOADED, Map.of(), null));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().code()).isEqualTo("TS0210014");
    }

    @Test
    void aMissingOrRejectedCredentialIsUnauthorized() {
        // Both the interceptor's "no valid credential" and the login flow's own rejection map to 401.
        assertThat(handler.handle(new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null)).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(handler.handle(new TapstateException(ControlError.AUTH_FAILED, Map.of(), null)).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void anUnderScopedOrNonLoopbackCallerIsForbidden() {
        assertThat(handler.handle(new TapstateException(
                ControlError.FORBIDDEN, Map.of("op", "artifact.apply", "required", "write"), null)).getStatusCode())
                .isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(handler.handle(new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null)).getStatusCode())
                .isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void aReadOfAPipelineWithNoObservationIsNotFound() {
        // The read faces serve a frontend with no stderr/exit channel, so a missing observation is a coded
        // 404 (the observation resource does not exist), like an artifact get or a lifecycle verb on an
        // unknown pipeline — never a bare 500 that hides the reason.
        ResponseEntity<ApiError> response =
                handler.handle(new TapstateException(MonitorError.NO_OBSERVATION, Map.of("pipeline", "pl1"), null));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().code()).isEqualTo("TS0400001");
        assertThat(response.getBody().message()).isNotBlank().isNotEqualTo("TS0400001");
    }

    @Test
    void anAlreadyClosedBootstrapChannelIsAConflict() {
        ResponseEntity<ApiError> response =
                handler.handle(new TapstateException(ControlError.BOOTSTRAP_CLOSED, Map.of(), null));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("TS0210018");
    }

    /**
     * The removal verb's five refusals, each a client-attributable status rather than the 500 an
     * unmapped domain falls through to. {@code statusFor} is a pure code-to-status switch with no
     * knowledge of which endpoint raised the code, so a code that both delete and apply can raise
     * necessarily answers the same status on both — these five are fixed here once for that reason.
     */
    @Test
    void everyArtifactRefusalIsAClientAttributableStatusRatherThanAServerError() {
        assertThat(ApiExceptionHandler.statusFor(ArtifactError.NOT_FOUND))
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(ApiExceptionHandler.statusFor(ArtifactError.PRECONDITION_REQUIRED))
                .isEqualTo(HttpStatus.PRECONDITION_REQUIRED);
        assertThat(ApiExceptionHandler.statusFor(ArtifactError.VERSION_CONFLICT))
                .isEqualTo(HttpStatus.PRECONDITION_FAILED);
        assertThat(ApiExceptionHandler.statusFor(ArtifactError.IN_USE))
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(ApiExceptionHandler.statusFor(ArtifactError.PIPELINE_NOT_STOPPED))
                .isEqualTo(HttpStatus.CONFLICT);
    }

    /**
     * The one code in this domain that is deliberately not client-attributable. It does not mean the
     * caller asked for something impermissible — the request was valid and was carried out, and the
     * server's own follow-up work is what failed. Answering it as a 4xx would tell the caller to fix
     * their request and try again, which is the one thing that cannot work: the artifact is gone, so
     * a retry can only ever answer {@code artifact.not-found}.
     */
    @Test
    void aPartlyExecutedRemovalIsAServerErrorRatherThanOneMoreClientRefusal() {
        assertThat(ApiExceptionHandler.statusFor(ArtifactError.RECLAIM_INCOMPLETE))
                .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * The refusal body must carry the parameters the caller acts on, not just a status. A caller told
     * only "409" cannot tell "something references this" from "the pipeline is running", and the two
     * next steps are different — read the referrers, or stop the pipeline.
     */
    @Test
    void aRefusalCarriesTheParametersTheCallerNeedsToActOnIt() {
        ResponseEntity<ApiError> inUse = handler.handle(new TapstateException(
                ArtifactError.IN_USE, Map.of("id", "src1", "referrers", List.of("pl1", "pl2")), null));
        assertThat(inUse.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(inUse.getBody().code()).isEqualTo("TS0240004");
        assertThat(inUse.getBody().params()).containsEntry("referrers", List.of("pl1", "pl2"));
        assertThat(inUse.getBody().message()).isNotBlank().isNotEqualTo("TS0240004");

        ResponseEntity<ApiError> running = handler.handle(new TapstateException(
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "pl1", "actual", "RUNNING", "desired", "RUNNING"), null));
        assertThat(running.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(running.getBody().params())
                .containsEntry("actual", "RUNNING")
                .containsEntry("desired", "RUNNING");
        assertThat(running.getBody().message()).isNotBlank().isNotEqualTo("TS0240005");
    }

    @Test
    void aConnectorDomainCodeDefaultsToServerSideRegardlessOfWhereItWasRaised() {
        // No connector code is a client error by its code alone: the same connector code raised on the
        // resolve path (connection test / discovery) is a server-side failure — e.g. a registered connector
        // that fails to link at test time is connector.load-failed but not the caller's fault. So statusFor
        // keeps every connector.* at 500; the register verb, which knows the uploaded artifact is the
        // client's, opts specific failures into a 400 via BadRequestCodedException instead of the code table.
        for (String code : List.of(
                "TS0060009",
                "TS0060014",
                "TS0060015",
                "TS0060016",
                "TS0060017",
                "TS0060006",
                "TS0060004",
                "TS0060005",
                // Attributed to the caller by the read face, which knows the source was theirs to name;
                // here it must stay server-side, or the same code on a resolve path would be miscalled.
                "TS0060031")) {
            assertThat(ApiExceptionHandler.statusFor(new StubCode(code))).as(code)
                    .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Test
    void aReadRefusedOnTheRequestAsWrittenIsABadRequest() {
        // Judgements this face makes before anything is sent, on the request itself. Left to the default
        // they answer 500, which tells a caller the server broke rather than that their request was
        // refused - the one thing a refusal has to be distinguishable from.
        for (String code : List.of(
                "TS0390002",
                "TS0390004",
                "TS0390003")) {
            assertThat(ApiExceptionHandler.statusFor(new StubCode(code))).as(code)
                    .isEqualTo(HttpStatus.BAD_REQUEST);
        }
    }

    @Test
    void historyCursorAndBudgetRefusalsHaveTheirPublishedStatuses() {
        assertThat(ApiExceptionHandler.statusFor(MonitorError.INVALID_CURSOR))
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(ApiExceptionHandler.statusFor(MonitorError.CURSOR_EXPIRED))
                .isEqualTo(HttpStatus.GONE);
        assertThat(ApiExceptionHandler.statusFor(MonitorError.QUERY_BUDGET_EXCEEDED))
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void aBoundaryAttributedCodedErrorIsABadRequestPreservingTheCode() {
        // A verb boundary that knows a domain code is the client's fault in its context wraps it as a
        // BadRequestCodedException: a 400 that still renders the underlying coded body (here a connector
        // artifact refused at register), even though the code defaults to 500 globally.
        TapstateException connectorFailure = new TapstateException(new StubCode("TS0060017"),
                Map.of("artifact", "bad.jar", "spec", "spec.json", "detail", "the spec is not valid JSON"), null);

        ResponseEntity<ApiError> response = handler.handle(new BadRequestCodedException(connectorFailure));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().code()).isEqualTo("TS0060017");
        assertThat(response.getBody().message()).isNotBlank().isNotEqualTo("TS0060017");
        assertThat(response.getBody().params()).containsEntry("artifact", "bad.jar");
    }

    @Test
    void theCanonicalCodeCrossesTheBoundaryAsAStringNotAnEnum() {
        TapstateException e = new TapstateException(DslError.MALFORMED_YAML, Map.of("detail", "bad token"), null);

        ApiError body = handler.handle(e).getBody();

        // the wire identity is the canonical code string (ApiError.code is a String); the enum never crosses
        assertThat(body.code()).isEqualTo("TS0010017");
    }

    @Test
    void aMemberThatCannotReadTheClusterIsUnavailableRatherThanBroken() {
        // The three answers this has to be told apart from are a 500 (the server broke), a 200 with an
        // empty member list (the cluster is empty), and a 200 listing this member alone (it is in a
        // cluster of one). All three are readings nobody took; 503 with a code is the member saying so.
        TapstateException e =
                new TapstateException(ClusterError.MEMBERSHIP_UNREADABLE, Map.of(), null);

        ResponseEntity<ApiError> response = handler.handle(e);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        ApiError body = response.getBody();
        assertThat(body.code()).isEqualTo("TS0370001");
        assertThat(body.message())
                .describedAs("rendered from the catalog, not left as the bare code -- a person reading "
                        + "a failing run's output is the first caller this reaches")
                .isNotEqualTo("TS0370001")
                .contains("cannot read who is in the cluster");
    }

    /** A coded error whose only relevant facet is its canonical code string — enough to exercise statusFor. */
    private record StubCode(String code) implements TapstateErrorCode {
        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of();
        }
    }
}
