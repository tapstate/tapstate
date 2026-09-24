package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.ClusterError;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.Severity;
import io.tapstate.core.dsl.DslError;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    void aClientInputErrorIsABadRequestWithACodedRenderedBody() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("field", "snapshot_mode");
        args.put("path", "options.snapshot_mode");
        TapstateException e = new TapstateException(DslError.UNKNOWN_FIELD, args, null);

        ResponseEntity<ApiError> response = handler.handle(e);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        ApiError body = response.getBody();
        assertThat(body.code()).isEqualTo("dsl.unknown-field");
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
        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.message()).contains("a `username` is required").isNotEqualTo("control.malformed-request");
        assertThat(body.params()).containsEntry("reason", "a `username` is required");
    }

    @Test
    void aCodedServerSideFailureKeepsTheCodedBodyButAnswersServerError() {
        TapstateException e = new TapstateException(ControlError.AUDIT_BLOCKED, Map.of("op", "artifact.apply"), null);

        ResponseEntity<ApiError> response = handler.handle(e);

        // a coded error the surface has no client-attributable mapping for is a server-side failure (500),
        // yet still carries the structured coded body — it is not a bare, uncoded crash
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody().code()).isEqualTo("control.audit-blocked");
        assertThat(response.getBody().message()).isNotBlank().isNotEqualTo("control.audit-blocked");
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
        assertThat(response.getBody().code()).isEqualTo("monitor.no-observation");
        assertThat(response.getBody().message()).isNotBlank().isNotEqualTo("monitor.no-observation");
    }

    @Test
    void anAlreadyClosedBootstrapChannelIsAConflict() {
        ResponseEntity<ApiError> response =
                handler.handle(new TapstateException(ControlError.BOOTSTRAP_CLOSED, Map.of(), null));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().code()).isEqualTo("control.bootstrap-closed");
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
        assertThat(inUse.getBody().code()).isEqualTo("artifact.in-use");
        assertThat(inUse.getBody().params()).containsEntry("referrers", List.of("pl1", "pl2"));
        assertThat(inUse.getBody().message()).isNotBlank().isNotEqualTo("artifact.in-use");

        ResponseEntity<ApiError> running = handler.handle(new TapstateException(
                ArtifactError.PIPELINE_NOT_STOPPED,
                Map.of("id", "pl1", "actual", "RUNNING", "desired", "RUNNING"), null));
        assertThat(running.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(running.getBody().params())
                .containsEntry("actual", "RUNNING")
                .containsEntry("desired", "RUNNING");
        assertThat(running.getBody().message()).isNotBlank().isNotEqualTo("artifact.pipeline-not-stopped");
    }

    @Test
    void aConnectorDomainCodeDefaultsToServerSideRegardlessOfWhereItWasRaised() {
        // No connector code is a client error by its code alone: the same connector code raised on the
        // resolve path (connection test / discovery) is a server-side failure — e.g. a registered connector
        // that fails to link at test time is connector.load-failed but not the caller's fault. So statusFor
        // keeps every connector.* at 500; the register verb, which knows the uploaded artifact is the
        // client's, opts specific failures into a 400 via BadRequestCodedException instead of the code table.
        for (String code : List.of(
                "connector.load-failed",
                "connector.no-connector-class",
                "connector.ambiguous-connector-class",
                "connector.spec-not-found",
                "connector.spec-invalid",
                "connector.registration-conflict",
                "connector.not-registered",
                "connector.ambiguous-registration",
                // Attributed to the caller by the read face, which knows the source was theirs to name;
                // here it must stay server-side, or the same code on a resolve path would be miscalled.
                "connector.capability-missing")) {
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
                "data-browser.invalid-limit",
                "data-browser.connector-not-browsable",
                "data-browser.unorderable-field")) {
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
        TapstateException connectorFailure = new TapstateException(new StubCode("connector.spec-invalid"),
                Map.of("artifact", "bad.jar", "spec", "spec.json", "detail", "the spec is not valid JSON"), null);

        ResponseEntity<ApiError> response = handler.handle(new BadRequestCodedException(connectorFailure));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().code()).isEqualTo("connector.spec-invalid");
        assertThat(response.getBody().message()).isNotBlank().isNotEqualTo("connector.spec-invalid");
        assertThat(response.getBody().params()).containsEntry("artifact", "bad.jar");
    }

    @Test
    void theCanonicalCodeCrossesTheBoundaryAsAStringNotAnEnum() {
        TapstateException e = new TapstateException(DslError.MALFORMED_YAML, Map.of("detail", "bad token"), null);

        ApiError body = handler.handle(e).getBody();

        // the wire identity is the canonical code string (ApiError.code is a String); the enum never crosses
        assertThat(body.code()).isEqualTo("dsl.malformed-yaml");
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
        assertThat(body.code()).isEqualTo("cluster.membership-unreadable");
        assertThat(body.message())
                .describedAs("rendered from the catalog, not left as the bare code -- a person reading "
                        + "a failing run's output is the first caller this reaches")
                .isNotEqualTo("cluster.membership-unreadable")
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
