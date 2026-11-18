package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.ClusterError;
import io.tapstate.control.core.ConnectorCatalogError;
import io.tapstate.control.core.DataBrowserError;
import io.tapstate.control.core.MonitorError;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineError;
import io.tapstate.control.core.PositionError;
import io.tapstate.control.core.SourceError;
import io.tapstate.control.core.StorageQuota;
import io.tapstate.control.core.StartRefusal;
import io.tapstate.control.core.CloudAuthenticationObserver;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.messages.MessageCatalog;
import org.springframework.http.HttpStatus;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Projects a coded first-party error onto a structured HTTP response. A {@link TapstateException} — a
 * user-facing, diagnosable failure — becomes a {@code {code, params, message}} body: the canonical code
 * string, the named arguments, and the message rendered from them through the shared catalog. The status
 * is chosen from the code: a client input error (a {@code dsl.*} validation failure or a
 * {@link ControlError#MALFORMED_REQUEST} refused at the boundary) is a 400; the authentication codes map to
 * 401 / 403 / 409; a lifecycle verb on an unknown pipeline is a 404 and a forbidden transition or stale
 * revision is a 409; a metrics, snapshot, or explanation read that requires an observation is a 404 when
 * none has been published; a member that cannot read the cluster right now is a 503, because it is neither the
 * caller's fault nor a failure but a window the member passes through; any other coded error keeps the structured
 * body but answers 500, since the surface has no client-attributable mapping for it yet. That mapping is the seam
 * later slices extend as more client-attributable codes land.
 *
 * <p>Only {@link TapstateException} is handled here. A programmer error / invariant violation (a bare NPE or
 * {@code IllegalStateException}) is left to crash into the container's default 500 — it must never be
 * laundered into a pretty coded body that hides the defect.
 */
@RestControllerAdvice
class ApiExceptionHandler {

    private final MessageCatalog catalog;

    ApiExceptionHandler(MessageCatalog catalog) {
        this.catalog = catalog;
    }

    @ExceptionHandler(TapstateException.class)
    ResponseEntity<ApiError> handle(TapstateException e) {
        if (StorageQuota.isQuotaCode(e.code()))
            e = new TapstateException(ControlError.STORAGE_QUOTA_EXCEEDED, Map.of(), null);
        markCloudError(e.code());
        MessageCatalog.Rendered rendered = catalog.render(e.code(), e.args());
        // Sorted so the params render identically regardless of throw-site order (a stable machine contract).
        var diagnostic = e instanceof io.tapstate.control.core.ConnectionTestRefusal refusal ? refusal.diagnostic() : Map.<String, Object>of();
        ApiError body = new ApiError(e.code().code(), new TreeMap<>(e.args()), rendered.message(), diagnostic);
        HttpStatus status=statusFor(e.code());
        if(e.code()==io.tapstate.core.validation.ValidationError.VALUE_LIMIT_EXCEEDED
                &&java.util.Set.of("validation.metadataFrames","validation.processEnvelope","validation.cooperativeHeadroom").contains(e.args().get("limit")))status=HttpStatus.TOO_MANY_REQUESTS;
        return ResponseEntity.status(status).cacheControl(CacheControl.noStore()).body(body);
    }

    @ExceptionHandler(io.tapstate.control.core.ValidationConfigurationService.StaleDefinition.class)
    ResponseEntity<ApiError> handleValidationConflict(io.tapstate.control.core.ValidationConfigurationService.StaleDefinition failure){
        MessageCatalog.Rendered rendered=catalog.render(failure.code(),failure.args());
        return ResponseEntity.status(HttpStatus.CONFLICT).cacheControl(CacheControl.noStore())
                .body(new ApiError(failure.code().code(),new TreeMap<>(failure.args()),rendered.message()));
    }

    @ExceptionHandler(UnavailableCodedException.class)
    ResponseEntity<ApiError> handle(UnavailableCodedException e) {
        var rendered = handle((TapstateException)e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).cacheControl(CacheControl.noStore()).body(rendered.getBody());
    }

    /**
     * A start that did not go ahead answers with the coded error a client already knows how to read, and
     * beside it what the start checks said and what the start had already changed in the definition.
     */
    @ExceptionHandler(StartRefusal.class)
    ResponseEntity<StartRefusalError> handle(StartRefusal e) {
        MessageCatalog.Rendered rendered = catalog.render(e.code(), e.args());
        StartRefusalError body = new StartRefusalError(e.code().code(), new TreeMap<>(e.args()),
                rendered.message(), e.report(), e.decisionsApplied().isEmpty() ? null : e.decisionsApplied());
        return ResponseEntity.status(statusFor(e.code())).cacheControl(CacheControl.noStore()).body(body);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ApiError> handleMalformed(HttpMessageNotReadableException ignored) {
        return handle(new TapstateException(
                ControlError.MALFORMED_REQUEST,
                Map.of("reason", "the JSON body does not match the request shape"),
                null));
    }

    /**
     * A coded error a verb boundary has attributed to the client's request answers 400 while rendering the
     * same coded body. This is how a domain code that is a client error in one verb's context but a
     * server-side failure in another gets the right status: the verb that has the context wraps it here,
     * rather than {@link #statusFor} guessing globally from the code alone.
     */
    @ExceptionHandler(BadRequestCodedException.class)
    ResponseEntity<ApiError> handle(BadRequestCodedException e) {
        markCloudError(e.code());
        MessageCatalog.Rendered rendered = catalog.render(e.code(), e.args());
        ApiError body = new ApiError(e.code().code(), new TreeMap<>(e.args()), rendered.message());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).cacheControl(CacheControl.noStore()).body(body);
    }

    private static void markCloudError(TapstateErrorCode code) {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            markCloudError(attributes.getRequest(), code);
        }
    }

    static void markCloudError(HttpServletRequest request, TapstateErrorCode code) {
        if (request != null && request.getAttribute(CloudAuthenticationObserver.REQUEST_ID_CONTEXT_KEY) != null) {
            request.setAttribute(CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY, code.code());
        }
    }

    /**
     * The HTTP status for a coded error. The authentication codes are client-attributable and map to the
     * usual auth statuses: no / invalid credential and a rejected login are 401, an under-scoped or
     * non-loopback caller is 403, and a bootstrap channel that has already closed is a 409 state conflict.
     * A client input error — a {@code dsl.*} validation failure or a {@link ControlError#MALFORMED_REQUEST} refused
     * at the boundary — is a 400. A lifecycle verb on a pipeline that was never applied is a 404; a transition
     * the state machine forbids, or a start/resume at a stale revision, is a 409 state conflict. A metrics,
     * snapshot, or explanation read that requires a published observation is likewise a 404 when none exists.
     * Catalog status reads of a known pipeline still return its catalog state before observation. Any other coded
     * error is a server-side failure (500) that still carries the structured body — never a bare, uncoded crash.
     */
    static HttpStatus statusFor(TapstateErrorCode code) {
        return HTTP_STATUSES.getOrDefault(code.code(),
                domainOf(code.code()).equals("dsl") ? HttpStatus.BAD_REQUEST : HttpStatus.INTERNAL_SERVER_ERROR);
    }

    // Status depends on the error contract, including when a code is shared by several endpoints.
    // A successful artifact removal whose cleanup fails remains a server error, not a retryable refusal.
    private static final Map<String, HttpStatus> HTTP_STATUSES = httpStatuses();

    private static Map<String, HttpStatus> httpStatuses() {
        Map<String, HttpStatus> statuses = new HashMap<>(Map.ofEntries(
            Map.entry(AlertError.INVALID_CONFIGURATION.code(), HttpStatus.BAD_REQUEST),
            Map.entry(AlertError.STALE_ACTION.code(), HttpStatus.CONFLICT),
            Map.entry(AlertError.COMMAND_CONFLICT.code(), HttpStatus.CONFLICT),
            Map.entry(AlertError.COMMAND_EXPIRED.code(), HttpStatus.CONFLICT),
            Map.entry(AlertError.COMMAND_NOT_YET_VALID.code(), HttpStatus.CONFLICT),
            Map.entry(AlertError.CHANNEL_UNAVAILABLE.code(), HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("actuation.source-schema-not-discovered", HttpStatus.CONFLICT),
            Map.entry(io.tapstate.core.common.ActuationErrorDefinitions.SOURCE_SCHEMA_NOT_DISCOVERED.code(), HttpStatus.CONFLICT),
            Map.entry(ControlError.STORAGE_CACHE_DATABASE_OCCUPIED.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.STORAGE_DATABASES_OVERLAP.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.STORAGE_SCOPE_CONFLICT.code(), HttpStatus.CONFLICT),
            Map.entry(ControlError.STORAGE_TEST_FAILED.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.STORAGE_TEST_REQUIRED.code(), HttpStatus.CONFLICT),
            Map.entry(ControlError.STORAGE_UNAVAILABLE.code(), HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry(ControlError.STORAGE_QUOTA_EXCEEDED.code(), HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("io.store-quota-exceeded", HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("dsl", HttpStatus.BAD_REQUEST),
            Map.entry("io.configuration-import-refused", HttpStatus.CONFLICT),
            Map.entry("io.pipeline-state-rebuild-required", HttpStatus.CONFLICT),
            Map.entry("io.pipeline-table-selection-rebuild-required", HttpStatus.CONFLICT),
            Map.entry("store.initialization-refused", HttpStatus.CONFLICT),
            Map.entry(io.tapstate.core.common.StoreErrorDefinitions.INITIALIZATION_REFUSED.code(), HttpStatus.CONFLICT),
            Map.entry("store.ai-provider-schema-unsupported", HttpStatus.CONFLICT),
            Map.entry(io.tapstate.core.common.StoreErrorDefinitions.AI_PROVIDER_SCHEMA_UNSUPPORTED.code(), HttpStatus.CONFLICT),
            Map.entry("store.invalid-uri", HttpStatus.BAD_REQUEST),
            Map.entry(io.tapstate.core.common.StoreErrorDefinitions.INVALID_URI.code(), HttpStatus.BAD_REQUEST),
            Map.entry("store.storage-connection-unsupported", HttpStatus.BAD_REQUEST),
            Map.entry(io.tapstate.core.common.StoreErrorDefinitions.STORAGE_CONNECTION_UNSUPPORTED.code(), HttpStatus.BAD_REQUEST),
            Map.entry("store.tls-trust-invalid", HttpStatus.BAD_REQUEST),
            Map.entry(io.tapstate.core.common.StoreErrorDefinitions.TLS_TRUST_INVALID.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.AI_NOT_CONFIGURED.code(), HttpStatus.CONFLICT),
            Map.entry(ControlError.AI_PROVIDER_AUTH_FAILED.code(), HttpStatus.UNPROCESSABLE_CONTENT),
            Map.entry(ControlError.AI_PROVIDER_REQUEST_REJECTED.code(), HttpStatus.BAD_GATEWAY),
            Map.entry(ControlError.AI_PROVIDER_RATE_LIMITED.code(), HttpStatus.TOO_MANY_REQUESTS),
            Map.entry(ControlError.AI_PROVIDER_FAILED.code(), HttpStatus.BAD_GATEWAY),
            Map.entry(ControlError.AI_PROVIDER_OUTPUT_LIMIT.code(), HttpStatus.BAD_GATEWAY),
            Map.entry(ControlError.AI_INVALID_PROPOSAL.code(), HttpStatus.UNPROCESSABLE_CONTENT),
            Map.entry(ControlError.AI_OVERLOADED.code(), HttpStatus.TOO_MANY_REQUESTS),
            Map.entry(ControlError.AUTH_FAILED.code(), HttpStatus.UNAUTHORIZED),
            Map.entry(ControlError.UNAUTHENTICATED.code(), HttpStatus.UNAUTHORIZED),
            Map.entry(ControlError.CLOUD_AUTH_UNAVAILABLE.code(), HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry(ControlError.AUTH_MODE_UNAVAILABLE.code(), HttpStatus.FORBIDDEN),
            Map.entry(ControlError.FORBIDDEN.code(), HttpStatus.FORBIDDEN),
            Map.entry(ControlError.BOOTSTRAP_FORBIDDEN.code(), HttpStatus.FORBIDDEN),
            Map.entry(ControlError.BOOTSTRAP_CLOSED.code(), HttpStatus.CONFLICT),
            Map.entry(SourceError.ID_MISMATCH.code(), HttpStatus.BAD_REQUEST),
            Map.entry(SourceError.NOT_FOUND.code(), HttpStatus.NOT_FOUND),
            Map.entry(SourceError.ALREADY_EXISTS.code(), HttpStatus.CONFLICT),
            Map.entry(SourceError.IN_USE.code(), HttpStatus.CONFLICT),
            Map.entry(SourceError.SRS_CHANGE_WHILE_RUNNING.code(), HttpStatus.CONFLICT),
            Map.entry(SourceError.VERSION_CONFLICT.code(), HttpStatus.PRECONDITION_FAILED),
            Map.entry(SourceError.PRECONDITION_REQUIRED.code(), HttpStatus.PRECONDITION_REQUIRED),
            Map.entry(ArtifactError.NOT_FOUND.code(), HttpStatus.NOT_FOUND),
            Map.entry(ArtifactError.PRECONDITION_REQUIRED.code(), HttpStatus.PRECONDITION_REQUIRED),
            Map.entry(ArtifactError.VERSION_CONFLICT.code(), HttpStatus.PRECONDITION_FAILED),
            Map.entry(ArtifactError.IN_USE.code(), HttpStatus.CONFLICT),
            Map.entry(ArtifactError.PIPELINE_NOT_STOPPED.code(), HttpStatus.CONFLICT),
            Map.entry(ArtifactError.RECLAIM_INCOMPLETE.code(), HttpStatus.INTERNAL_SERVER_ERROR),
            Map.entry(ConnectorCatalogError.NOT_FOUND.code(), HttpStatus.NOT_FOUND),
            Map.entry("connector.test-timeout", HttpStatus.GATEWAY_TIMEOUT),
            Map.entry(PipelineError.NOT_FOUND.code(), HttpStatus.NOT_FOUND),
            Map.entry(PipelineError.ALREADY_EXISTS.code(), HttpStatus.CONFLICT),
            Map.entry(PipelineError.ID_MISMATCH.code(), HttpStatus.BAD_REQUEST),
            Map.entry(PipelineError.PRECONDITION_REQUIRED.code(), HttpStatus.PRECONDITION_REQUIRED),
            Map.entry(PipelineError.VERSION_CONFLICT.code(), HttpStatus.PRECONDITION_FAILED),
            Map.entry(PipelineError.DEFINITION_CHANGE_UNAVAILABLE.code(), HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry(PipelineDraftError.NOT_FOUND.code(), HttpStatus.NOT_FOUND),
            Map.entry(PipelineDraftError.ALREADY_EXISTS.code(), HttpStatus.CONFLICT),
            Map.entry(PipelineDraftError.MODE_CONFLICT.code(), HttpStatus.CONFLICT),
            Map.entry(PipelineDraftError.ARTIFACT_CONFLICT.code(), HttpStatus.CONFLICT),
            Map.entry(PipelineDraftError.REVISION_CONFLICT.code(), HttpStatus.PRECONDITION_FAILED),
            Map.entry(PipelineDraftError.PRECONDITION_REQUIRED.code(), HttpStatus.PRECONDITION_REQUIRED),
            Map.entry(PipelineDraftError.INVALID.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.MALFORMED_REQUEST.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ControlError.PREVIEW_OVERLOADED.code(), HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry(LifecycleError.UNKNOWN_PIPELINE.code(), HttpStatus.NOT_FOUND),
            Map.entry(LifecycleError.ILLEGAL_TRANSITION.code(), HttpStatus.CONFLICT),
            Map.entry(LifecycleError.INCOMPATIBLE_REVISION.code(), HttpStatus.CONFLICT),
            Map.entry(LifecycleError.START_NEEDS_CONFIRMATION.code(), HttpStatus.CONFLICT),
            Map.entry(LifecycleError.START_BLOCKED.code(), HttpStatus.CONFLICT),
            Map.entry(LifecycleError.INVALID_START_DECISION.code(), HttpStatus.BAD_REQUEST),
            Map.entry(LifecycleError.PIPELINE_NOT_RUNNABLE.code(), HttpStatus.CONFLICT),
            Map.entry(LifecycleError.PURGE_STATE_NOT_STATED.code(), HttpStatus.BAD_REQUEST),
            Map.entry(MonitorError.NO_OBSERVATION.code(), HttpStatus.NOT_FOUND),
            Map.entry(MonitorError.INVALID_CURSOR.code(), HttpStatus.BAD_REQUEST),
            Map.entry(MonitorError.QUERY_BUDGET_EXCEEDED.code(), HttpStatus.BAD_REQUEST),
            Map.entry(MonitorError.CURSOR_EXPIRED.code(), HttpStatus.GONE),
            Map.entry(DataBrowserError.UNKNOWN_COLLECTION.code(), HttpStatus.NOT_FOUND),
            Map.entry(DataBrowserError.INVALID_LIMIT.code(), HttpStatus.BAD_REQUEST),
            Map.entry(DataBrowserError.CONNECTOR_NOT_BROWSABLE.code(), HttpStatus.BAD_REQUEST),
            Map.entry(DataBrowserError.UNORDERABLE_FIELD.code(), HttpStatus.BAD_REQUEST),
            Map.entry(PositionError.WRITE_BACK_WHILE_LIVE.code(), HttpStatus.CONFLICT),
            Map.entry(PositionError.CHAIN_NOT_READ.code(), HttpStatus.BAD_REQUEST),
            Map.entry(PositionError.FIELD_NOT_EDITABLE.code(), HttpStatus.BAD_REQUEST),
            Map.entry(PositionError.NOTHING_TO_WRITE.code(), HttpStatus.BAD_REQUEST),
            Map.entry(ClusterError.MEMBERSHIP_UNREADABLE.code(), HttpStatus.SERVICE_UNAVAILABLE)));
        // Explicit registration survives a change in wire identity. The domain fallback above serves
        // only legacy codes that have not yet migrated; it must not classify new numeric identities.
        for (DslError code : DslError.values()) statuses.put(code.code(), HttpStatus.BAD_REQUEST);
        return Map.copyOf(statuses);
    }

    /** The {@code <domain>} segment of a canonical {@code <domain>.<symbol>} code. */
    private static String domainOf(String code) {
        int dot = code.indexOf('.');
        return dot < 0 ? code : code.substring(0, dot);
    }
}
