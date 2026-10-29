package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StartRefusal;
import io.tapstate.control.core.CloudAuthenticationObserver;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
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
import java.util.TreeMap;

/**
 * Projects a coded first-party error onto a structured HTTP response. A {@link TapstateException} — a
 * user-facing, diagnosable failure — becomes a {@code {code, params, message}} body: the canonical code
 * string, the named arguments, and the message rendered from them through the shared catalog. The status
 * is chosen from the code: a client input error (a {@code dsl.*} validation failure or a
 * {@code control.malformed-request} refused at the boundary) is a 400; the authentication codes map to
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
        markCloudError(e.code());
        MessageCatalog.Rendered rendered = catalog.render(e.code(), e.args());
        // Sorted so the params render identically regardless of throw-site order (a stable machine contract).
        ApiError body = new ApiError(e.code().code(), new TreeMap<>(e.args()), rendered.message());
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
     * A client input error — a {@code dsl.*} validation failure or a {@code control.malformed-request} refused
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
    private static final Map<String, HttpStatus> HTTP_STATUSES = Map.ofEntries(
            Map.entry("control.storage-unavailable", HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("control.storage-test-required", HttpStatus.CONFLICT),
            Map.entry("control.storage-scope-conflict", HttpStatus.CONFLICT),
            Map.entry("io.configuration-import-refused", HttpStatus.CONFLICT),
            Map.entry("store.initialization-refused", HttpStatus.CONFLICT),
            Map.entry("actuation.source-schema-not-discovered", HttpStatus.CONFLICT),
            Map.entry("io.pipeline-table-selection-rebuild-required", HttpStatus.CONFLICT),
            Map.entry("io.pipeline-state-rebuild-required", HttpStatus.CONFLICT),
            Map.entry("control.storage-test-failed", HttpStatus.BAD_REQUEST),
            Map.entry("control.storage-databases-overlap", HttpStatus.BAD_REQUEST),
            Map.entry("store.storage-connection-unsupported", HttpStatus.BAD_REQUEST),
            Map.entry("store.invalid-uri", HttpStatus.BAD_REQUEST),
            Map.entry("store.tls-trust-invalid", HttpStatus.BAD_REQUEST),
            Map.entry("control.ai-not-configured", HttpStatus.CONFLICT),
            Map.entry("control.ai-provider-auth-failed", HttpStatus.UNPROCESSABLE_CONTENT),
            Map.entry("control.ai-provider-failed", HttpStatus.BAD_GATEWAY),
            Map.entry("control.ai-invalid-proposal", HttpStatus.UNPROCESSABLE_CONTENT),
            Map.entry("control.ai-overloaded", HttpStatus.TOO_MANY_REQUESTS),
            Map.entry("control.auth-failed", HttpStatus.UNAUTHORIZED),
            Map.entry("control.unauthenticated", HttpStatus.UNAUTHORIZED),
            Map.entry("control.cloud-auth-unavailable", HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("control.auth-mode-unavailable", HttpStatus.FORBIDDEN),
            Map.entry("control.forbidden", HttpStatus.FORBIDDEN),
            Map.entry("control.bootstrap-forbidden", HttpStatus.FORBIDDEN),
            Map.entry("control.bootstrap-closed", HttpStatus.CONFLICT),
            Map.entry("source.id-mismatch", HttpStatus.BAD_REQUEST),
            Map.entry("source.not-found", HttpStatus.NOT_FOUND),
            Map.entry("source.already-exists", HttpStatus.CONFLICT),
            Map.entry("source.in-use", HttpStatus.CONFLICT),
            Map.entry("source.srs-change-while-running", HttpStatus.CONFLICT),
            Map.entry("source.version-conflict", HttpStatus.PRECONDITION_FAILED),
            Map.entry("source.precondition-required", HttpStatus.PRECONDITION_REQUIRED),
            Map.entry("artifact.not-found", HttpStatus.NOT_FOUND),
            Map.entry("artifact.precondition-required", HttpStatus.PRECONDITION_REQUIRED),
            Map.entry("artifact.version-conflict", HttpStatus.PRECONDITION_FAILED),
            Map.entry("artifact.in-use", HttpStatus.CONFLICT),
            Map.entry("artifact.pipeline-not-stopped", HttpStatus.CONFLICT),
            Map.entry("artifact.reclaim-incomplete", HttpStatus.INTERNAL_SERVER_ERROR),
            Map.entry("connector.not-found", HttpStatus.NOT_FOUND),
            Map.entry("pipeline.not-found", HttpStatus.NOT_FOUND),
            Map.entry("pipeline.already-exists", HttpStatus.CONFLICT),
            Map.entry("pipeline.id-mismatch", HttpStatus.BAD_REQUEST),
            Map.entry("pipeline.precondition-required", HttpStatus.PRECONDITION_REQUIRED),
            Map.entry("pipeline.version-conflict", HttpStatus.PRECONDITION_FAILED),
            Map.entry("pipeline.definition-change-unavailable", HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("pipeline-draft.not-found", HttpStatus.NOT_FOUND),
            Map.entry("pipeline-draft.already-exists", HttpStatus.CONFLICT),
            Map.entry("pipeline-draft.mode-conflict", HttpStatus.CONFLICT),
            Map.entry("pipeline-draft.artifact-conflict", HttpStatus.CONFLICT),
            Map.entry("pipeline-draft.revision-conflict", HttpStatus.PRECONDITION_FAILED),
            Map.entry("pipeline-draft.precondition-required", HttpStatus.PRECONDITION_REQUIRED),
            Map.entry("pipeline-draft.invalid", HttpStatus.BAD_REQUEST),
            Map.entry("control.malformed-request", HttpStatus.BAD_REQUEST),
            Map.entry("control.preview-overloaded", HttpStatus.SERVICE_UNAVAILABLE),
            Map.entry("lifecycle.unknown-pipeline", HttpStatus.NOT_FOUND),
            Map.entry("lifecycle.illegal-transition", HttpStatus.CONFLICT),
            Map.entry("lifecycle.incompatible-revision", HttpStatus.CONFLICT),
            Map.entry("lifecycle.start-needs-confirmation", HttpStatus.CONFLICT),
            Map.entry("lifecycle.start-blocked", HttpStatus.CONFLICT),
            Map.entry("lifecycle.invalid-start-decision", HttpStatus.BAD_REQUEST),
            Map.entry("lifecycle.pipeline-not-runnable", HttpStatus.CONFLICT),
            Map.entry("lifecycle.purge-state-not-stated", HttpStatus.BAD_REQUEST),
            Map.entry("monitor.no-observation", HttpStatus.NOT_FOUND),
            Map.entry("monitor.invalid-cursor", HttpStatus.BAD_REQUEST),
            Map.entry("monitor.query-budget-exceeded", HttpStatus.BAD_REQUEST),
            Map.entry("monitor.cursor-expired", HttpStatus.GONE),
            Map.entry("data-browser.unknown-collection", HttpStatus.NOT_FOUND),
            Map.entry("data-browser.invalid-limit", HttpStatus.BAD_REQUEST),
            Map.entry("data-browser.connector-not-browsable", HttpStatus.BAD_REQUEST),
            Map.entry("data-browser.unorderable-field", HttpStatus.BAD_REQUEST),
            Map.entry("position.write-back-while-live", HttpStatus.CONFLICT),
            Map.entry("position.chain-not-read", HttpStatus.BAD_REQUEST),
            Map.entry("position.field-not-editable", HttpStatus.BAD_REQUEST),
            Map.entry("position.nothing-to-write", HttpStatus.BAD_REQUEST),
            Map.entry("cluster.membership-unreadable", HttpStatus.SERVICE_UNAVAILABLE));

    /** The {@code <domain>} segment of a canonical {@code <domain>.<symbol>} code. */
    private static String domainOf(String code) {
        int dot = code.indexOf('.');
        return dot < 0 ? code : code.substring(0, dot);
    }
}
