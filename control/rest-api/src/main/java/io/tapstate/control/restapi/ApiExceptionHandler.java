package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;
import java.util.TreeMap;

/**
 * Projects a coded first-party error onto a structured HTTP response. A {@link TapstateException} — a
 * user-facing, diagnosable failure — becomes a {@code {code, params, message}} body: the canonical code
 * string, the named arguments, and the message rendered from them through the shared catalog. The status
 * is chosen from the code: a client input error (a {@code dsl.*} validation failure or a
 * {@code control.malformed-request} refused at the boundary) is a 400; the authentication codes map to
 * 401 / 403 / 409; a lifecycle verb on an unknown pipeline is a 404 and a forbidden transition or stale
 * revision is a 409; a status / metrics / snapshot read of a pipeline that has published no observation is a
 * 404; any other coded error keeps the structured body but answers 500, since the surface has no
 * client-attributable mapping for it yet. That mapping is the seam later slices extend as more
 * client-attributable codes land.
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
        MessageCatalog.Rendered rendered = catalog.render(e.code(), e.args());
        // Sorted so the params render identically regardless of throw-site order (a stable machine contract).
        ApiError body = new ApiError(e.code().code(), new TreeMap<>(e.args()), rendered.message());
        return ResponseEntity.status(statusFor(e.code())).body(body);
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
        MessageCatalog.Rendered rendered = catalog.render(e.code(), e.args());
        ApiError body = new ApiError(e.code().code(), new TreeMap<>(e.args()), rendered.message());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    /**
     * The HTTP status for a coded error. The authentication codes are client-attributable and map to the
     * usual auth statuses: no / invalid credential and a rejected login are 401, an under-scoped or
     * non-loopback caller is 403, and a bootstrap channel that has already closed is a 409 state conflict.
     * A client input error — a {@code dsl.*} validation failure or a {@code control.malformed-request} refused
     * at the boundary — is a 400. A lifecycle verb on a pipeline that was never applied is a 404; a transition
     * the state machine forbids, or a start/resume at a stale revision, is a 409 state conflict. A status /
     * metrics / snapshot read of a pipeline that has published no observation is likewise a 404. Any other coded
     * error is a server-side failure (500) that still carries the structured body — never a bare, uncoded crash.
     */
    static HttpStatus statusFor(TapstateErrorCode code) {
        return switch (code.code()) {
            case "control.auth-failed", "control.unauthenticated" -> HttpStatus.UNAUTHORIZED;
            case "control.forbidden", "control.bootstrap-forbidden" -> HttpStatus.FORBIDDEN;
            case "control.bootstrap-closed" -> HttpStatus.CONFLICT;
            case "source.id-mismatch" -> HttpStatus.BAD_REQUEST;
            case "source.not-found" -> HttpStatus.NOT_FOUND;
            case "source.already-exists", "source.in-use" -> HttpStatus.CONFLICT;
            case "source.version-conflict" -> HttpStatus.PRECONDITION_FAILED;
            case "source.precondition-required" -> HttpStatus.PRECONDITION_REQUIRED;
            // The artifact refusals, mirroring the source.* mapping above because they mean the same things
            // one kind wider. This switch sees only the code, never the endpoint that raised it, so a code
            // both delete and apply can raise answers the same status on both: version-conflict is a 412 on
            // apply too, where the precondition travels in the request body rather than an If-Match header.
            // Giving one of them an endpoint-dependent status means changing this method's shape, not this
            // line — a delete path and an apply path that each map half of it would be worse than either.
            case "artifact.not-found" -> HttpStatus.NOT_FOUND;
            case "artifact.precondition-required" -> HttpStatus.PRECONDITION_REQUIRED;
            case "artifact.version-conflict" -> HttpStatus.PRECONDITION_FAILED;
            case "artifact.in-use", "artifact.pipeline-not-stopped" -> HttpStatus.CONFLICT;
            // Not a 4xx: the request was valid and was carried out. What failed is the server's own
            // follow-up work, and the body's code — not the status — is what tells the caller the
            // removal stands and must not be retried.
            case "artifact.reclaim-incomplete" -> HttpStatus.INTERNAL_SERVER_ERROR;
            case "connector.not-found" -> HttpStatus.NOT_FOUND;
            case "pipeline.not-found" -> HttpStatus.NOT_FOUND;
            // A request refused at the HTTP boundary as structurally malformed is a client input error, like dsl.*.
            case "control.malformed-request" -> HttpStatus.BAD_REQUEST;
            // A lifecycle verb on a pipeline that was never applied is a 404; a verb the state machine forbids
            // from the current state, or a start/resume at a stale revision, is a 409 state conflict.
            case "lifecycle.unknown-pipeline" -> HttpStatus.NOT_FOUND;
            case "lifecycle.illegal-transition", "lifecycle.incompatible-revision" -> HttpStatus.CONFLICT;
            // A status / metrics / snapshot read of a pipeline that has published no observation is a 404: the
            // observation resource does not exist yet, like a get of an unknown artifact.
            case "monitor.no-observation" -> HttpStatus.NOT_FOUND;
            // A browse of a collection the source's database does not hold is a 404 — the collection a caller
            // named does not exist, like a get of an unknown artifact; a size this face will not serve is
            // input it refused before reaching a connector, so it is a 400. Both are the caller's to fix, and
            // both would otherwise fall through to a 500 that blames the server for a mistyped request.
            case "data-browser.unknown-collection" -> HttpStatus.NOT_FOUND;
            // All of these are judgements made before anything is sent, on the request as written: a
            // size this face will not serve, and a source whose connector it cannot ask for rows at all.
            // Left to the default they would come back as 500s blaming the server for the caller's
            // request - and a caller cannot tell that apart from the product having fallen over, which
            // is the one thing a refusal has to be distinguishable from.
            // A third of the same kind: an order by a field whose own name holds a dot, which cannot be
            // served in any order at all and is refused rather than answered in one nobody applied.
            case "data-browser.invalid-limit", "data-browser.connector-not-browsable",
                 "data-browser.unorderable-field" ->
                    HttpStatus.BAD_REQUEST;
            default -> switch (domainOf(code.code())) {
                case "dsl" -> HttpStatus.BAD_REQUEST;
                default -> HttpStatus.INTERNAL_SERVER_ERROR;
            };
        };
    }

    /** The {@code <domain>} segment of a canonical {@code <domain>.<symbol>} code. */
    private static String domainOf(String code) {
        int dot = code.indexOf('.');
        return dot < 0 ? code : code.substring(0, dot);
    }
}
