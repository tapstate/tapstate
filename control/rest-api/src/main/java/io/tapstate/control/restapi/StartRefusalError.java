package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.StartCheckReport;
import io.tapstate.control.core.StartOutcome;

import java.util.List;
import java.util.Map;

/**
 * The body of a start that did not go ahead: every field of a coded error, in the same places, and beside
 * them what the start checks said and what the start had already changed.
 *
 * <p>A superset on purpose. A client that only knows the coded error shape reads the code, the arguments
 * and a message that says by itself how to answer; a client that knows start checks reads the report.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record StartRefusalError(String code, Map<String, Object> params, String message, StartCheckReport startChecks,
        List<StartOutcome.Applied> decisionsApplied) {
}
