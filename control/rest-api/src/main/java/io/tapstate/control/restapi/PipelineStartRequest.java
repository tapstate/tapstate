package io.tapstate.control.restapi;

import io.tapstate.control.core.StartDecision;

import java.util.List;

/**
 * The optional body of a start: the answers to the start checks' questions. A start sent with no body
 * answers none, which is how every client written before start checks starts a pipeline.
 */
record PipelineStartRequest(List<StartDecision> decisions) {

    /** The answers carried, none when there is no body. */
    static List<StartDecision> decisionsOf(PipelineStartRequest request) {
        return request == null || request.decisions() == null ? List.of() : request.decisions();
    }
}
