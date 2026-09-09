package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineView;

import java.util.List;

/** Ordered Pipeline collection returned by the HTTP list endpoint. */
record PipelineList(List<PipelineView> items) {

    PipelineList {
        items = List.copyOf(items);
    }
}
