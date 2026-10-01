package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineCatalogItem;

import java.util.List;

/** Ordered Pipeline collection returned by the HTTP list endpoint. */
record PipelineList(List<PipelineCatalogItem> items) {

    PipelineList {
        items = List.copyOf(items);
    }
}
