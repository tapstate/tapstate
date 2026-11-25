package io.tapstate.control.restapi;

import io.tapstate.control.core.ViewCatalogItem;
import io.tapstate.control.core.PipelineOutputNames;
import java.util.List;

/** Ordered collection of materialized views and table replicas. */
record ViewList(List<ViewCatalogItem> items, List<PipelineOutputNames.Unresolved> unresolved) {
    ViewList {
        items = List.copyOf(items);
        unresolved = List.copyOf(unresolved);
    }
}
