package io.tapstate.control.restapi;

import io.tapstate.control.core.ViewCatalogItem;
import java.util.List;

/** Ordered collection of materialized views and table replicas. */
record ViewList(List<ViewCatalogItem> items) {
    ViewList {
        items = List.copyOf(items);
    }
}
