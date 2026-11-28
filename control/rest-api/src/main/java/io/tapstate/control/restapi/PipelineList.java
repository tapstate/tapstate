package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

/** Ordered Pipeline collection returned by the HTTP list endpoint. */
record PipelineList(List<Item> items) {

    PipelineList {
        items = List.copyOf(items);
    }

    static PipelineList of(List<PipelineCatalogItem> items, MessageCatalog catalog, Locale locale) {
        return new PipelineList(items.stream().map(item -> Item.of(item, catalog, locale)).toList());
    }

    record Item(String id, String name, String description, String mode, Long revision, Instant updatedAt,
            String contentHash, boolean hasArtifact, Status status,
            @JsonInclude(JsonInclude.Include.NON_NULL)
            PipelineCatalogItem.SyncLag syncLag) {

        static Item of(PipelineCatalogItem item, MessageCatalog catalog, Locale locale) {
            return new Item(item.id(), item.name(), item.description(), item.mode(), item.revision(), item.updatedAt(),
                    item.contentHash(), item.hasArtifact(), Status.of(item.status(), catalog, locale), item.syncLag());
        }
    }

    record Status(PipelineCatalogItem.DisplayState state, PipelineCatalogItem.UiState displayState,
            PipelineState desiredState, PipelineState observedState, ApiError failure, Instant observedAt,
            ApiError intentReadFailure, PipelineCatalogItem.StopState stopState) {

        static Status of(PipelineCatalogItem.Status status, MessageCatalog catalog, Locale locale) {
            return new Status(status.state(), status.displayState(), status.desiredState(), status.observedState(),
                    ApiError.from(status.failure(), catalog, locale), status.observedAt(),
                    ApiError.from(status.intentReadFailure(), catalog, locale), status.stopState());
        }
    }
}
