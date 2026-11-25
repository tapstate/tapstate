package io.tapstate.spi.capture;

import io.tapstate.core.model.PipelineNode;
import java.util.List;
import java.util.Objects;

/**
 * Native connector notes owned by one physical capture, independently of its current pipeline owner.
 * Known earlier source nodes may supply compatible notes when the shared namespace is first opened;
 * their original state is preserved, and conflicting values must never be selected by guesswork.
 */
public record SharedNotes(String sharedBy, List<PipelineNode> carriedFrom) {

    public SharedNotes {
        Objects.requireNonNull(sharedBy, "sharedBy");
        if (sharedBy.isBlank()) {
            throw new IllegalArgumentException("shared notes belong to a non-blank physical capture");
        }
        carriedFrom = carriedFrom == null ? List.of() : List.copyOf(carriedFrom.stream().distinct().toList());
    }
}
