package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Objects;

/**
 * How heavy one namespace's state is to keep since its counting began: how many bytes each key took when it was
 * written - at a nest's root level, how heavy each document is - the largest of them, how many bytes each commit
 * group wrote, and for a level that renders rollup fields, how long folding them took per document.
 *
 * <p>Every change to a document rewrites that document's whole state, so its weight is what a change costs. A
 * distribution of it says how many documents are heavy; the largest says how close the heaviest has come to what
 * one stored document may hold.
 *
 * @param documentBytes bytes per written key, over the registered bounds
 * @param largestDocumentBytes the most bytes one key's state has taken
 * @param commitBytes bytes per commit group, over the registered bounds
 * @param foldDuration seconds per rollup fold, over the registered bounds; null where nothing was folded
 * @param countingSince when these counts began
 */
public record StateWeightReading(HistogramValue documentBytes, long largestDocumentBytes, HistogramValue commitBytes,
        HistogramValue foldDuration, Instant countingSince) {

    public StateWeightReading {
        Objects.requireNonNull(documentBytes, "documentBytes");
        Objects.requireNonNull(commitBytes, "commitBytes");
        Objects.requireNonNull(countingSince, "countingSince");
        if (largestDocumentBytes < 0) {
            throw new IllegalArgumentException("a size must not be negative");
        }
    }
}
