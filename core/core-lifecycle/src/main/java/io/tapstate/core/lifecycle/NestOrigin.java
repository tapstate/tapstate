package io.tapstate.core.lifecycle;

import java.util.Objects;

/**
 * Where in a pipeline's definition one nest state namespace comes from: the nest step that keeps it, the
 * level of that nest's document it serves, and which of the step's maps it is.
 *
 * <p>A namespace is named by a digest of its physical coordinates, which is what keeps two runs and two
 * levels apart and also what makes it unreadable: a reading filed under a sixty-four character digest
 * says nothing about which part of the document it is about. These three are the definition's own names
 * for the same place, so a reader can be shown "the {@code lines} level of {@code order_doc}" instead.
 * They are names the definition draws, never values read out of a row, and each namespace has exactly
 * one of them, so carrying them grows no series.
 *
 * @param step the nest step's id as the pipeline definition names it
 * @param path the level within the nest's document, as the canonical JSON array of its path components;
 *     {@code []} is the root
 * @param map which of the step's maps this is: {@code state}, {@code parking}, {@code lookup:<alias>} or
 *     {@code references:<alias>}
 */
public record NestOrigin(String step, String path, String map) {

    public NestOrigin {
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(map, "map");
    }
}
