package io.tapstate.core.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Optional metadata block of any resource (§2). Carries annotations and server-managed provenance;
 * never carries the resource identity ({@code metadata.name} was abolished by F6).
 */
@Doc("Optional metadata: labels, description, and server-managed provenance. Never carries resource identity.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Metadata(
        @Doc("Arbitrary key/value labels attached to the resource for grouping and selection.")
        Map<String, String> labels,
        @Doc("Free-text description of the resource; never identity.")
        @JsonInclude(JsonInclude.Include.ALWAYS)
        String description,
        @Doc("Server-managed marker carried by resources created in a managed Cloud Cluster.")
        Boolean cloud,
        @Doc("Server-managed stable user id of the authenticated creator, when one is known.")
        @JsonProperty("user_id")
        String userId) {

    public Metadata {
        labels = labels == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(labels));
    }

    /** The authoring shape used before server-managed Cloud attribution fields were added. */
    public Metadata(Map<String, String> labels, String description) {
        this(labels, description, null, null);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return labels.isEmpty()
                && (description == null || description.isEmpty())
                && cloud == null
                && (userId == null || userId.isEmpty());
    }

    @Override
    public Map<String, String> labels() {
        return Objects.requireNonNullElse(labels, Map.of());
    }
}
