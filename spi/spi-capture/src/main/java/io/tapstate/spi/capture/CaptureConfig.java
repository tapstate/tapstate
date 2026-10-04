package io.tapstate.spi.capture;

import io.tapstate.core.model.PipelineNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A resolved capture configuration: which connector to run, the connection settings to run it with,
 * the streams to read, and which pipeline node is reading them. An immutable value.
 *
 * <p>{@code connectorId} is the catalog id of the connector. {@code settings} are the resolved
 * connection values keyed by the connector's config field names. {@code streams} are the logical
 * stream (table) names to capture; each becomes the {@code src} of the events yielded. An empty
 * {@code streams} means every stream the connector exposes.
 *
 * <p>{@code node} is the pipeline and source the read is being driven for. It scopes whatever the
 * connector keeps for itself, so the two phases of one run - the full load and the change tail that
 * follows it - are one node and read each other's notes, while two pipelines reading the same
 * database keep theirs apart. It is null for a read that has no node to name: schema discovery, a
 * connection test and a data browse each live for a single call, so nothing they wrote would ever be
 * read back.
 *
 * <p>The node is deliberately absent from what a shared change stream is keyed by. That identity is
 * the physical source coordinate - the point of it is that two pipelines reading one database mine it
 * once - so folding a per-pipeline id into it would give every pipeline its own stream and mine the
 * same log as many times as there are readers.
 *
 * <p>{@code sharedNotes}, when present, scopes native connector notes to the physical capture while
 * {@code node} still identifies the pipeline whose run and logs opened it. Ordinary and direct reads
 * keep the node's existing private notes.
 *
 * <p>{@code settings} and {@code streams} are held as unmodifiable defensive copies; a null map or
 * list is normalized to empty.
 */
public record CaptureConfig(
        String connectorId, Map<String, Object> settings, List<String> streams, PipelineNode node,
        SharedNotes sharedNotes) {

    public CaptureConfig {
        Objects.requireNonNull(connectorId, "connectorId");
        settings = settings == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(settings));
        streams = streams == null ? List.of() : List.copyOf(streams);
    }

    /** A capture using its node's own notes, preserving the ordinary connector configuration contract. */
    public CaptureConfig(String connectorId, Map<String, Object> settings, List<String> streams, PipelineNode node) {
        this(connectorId, settings, streams, node, null);
    }

    /** A config for a read that names no node — the read-only drives, which have none. */
    public CaptureConfig(String connectorId, Map<String, Object> settings, List<String> streams) {
        this(connectorId, settings, streams, null);
    }

    /** The same config, read on behalf of {@code node}. */
    public CaptureConfig at(PipelineNode node) {
        return new CaptureConfig(connectorId, settings, streams, node, sharedNotes);
    }

    /** The same owner and source configuration, with native notes belonging to one physical capture. */
    public CaptureConfig sharing(SharedNotes notes) {
        return new CaptureConfig(connectorId, settings, streams, node, notes);
    }
}
