package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One {@code serve.push} element — event-stream egress, no table model (ADR-0016 §8,
 * X11): {@code {id?, source, topic?, format?, options?}}; the connector category derives
 * from the referenced source, never a {@code type} field.
 */
@Doc("A serve.push egress element that streams change events to a target connector topic, with no table model.")
public record PushElement(@Doc("Optional id for this push element; defaults to a generated id when omitted.")
                          String id,
                          @Doc(value = "Id of the source resource this egress reads change events from.", required = true)
                          String source,
                          @Doc("Target topic or channel the change events are pushed to.")
                          String topic,
                          @Doc("Serialization format used to encode each pushed change event.")
                          PushFormat format) {

    public PushElement {
        Objects.requireNonNull(source, "source");
    }
}
