package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.model.PipelineResource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * One answer a finding offers.
 *
 * <p>Its kind is an open set, and a client chooses how to present an action from {@link #destructive}
 * and {@link #changes} alone -- never from the kind -- so a check can add an action without any client
 * learning about it. The one kind a client may recognise is {@link #ACKNOWLEDGE}, go ahead as configured,
 * which every confirmation offers exactly once.
 *
 * <p>An action that changes the definition carries the change as a pure function over the definition,
 * together with a preview of it. The function is only ever applied by the start itself, through the
 * same write a person editing the pipeline goes through; a check never writes anything.
 *
 * @param id          what a decision names, unique within the finding
 * @param kind        {@link #ACKNOWLEDGE}, {@link #CHANGE_DEFINITION}, or a kind a later check adds
 * @param destructive whether taking it loses data a person may want
 * @param code        names its label
 * @param params      the named values its label is rendered from
 * @param changes     what it changes in the definition, for a person to see before choosing it
 * @param change      the change itself; null for an action that changes nothing
 */
public record StartAction(
        String id, String kind, boolean destructive, TapstateErrorCode code, Map<String, Object> params,
        List<Change> changes, UnaryOperator<PipelineResource> change) {

    /** Go ahead with the definition as it is. */
    public static final String ACKNOWLEDGE = "ACKNOWLEDGE";
    /** Change the definition, then go ahead with the changed one. */
    public static final String CHANGE_DEFINITION = "CHANGE_DEFINITION";

    public StartAction {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(code, "code");
        params = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(params, "params")));
        changes = List.copyOf(Objects.requireNonNull(changes, "changes"));
        for (String name : code.placeholders()) {
            if (!params.containsKey(name)) {
                throw new IllegalArgumentException("action " + id + " gives no value for " + name);
            }
        }
        if (ACKNOWLEDGE.equals(kind) && (destructive || change != null || !changes.isEmpty())) {
            throw new IllegalArgumentException("action " + id + " goes ahead as configured, so it changes nothing");
        }
        if (CHANGE_DEFINITION.equals(kind) && (change == null || changes.isEmpty())) {
            throw new IllegalArgumentException("action " + id + " changes the definition, so it says how");
        }
    }

    /** The action that goes ahead with the definition as it is. */
    public static StartAction acknowledge(String id, TapstateErrorCode code, Map<String, Object> params) {
        return new StartAction(id, ACKNOWLEDGE, false, code, params, List.of(), null);
    }

    /** An action that changes the definition before the start goes ahead. */
    public static StartAction changeDefinition(String id, boolean destructive, TapstateErrorCode code,
            Map<String, Object> params, List<Change> changes, UnaryOperator<PipelineResource> change) {
        return new StartAction(id, CHANGE_DEFINITION, destructive, code, params, changes,
                Objects.requireNonNull(change, "change"));
    }

    /** Whether this is the answer "go ahead as configured". */
    public boolean acknowledges() {
        return ACKNOWLEDGE.equals(kind);
    }

    /**
     * One field an action changes.
     *
     * @param element the write element changed
     * @param field   the field, as it is written in the definition
     * @param from    its value now
     * @param to      its value after
     */
    public record Change(String element, String field, String from, String to) {

        public Change {
            Objects.requireNonNull(element, "element");
            Objects.requireNonNull(field, "field");
            Objects.requireNonNull(to, "to");
        }
    }
}
