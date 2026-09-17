package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One of the two config forms a connector's own spec declares, and the reader for what it declares.
 *
 * <p>Both forms are the connector's own declaration, and they say different things about the same
 * authored value:
 *
 * <ul>
 *   <li>the <b>connection</b> form ({@code configOptions.connection}) is what a connection is authored
 *       against — where the database is and how to reach it;</li>
 *   <li>the <b>node</b> form ({@code configOptions.node}) belongs to one use of that connection inside
 *       a pipeline — how this read or this write behaves. A connector reads these off the node config
 *       the host hands it, and nowhere else.</li>
 * </ul>
 *
 * <p>The host authors one settings map, so the form a name is declared in is the only thing that says
 * which config a connector will look for that setting in. That makes both forms host-side questions,
 * asked of the raw spec text the ref carries: read with the host's json reader, never under the
 * connector's loader, and answered only from what the connector shipped — no reflection, and no table
 * of field names of our own.
 */
enum ConnectorForm {

    CONNECTION("connection"),
    NODE("node");

    /** The key this form is declared under inside the spec's {@code configOptions} object. */
    private final String key;

    ConnectorForm(String key) {
        this.key = key;
    }

    /**
     * Every leaf item this form declares, keyed by the name the connector reads that value under, and
     * empty when there is nothing to read it from — no spec, a spec that is not an object, or one
     * declaring no such form. Connectors are free to ship one form and not the other, and the synthetic
     * connectors carry no spec at all.
     */
    Map<String, Map<?, ?>> items(String spec) {
        return spec == null || !(JsonReader.parse(spec) instanceof Map<?, ?> root) ? Map.of() : itemsIn(root);
    }

    /**
     * As {@link #items(String)}, from an already-parsed spec — so a caller that reads both forms parses
     * the spec once rather than once per form.
     */
    Map<String, Map<?, ?>> itemsIn(Map<?, ?> specRoot) {
        Map<String, Map<?, ?>> items = new LinkedHashMap<>();
        if (specRoot.get("configOptions") instanceof Map<?, ?> configOptions
                && configOptions.get(key) instanceof Map<?, ?> form
                && form.get("properties") instanceof Map<?, ?> properties) {
            collect(properties, items);
        }
        return items;
    }

    /**
     * Collects the form's leaves, flattening container nodes: a form groups fields under a node of its
     * own (a titled section, an optional-fields block), while the connector still reads them by their
     * leaf name.
     */
    private static void collect(Map<?, ?> properties, Map<String, Map<?, ?>> out) {
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof Map<?, ?> item)) {
                continue; // a non-object property value is a malformed form item — skip it, don't guess
            }
            if (item.get("properties") instanceof Map<?, ?> nested) {
                collect(nested, out);
            } else {
                out.put(name, item);
            }
        }
    }
}
