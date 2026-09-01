package io.tapstate.core.dsl;

import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A located view over a YAML mapping node (plan poc1 B3): typed key access plus strict
 * unknown-key rejection (§11.5). Every error carries the field path and the 1-based source
 * position taken from the underlying snakeyaml marks, so the parse layer reports
 * file / line / field-path the way the corpus contract requires.
 */
final class YamlMap {

    private final String path;                                  // "" at the document root
    private final Map<String, Node> values = new LinkedHashMap<>();
    private final Map<String, Node> keyNodes = new LinkedHashMap<>();
    private Node self;                                          // the mapping itself, for locating

    private YamlMap(String path) {
        this.path = path;
    }

    static YamlMap of(MappingNode node, String path) {
        YamlMap m = new YamlMap(path);
        m.self = node;
        for (NodeTuple tuple : node.getValue()) {
            String key = ((ScalarNode) tuple.getKeyNode()).getValue();
            m.values.put(key, tuple.getValueNode());
            m.keyNodes.put(key, tuple.getKeyNode());
        }
        return m;
    }

    /** Rejects any key outside {@code allowed} with code {@link DslError#UNKNOWN_FIELD} (§11.5). */
    void requireOnly(Set<String> allowed) {
        for (String key : values.keySet()) {
            if (!allowed.contains(key)) {
                throw error(DslError.UNKNOWN_FIELD, childPath(key), keyNodes.get(key),
                        Map.of("field", key));
            }
        }
    }

    /**
     * Rejects a mapping missing any of {@code required} with code {@link DslError#MISSING_FIELD}.
     * Reported at the mapping's own position: an absent key has no node to point at, and a diagnostic
     * naming a field with no place to put it is most of the answer missing in a directory of files.
     *
     * <p>Reports the alphabetically first of several, so the same document always names the same
     * field. Iteration order over the caller's set is not specified, and a diagnostic that varies
     * between runs is one no test can pin and no user can compare against a colleague's.
     */
    void requirePresent(Set<String> required) {
        String missing = null;
        for (String key : required) {
            if (!values.containsKey(key) && (missing == null || key.compareTo(missing) < 0)) {
                missing = key;
            }
        }
        if (missing != null) {
            throw error(DslError.MISSING_FIELD, childPath(missing), self, Map.of("field", missing));
        }
    }

    boolean has(String key) {
        return values.containsKey(key);
    }

    /** Declared keys in insertion order. */
    Set<String> keys() {
        return values.keySet();
    }

    /** Scalar value as a raw string (no type coercion); null if absent; error if not a scalar. */
    String string(String key) {
        Node n = values.get(key);
        if (n == null) {
            return null;
        }
        if (!(n instanceof ScalarNode sc)) {
            throw errorAt(key, DslError.ILLEGAL_VALUE,
                    Map.of("value", nodeTypeName(n), "expected", "a scalar"));
        }
        return sc.getValue();
    }

    /** Free-form typed value (scalar / list / map) per the Tapstate dialect; null if absent. */
    Object value(String key) {
        Node n = values.get(key);
        return n == null ? null : nodeValue(n);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> freeMap(String key) {
        return (Map<String, Object>) value(key);
    }

    /** Sub-mapping as a located view, carrying the extended path; null if absent; error if not a mapping. */
    YamlMap mapping(String key) {
        Node n = values.get(key);
        if (n == null) {
            return null;
        }
        if (!(n instanceof MappingNode mn)) {
            throw errorAt(key, DslError.ILLEGAL_VALUE,
                    Map.of("value", nodeTypeName(n), "expected", "a mapping"));
        }
        return of(mn, childPath(key));
    }

    /** Sequence elements as raw nodes; null if absent; error if not a sequence. */
    List<Node> seq(String key) {
        Node n = values.get(key);
        if (n == null) {
            return null;
        }
        if (!(n instanceof SequenceNode sn)) {
            throw errorAt(key, DslError.ILLEGAL_VALUE,
                    Map.of("value", nodeTypeName(n), "expected", "a list"));
        }
        return sn.getValue();
    }

    Node node(String key) {
        return values.get(key);
    }

    String childPath(String key) {
        return path.isEmpty() ? key : path + "." + key;
    }

    /** Builds an error positioned at the value node of {@code key} (or the map's own mark). */
    DslException errorAt(String key, DslError code, Map<String, Object> args) {
        return error(code, childPath(key), values.get(key), args);
    }

    // ---- node typing + error construction -----------------------------------------

    /** Converts a node to a plain Java value under the Tapstate dialect (only true/false bool). */
    static Object nodeValue(Node node) {
        if (node instanceof ScalarNode sc) {
            Tag tag = sc.getTag();
            String v = sc.getValue();
            if (Tag.NULL.equals(tag)) {
                return null;
            }
            if (Tag.BOOL.equals(tag)) {
                return Boolean.valueOf(v);
            }
            if (Tag.INT.equals(tag)) {
                return parseInt(v);
            }
            if (Tag.FLOAT.equals(tag)) {
                return Double.valueOf(v);
            }
            return v;
        }
        if (node instanceof MappingNode mn) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (NodeTuple t : mn.getValue()) {
                m.put(((ScalarNode) t.getKeyNode()).getValue(), nodeValue(t.getValueNode()));
            }
            return m;
        }
        if (node instanceof SequenceNode sn) {
            List<Object> l = new ArrayList<>();
            for (Node item : sn.getValue()) {
                l.add(nodeValue(item));
            }
            return l;
        }
        return null;
    }

    private static Object parseInt(String raw) {
        boolean negative = raw.startsWith("-");
        String s = (raw.startsWith("+") || raw.startsWith("-")) ? raw.substring(1) : raw;
        int radix = 10;
        if (s.startsWith("0x")) {
            radix = 16;
            s = s.substring(2);
        } else if (s.startsWith("0o")) {
            radix = 8;
            s = s.substring(2);
        }
        if (negative) {
            s = "-" + s;
        }
        try {
            return Integer.valueOf(s, radix);
        } catch (NumberFormatException overflow) {
            return Long.valueOf(s, radix);
        }
    }

    /** Builds an error with code {@code code} and named args, positioned at {@code at} (or unlocated). */
    static DslException error(DslError code, String path, Node at, Map<String, Object> args) {
        Mark mark = at == null ? null : at.getStartMark();
        int line = mark == null ? 0 : mark.getLine() + 1;
        int column = mark == null ? 0 : mark.getColumn() + 1;
        return new DslException(code, path, line, column, null, args);
    }

    /** Returns {@code node} as a scalar's value, or errors positioned at the node. */
    static String requireScalar(Node node, String path) {
        if (!(node instanceof ScalarNode sc)) {
            throw error(DslError.ILLEGAL_VALUE, path, node,
                    Map.of("value", nodeTypeName(node), "expected", "a scalar"));
        }
        return sc.getValue();
    }

    /** Returns {@code node} as a located mapping, or errors positioned at the node. */
    static YamlMap requireMapping(Node node, String path) {
        if (!(node instanceof MappingNode mn)) {
            throw error(DslError.ILLEGAL_VALUE, path, node,
                    Map.of("value", nodeTypeName(node), "expected", "a mapping"));
        }
        return of(mn, path);
    }

    static String nodeTypeName(Node n) {
        if (n instanceof ScalarNode) {
            return "a scalar";
        }
        if (n instanceof MappingNode) {
            return "a mapping";
        }
        if (n instanceof SequenceNode) {
            return "a list";
        }
        return "unknown";
    }
}
