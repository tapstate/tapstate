package io.tapstate.core.dsl;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Lifts an already-structured document -- plain maps, lists and scalars, as a document store hands
 * them back -- into the node tree the parser binds from.
 *
 * <p>This exists so there is one binder rather than two. The alternative is a second mapping from
 * documents to the model, standing beside the parser's and agreeing with it only for as long as
 * someone keeps them in step; the way that fails is a field read back as null, which is
 * indistinguishable from a field that was never written.
 *
 * <p>Nodes built here carry no source marks, so diagnostics about them report no line or column. That
 * is the truth about a stored document: it has no line 12 to send anyone to, and the field path the
 * error already carries is the whole of what can be pointed at.
 */
final class TreeNodes {

    private TreeNodes() {
    }

    /** The document root as a mapping node; the path of a root-level key is the key itself. */
    static MappingNode mapping(Map<?, ?> map, String path) {
        List<NodeTuple> tuples = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = String.valueOf(entry.getKey());
            tuples.add(new NodeTuple(scalar(Tag.STR, key), of(entry.getValue(), child(path, key))));
        }
        return new MappingNode(Tag.MAP, tuples, DumperOptions.FlowStyle.BLOCK);
    }

    /**
     * One stored value as a node. Scalars are tagged by their Java type rather than re-resolved from
     * text, so a stored string that happens to read like a number stays a string -- the carrier already
     * knows which it is, and re-guessing would be the one place this path could disagree with the text
     * path it shares a binder with.
     */
    private static Node of(Object value, String path) {
        return switch (value) {
            case null -> scalar(Tag.NULL, "null");
            case String s -> scalar(Tag.STR, s);
            case Boolean b -> scalar(Tag.BOOL, b.toString());
            // Whole numbers and fractional ones read back differently, so the tag follows the value
            // rather than the class it arrived as: a store is free to hand an int back as a long, and
            // free to hold a decimal in a type of its own.
            case Number n -> scalar(isWhole(n) ? Tag.INT : Tag.FLOAT, n.toString());
            case Map<?, ?> m -> mapping(m, path);
            case List<?> l -> sequence(l, path);
            // Anything else is a value this product never writes, so a document holding one was
            // written by something else. Naming the field and the type it holds is the only useful
            // thing to say about it; guessing a conversion would store the guess.
            default -> throw YamlMap.error(DslError.ILLEGAL_VALUE, path, null,
                    Map.of("value", value.getClass().getSimpleName(),
                            "expected", "a scalar, a mapping or a list"));
        };
    }

    private static boolean isWhole(Number number) {
        return number instanceof Byte || number instanceof Short || number instanceof Integer
                || number instanceof Long || number instanceof java.math.BigInteger;
    }

    private static SequenceNode sequence(List<?> list, String path) {
        List<Node> items = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            items.add(of(list.get(i), path + "[" + i + "]"));
        }
        return new SequenceNode(Tag.SEQ, items, DumperOptions.FlowStyle.BLOCK);
    }

    private static ScalarNode scalar(Tag tag, String value) {
        return new ScalarNode(tag, value, null, null, DumperOptions.ScalarStyle.PLAIN);
    }

    private static String child(String path, String key) {
        return path.isEmpty() ? key : path + "." + key;
    }
}
