package io.tapstate.e2e;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Prior material constraints for bounded report preparation; observed output size never defines its own cap. */
final class BenchmarkReturnWorkBudgetDefinitions {
    private BenchmarkReturnWorkBudgetDefinitions() { }

    record OutputPlan(long nodeUpper, int depthUpper, int leafUtf8Upper, long stringUtf8Upper,
                      int keyUtf8Upper, int numberTextUpper) {
        OutputPlan {
            require(nodeUpper > 0 && depthUpper > 0 && depthUpper <= 32 && leafUtf8Upper > 0
                            && stringUtf8Upper > 0 && keyUtf8Upper > 0 && numberTextUpper >= 24,
                    "prior output material bounds are missing");
            Math.multiplyExact(4L, leafUtf8Upper);
            Math.addExact(Math.multiplyExact(nodeUpper, Math.addExact(8L, numberTextUpper)),
                    Math.multiplyExact(6L, stringUtf8Upper));
        }

        long encodedUpperBytes() {
            long structuralAndNumbers = Math.multiplyExact(nodeUpper, Math.addExact(8L, numberTextUpper));
            return Math.addExact(structuralAndNumbers, Math.multiplyExact(6L, stringUtf8Upper));
        }
    }

    record CheckedTree(long nodes, long stringUtf8Bytes, long primitiveElements, long transientUtf8CopyUpperBytes) { }
    record FrozenTree(Object value, CheckedTree work) { }

    /** Validate the supplied bounded tree before a JSON writer is allowed to allocate its output buffer. */
    static CheckedTree requireOutputTree(Object tree, OutputPlan prior) {
        require(prior != null, "prior output plan is missing");
        var check = new TreeCheck(prior, false);
        try { check.value(tree, 1); }
        catch (ArithmeticException overflow) { throw new AssertionError("return work output material arithmetic overflowed", overflow); }
        return new CheckedTree(check.nodes, check.strings, check.primitives, check.copyUpper);
    }

    /** The encoder consumes this checked immutable copy even if a caller later changes its original containers. */
    static FrozenTree freezeOutputTree(Object tree, OutputPlan prior) {
        require(prior != null, "prior output plan is missing");
        var check = new TreeCheck(prior, true);
        try {
            Object frozen = check.value(tree, 1);
            return new FrozenTree(frozen, new CheckedTree(check.nodes, check.strings, check.primitives, check.copyUpper));
        } catch (ArithmeticException overflow) {
            throw new AssertionError("return work output material arithmetic overflowed", overflow);
        }
    }

    private static final class TreeCheck {
        private final OutputPlan plan;
        private final boolean freeze;
        private final IdentityHashMap<Object, Boolean> parents = new IdentityHashMap<>();
        private long nodes, strings, primitives, copyUpper;
        private TreeCheck(OutputPlan plan, boolean freeze) { this.plan = plan; this.freeze = freeze; }

        private void node(int depth) {
            nodes = Math.incrementExact(nodes);
            require(nodes <= plan.nodeUpper() && depth <= plan.depthUpper(), "output tree exceeds its prior node or depth bound");
        }

        private Object value(Object value, int depth) {
            node(depth);
            if (value == null || value instanceof Boolean) { primitives = Math.incrementExact(primitives); return value; }
            if (value instanceof String text) { text(text, plan.leafUtf8Upper()); primitives = Math.incrementExact(primitives); return value; }
            if (value instanceof Number number) { number(number); primitives = Math.incrementExact(primitives); return value; }
            if (value instanceof Map<?, ?> map) {
                require(map.size() <= plan.nodeUpper() - nodes, "output map exceeds the remaining prior node bound");
                enter(map);
                var copy = freeze ? new java.util.LinkedHashMap<String, Object>() : null;
                try {
                    for (var entry : map.entrySet()) {
                        node(depth + 1);
                        require(entry.getKey() instanceof String, "output map key is not a bounded string");
                        text((String) entry.getKey(), plan.keyUtf8Upper());
                        Object child = value(entry.getValue(), depth + 1);
                        if (freeze) { copy.put((String) entry.getKey(), child); }
                    }
                } finally { parents.remove(map); }
                return freeze ? java.util.Collections.unmodifiableMap(copy) : map;
            }
            if (value instanceof List<?> list) {
                require(list.size() <= plan.nodeUpper() - nodes, "output list exceeds the remaining prior node bound");
                enter(list);
                var copy = freeze ? new java.util.ArrayList<Object>() : null;
                try { for (Object item : list) { Object child = value(item, depth + 1); if (freeze) { copy.add(child); } } }
                finally { parents.remove(list); }
                return freeze ? java.util.Collections.unmodifiableList(copy) : list;
            }
            throw new AssertionError("return work output contains an unsupported JSON value");
        }

        private void enter(Object container) {
            require(parents.put(container, true) == null, "output tree contains a cycle");
        }

        private void text(String value, int limit) {
            require(value.length() <= limit, "output text exceeds its prior length bound");
            copyUpper = Math.addExact(copyUpper, Math.multiplyExact(4L, value.length()));
            long bytes = value.getBytes(StandardCharsets.UTF_8).length;
            require(bytes <= limit, "output text exceeds its prior UTF-8 bound");
            strings = Math.addExact(strings, bytes);
            require(strings <= plan.stringUtf8Upper(), "output text exceeds its prior aggregate byte bound");
        }

        private void number(Number value) {
            require(java.util.Set.of(Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
                            BigInteger.class, BigDecimal.class).contains(value.getClass()),
                    "output number has an unsupported implementation");
            if (value instanceof Double number) { require(Double.isFinite(number), "output number is not finite"); }
            else if (value instanceof Float number) { require(Float.isFinite(number), "output number is not finite"); }
            else if (value instanceof BigInteger number) { require(number.bitLength() <= 64, "output integer exceeds its prior numeric shape"); }
            else if (value instanceof BigDecimal number) {
                require(number.precision() <= plan.numberTextUpper() && Math.abs((long) number.scale()) <= plan.numberTextUpper(),
                        "output decimal exceeds its prior numeric shape");
            } else {
                require(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long,
                        "output number has an unsupported implementation");
            }
            require(value.toString().length() <= plan.numberTextUpper(), "output numeric text exceeds its prior bound");
        }
    }

    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return work output " + reason); }
    }
}
