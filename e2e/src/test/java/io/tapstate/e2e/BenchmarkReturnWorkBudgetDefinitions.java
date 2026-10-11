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

        String sha256() {
            String value = "OUTPUT_TREE_PLAN_V1:" + nodeUpper + ":" + depthUpper + ":" + leafUtf8Upper + ":"
                    + stringUtf8Upper + ":" + keyUtf8Upper + ":" + numberTextUpper;
            try {
                var scope = BenchmarkReturnWorkBudget.current();
                if (scope != null) { scope.add(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, value.length(), 1, value.length(), 0)); }
                return BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        () -> java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                                .digest(value.getBytes(StandardCharsets.US_ASCII))));
            } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        }
    }

    record CheckedTree(long nodes, long stringUtf8Bytes, long primitiveElements, long transientUtf8CopyUpperBytes) { }
    record FrozenTree(Object value, CheckedTree work) { }

    /** Bound the lexical material before the recursive decoder allocates a nested tree. */
    static void requireJsonShape(String raw, int byteUpper, int depthUpper, int nodeUpper, int leafUpper) {
        require(raw != null && raw.length() <= byteUpper, "JSON exceeds its prior character bound");
        long bytes = 0;
        int depth = 0, nodes = 0, leaf = 0, scalar = 0;
        boolean quoted = false, escaped = false, inScalar = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            int utf8Upper = c <= 127 ? 1 : c <= 2047 ? 2
                    : Character.isHighSurrogate(c) && i + 1 < raw.length() && Character.isLowSurrogate(raw.charAt(i + 1)) ? 4
                    : Character.isLowSurrogate(c) && i > 0 && Character.isHighSurrogate(raw.charAt(i - 1)) ? 0 : 3;
            bytes = Math.addExact(bytes, utf8Upper);
            require(bytes <= byteUpper, "JSON exceeds its prior UTF-8 bound");
            if (quoted) {
                if (escaped) { escaped = false; }
                else if (c == '\\') { escaped = true; }
                else if (c == '"') { quoted = false; continue; }
                require(++leaf <= leafUpper, "JSON string exceeds its prior leaf bound");
                continue;
            }
            if (c == '"') {
                quoted = true; leaf = 0; inScalar = false;
                require(++nodes <= nodeUpper, "JSON exceeds its prior node bound");
            } else if (c == '{' || c == '[') {
                inScalar = false;
                require(++depth <= depthUpper && ++nodes <= nodeUpper, "JSON exceeds its prior shape bound");
            } else if (c == '}' || c == ']') {
                inScalar = false; require(--depth >= 0, "JSON shape is malformed");
            } else if (c == ',' || c == ':' || Character.isWhitespace(c)) {
                inScalar = false;
            } else {
                if (!inScalar) { require(++nodes <= nodeUpper, "JSON exceeds its prior node bound"); scalar = 0; inScalar = true; }
                require(++scalar <= 32, "JSON scalar exceeds its prior numeric or literal bound");
            }
        }
        require(!quoted && !escaped && depth == 0, "JSON shape is incomplete");
    }

    record ProducerInput(String runId, String inputSha256, BenchmarkReturnWorkBudget.Work independentlyRegisteredUpper,
                         long connectorHashBytes, int runtimeArguments, long runtimeArgumentBytes) {
        ProducerInput {
            require(runId != null && !runId.isEmpty() && runId.length() <= 128
                            && runId.chars().allMatch(c -> c >= 32 && c <= 126)
                            && inputSha256 != null && inputSha256.matches("[a-f0-9]{64}")
                            && independentlyRegisteredUpper != null, "producer input registration is missing");
            require(connectorHashBytes >= 1 && connectorHashBytes <= 128L * 1024 * 1024
                            && runtimeArguments >= 0 && runtimeArguments <= 128
                            && runtimeArgumentBytes >= 0 && runtimeArgumentBytes <= 2093056,
                    "producer material registration exceeds the source caps");
        }

        String encoded() {
            var input = new java.util.LinkedHashMap<String, Object>();
            input.put("schemaVersion", 1); input.put("definitionId", "WRITE_RETURN_PRODUCER_WORK_V1");
            input.put("sourceFormulaSha256", BenchmarkReturnWorkBudgetProducerSeal.FORMULA_SHA256);
            input.put("inputSha256", inputSha256); input.put("runId", runId);
            input.put("limits", independentlyRegisteredUpper.evidence());
            input.put("materialLimits", Map.of("connectorHashBytes", connectorHashBytes, "runtimeArguments", runtimeArguments,
                    "runtimeArgumentBytes", runtimeArgumentBytes, "primaryKeys", 2));
            var tree = freezeOutputTree(input, new OutputPlan(128, 4, 1024, 8192, 128, 24));
            byte[] raw = io.tapstate.core.common.JsonWriter.write(tree.value()).getBytes(StandardCharsets.US_ASCII);
            require(raw.length <= 16384, "producer input encoding exceeds its fixed transport cap");
            return java.util.Base64.getEncoder().encodeToString(raw);
        }
    }

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
