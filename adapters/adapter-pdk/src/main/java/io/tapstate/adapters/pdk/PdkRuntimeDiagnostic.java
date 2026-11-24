package io.tapstate.adapters.pdk;

import io.tapstate.core.catalog.SecretConfigPaths;
import io.tapstate.core.catalog.SpecNormalizer;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.RuntimeDiagnosticProjection;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.PipelineNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** An admitted connector handle's immutable artifact and secret context; never a global failure index. */
final class PdkRuntimeDiagnostic {
    private static final PdkRuntimeDiagnostic UNAVAILABLE = new PdkRuntimeDiagnostic(null, List.of(), null, false);
    private final PdkDiagnosticCatalog catalog;
    private final List<String> secrets;
    private final PipelineNode node;
    private final boolean source;

    private PdkRuntimeDiagnostic(PdkDiagnosticCatalog catalog, List<String> secrets, PipelineNode node, boolean source) {
        this.source = source;
        this.catalog = catalog;
        this.secrets = List.copyOf(secrets);
        this.node = node;
    }

    static PdkRuntimeDiagnostic bind(String connectorId, ConnectorRef ref, Map<String, Object> settings, PipelineNode node) {
        return bind(connectorId, ref, settings, node, PipelineNode.Purpose.SYNC);
    }

    static PdkRuntimeDiagnostic bind(String connectorId, ConnectorRef ref, Map<String, Object> settings, PipelineNode node,
            PipelineNode.Purpose purpose) {
        try {
            var values = new ArrayList<String>();
            Object parsed = JsonReader.parse(ref.spec());
            if (!(parsed instanceof Map<?, ?> raw)) return UNAVAILABLE;
            var spec = new LinkedHashMap<String, Object>();
            raw.forEach((key, value) -> spec.put((String) key, value));
            var normalized = SpecNormalizer.normalize(spec);
            normalized.config().stream().filter(field -> field.secret())
                    .forEach(field -> scalars(settings.get(field.name()), values, 0));
            SecretConfigPaths.shared().forEach(path -> scalars(path.value(settings), values, 0));
            // URI credentials and injected trust material need protection even when omitted by the form.
            for (var entry : settings.entrySet()) {
                if (entry.getValue() instanceof String text) {
                    values.addAll(MongoUriUserInfo.secretValues(text));
                    if (entry.getKey().toLowerCase(Locale.ROOT).contains("bundle")) scalars(text, values, 0);
                }
            }
            return new PdkRuntimeDiagnostic(PdkDiagnosticCatalog.load(connectorId, ref), values, node, purpose == PipelineNode.Purpose.CAPTURE);
        } catch (RuntimeException | LinkageError unavailable) {
            return UNAVAILABLE;
        }
    }

    io.tapstate.core.common.FailureFact failureFact(Throwable failure) {
        try {
            return captureFact(failure);
        } catch (RuntimeException | LinkageError unavailable) {
            return io.tapstate.core.common.FailureFact.unavailable(failure);
        }
    }

    private io.tapstate.core.common.FailureFact captureFact(Throwable failure) {
        io.tapstate.core.common.FailureFact.PublicFailure publicFailure =
                new io.tapstate.core.common.FailureFact.PublicFailure("", Map.of());
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 32; depth++) {
            if (current instanceof io.tapstate.core.common.TapstateException coded) {
                var params = new LinkedHashMap<String, String>();
                coded.args().forEach((key, value) -> {
                    String safe;
                    try {
                        safe = catalog == null ? "[diagnostic unavailable]"
                                : SecretRedactor.redactConnectionDiagnostic(String.valueOf(value), secrets);
                        int newline = safe.indexOf('\n');
                        boolean cut = newline >= 0 || safe.length() > 200;
                        if (newline >= 0) safe = safe.substring(0, newline);
                        if (safe.length() > 200) safe = safe.substring(0, 200);
                        if (cut) safe += " …";
                    } catch (RuntimeException | LinkageError unavailable) { safe = "[diagnostic unavailable]"; }
                    params.put(key, safe);
                });
                publicFailure = new io.tapstate.core.common.FailureFact.PublicFailure(coded.code().code(), params);
                break;
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return new io.tapstate.core.common.FailureFact(failure, capture(failure), publicFailure);
    }

    Map<String, Object> capture(Throwable failure) {
        if (catalog == null) return Map.of("truncated", true);
        try {
            String code = failure instanceof io.tapstate.core.common.TapstateException coded ? coded.code().code()
                    : failure instanceof io.tapdata.exception.TapCodeException coded ? coded.getCode() : null;
            var raw = PdkConnectionDiagnostic.capture(failure, null, null, code, catalog, Locale.ENGLISH);
            var safe = redact(raw);
            if (node != null) safe.put("references", List.of(source ? Map.of("type", "source", "id", node.nodeId())
                    : Map.of("type", "pipeline-node", "id", node.nodeId(), "pipelineId", node.pipelineId())));
            return RuntimeDiagnosticProjection.fit(safe);
        } catch (RuntimeException | LinkageError unavailable) {
            return Map.of("truncated", true);
        }
    }

    private Map<String, Object> redact(Map<?, ?> input) {
        var output = new LinkedHashMap<String, Object>();
        input.forEach((key, value) -> output.put((String) key, safe(value)));
        if (output.get("stackTrace") instanceof String stack && stack.lines().count() > 32) {
            output.put("stackTrace", stack.lines().limit(32).collect(java.util.stream.Collectors.joining("\n")));
            output.put("truncated", true);
        }
        return output;
    }

    private Object safe(Object value) {
        if (value instanceof String text) return SecretRedactor.redactConnectionDiagnostic(text, secrets);
        if (value instanceof Map<?, ?> fields) return redact(fields);
        if (value instanceof List<?> values) return values.stream().map(this::safe).toList();
        return value;
    }

    private static void scalars(Object value, List<String> target, int depth) {
        if (depth > 32 || target.size() > 1024) throw new IllegalArgumentException("secret context exceeds budget");
        if (value instanceof Map<?, ?> fields) fields.values().forEach(child -> scalars(child, target, depth + 1));
        else if (value instanceof Iterable<?> values) values.forEach(child -> scalars(child, target, depth + 1));
        else if (value != null) {
            String text = String.valueOf(value);
            target.add(text);
            if (text.indexOf('\n') >= 0) text.lines().filter(line -> !line.isBlank()).forEach(target::add);
        }
    }
}
