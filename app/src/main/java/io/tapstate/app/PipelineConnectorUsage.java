package io.tapstate.app;

import io.tapstate.control.core.ConnectorUsageQuery;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.PipelineDraftStore;
import io.tapstate.spi.store.PipelineIntentStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Reads effective CONFIG bindings without opening JARs or exposing recovery note contents. */
final class PipelineConnectorUsage implements ConnectorUsageQuery {
    private final PipelineIntentStore intents;
    private final PipelineDraftStore drafts;
    private final Runnable active;

    PipelineConnectorUsage(PipelineIntentStore intents, PipelineDraftStore drafts, Runnable active) {
        this.intents = Objects.requireNonNull(intents);
        this.drafts = Objects.requireNonNull(drafts);
        this.active = Objects.requireNonNull(active);
    }

    @Override public List<Version> forPipeline(String pipelineId) {
        active.run();
        var original = intents.load(pipelineId);
        if (original.isEmpty() || original.orElseThrow().deleted()) return List.of();
        var current = intents.connectorBindings().load(original.orElseThrow().scope());
        active.run();
        return current.map(value -> versions(value.effective())).orElseGet(List::of);
    }

    @Override public Page usages(String connectorId, String sha256, String search, int page, int pageSize) {
        if (connectorId == null || connectorId.isBlank() || sha256 == null || !sha256.matches("[a-f0-9]{64}"))
            throw malformed("a connector id and lowercase SHA-256 are required");
        if (page < 1 || pageSize < 1 || pageSize > 100) throw malformed("page must be positive and pageSize between 1 and 100");
        active.run();
        var bindings = intents.connectorBindings().loadAll();
        var names = new HashMap<String, String>();
        for (var draft : drafts.listSummaries())
            if (draft.name() != null && !draft.name().isBlank()) names.put(draft.pipelineId(), draft.name());
        String query = search == null ? "" : search.strip().toLowerCase(Locale.ROOT);
        var rows = new ArrayList<Pipeline>();
        for (var entry : bindings.entrySet()) {
            var current = entry.getValue().effective().stream()
                    .filter(value -> value.connectorId().equals(connectorId) && value.sha256().equals(sha256)).toList();
            if (current.isEmpty()) continue;
            String id = entry.getKey(), name = names.getOrDefault(id, id);
            if (!id.toLowerCase(Locale.ROOT).contains(query) && !name.toLowerCase(Locale.ROOT).contains(query)) continue;
            rows.add(new Pipeline(id, name, versions(current)));
        }
        rows.sort(Comparator.comparing(Pipeline::pipelineId));
        long offset = (long) (page - 1) * pageSize;
        List<Pipeline> selected = offset >= rows.size() ? List.of()
                : rows.subList((int) offset, (int) Math.min(rows.size(), offset + pageSize));
        active.run();
        return new Page(page, pageSize, rows.size(), selected);
    }

    private static List<Version> versions(List<ConnectorRunBindings.Binding> bindings) {
        return bindings.stream().map(value -> new Version(value.nodeId(), value.purpose(), value.connectorId(),
                value.version(), value.sha256(), value.captureId(), value.chainId()))
                .sorted(Comparator.comparing(Version::purpose).thenComparing(Version::nodeId)).toList();
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }
}
