package io.tapstate.core.alerting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

/** Scoped root-cause notification inhibition. The host separately proves the current resource dependency. */
public final class AlertInhibitionPolicy {
    public record Edge(String rootType, String dependentType) {
        public Edge {
            rootType = type(rootType); dependentType = type(dependentType);
            AlertValidation.require(!rootType.equals(dependentType), "inhibition", "self-cycle");
        }
        private static String type(String value) {
            AlertValidation.text(value, "inhibitionType", 128);
            AlertValidation.require(value.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), "inhibitionType", "invalid-id");
            return value;
        }
    }
    private final AlarmKey.Scope scope;
    private final Set<Edge> edges;
    public AlertInhibitionPolicy(AlarmKey.Scope scope, List<Edge> definitions) {
        this.scope = Objects.requireNonNull(scope, "scope");
        AlertValidation.require(definitions != null && definitions.size() <= 64 && definitions.stream().allMatch(Objects::nonNull), "inhibition", "too-many-or-missing-edges");
        edges = Set.copyOf(definitions);
        AlertValidation.require(edges.size() == definitions.size(), "inhibition", "duplicate-edge");
        Map<String, List<String>> graph = new HashMap<>();
        for (Edge edge : edges) { graph.computeIfAbsent(edge.rootType(), ignored -> new ArrayList<>()).add(edge.dependentType()); }
        Set<String> seen = new HashSet<>();
        for (String node : graph.keySet()) { visit(node, graph, seen, new HashSet<>()); }
    }
    public boolean suppresses(AlertSnapshot root, AlertSnapshot dependent, boolean dependencyCurrentlyAuthorized) {
        return dependencyCurrentlyAuthorized && scope.equals(root.rule().key().scope()) && scope.equals(dependent.rule().key().scope())
                && !root.rule().key().equals(dependent.rule().key())
                && !root.rule().key().typeId().equals(dependent.rule().key().typeId()) && active(root) && active(dependent)
                && root.quality() == AlertSnapshot.Quality.VALID
                && edges.contains(new Edge(root.rule().key().typeId(), dependent.rule().key().typeId()));
    }
    private static boolean active(AlertSnapshot snapshot) {
        return snapshot.enabled() && snapshot.status() == AlertSnapshot.Status.FIRING && snapshot.episode() != null && snapshot.episode().endedAt() == null;
    }
    private static void visit(String node, Map<String, List<String>> graph, Set<String> seen, Set<String> visiting) {
        if (seen.contains(node)) { return; }
        AlertValidation.require(visiting.add(node), "inhibition", "cycle");
        for (String child : graph.getOrDefault(node, List.of())) { visit(child, graph, seen, visiting); }
        visiting.remove(node); seen.add(node);
    }
}
