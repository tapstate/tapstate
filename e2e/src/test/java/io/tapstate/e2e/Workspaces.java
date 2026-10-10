package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * The resource documents the removal cases apply, written once so that two cases asserting different
 * things about the same arrangement are actually asserting them about the same arrangement.
 *
 * <p>They are the harness's own connector over directories, which is what lets these cases run in an
 * ordinary build: whether a removal is refused, and what it reclaims, are control-plane facts that no real
 * database is needed to demonstrate.
 */
final class Workspaces {

    private Workspaces() {
    }

    /**
     * A cdc source over a directory. A cdc read does not end, which every case that needs a pipeline to
     * still be running when it gets there depends on.
     */
    static String cdcSourceYaml(String id, Path uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, uri);
    }

    /**
     * A cdc source over a directory, reading {@code tables}, with the harness connector's own test settings -
     * a {@code hold} directory, or a {@code read_witness} one - beside the address.
     */
    static String cdcSourceYaml(String id, Path uri, List<String> tables, Map<String, String> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, config(uri, settings), String.join(", ", tables));
    }

    /**
     * A sink endpoint with the harness connector's own test settings beside the address - a {@code write_witness}
     * directory, or a {@code hold} directory.
     */
    static String targetYaml(String id, Path uri, Map<String, String> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { %s }
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, config(uri, settings));
    }

    private static String config(Path uri, Map<String, String> settings) {
        StringBuilder config = new StringBuilder("uri: \"" + uri + "\"");
        settings.forEach((name, value) -> config.append(", ").append(name).append(": \"").append(value).append('"'));
        return config.toString();
    }

    /** A sink endpoint: an address and nothing else, since nothing is read from it. */
    static String targetYaml(String id, Path uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, uri);
    }

    /**
     * A pipeline carrying every change from one source to one target. The filter admits everything - these
     * cases are about removal, and a predicate that dropped rows would only add a second reason for a count
     * not to move.
     */
    static String pipelineYaml(String pipelineId, String sourceId, String targetId, String table) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: %s_step, from: [ %s ], type: filter, expr: "op != 'x'" }
                serve:
                  from: %s_step
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, sourceId, pipelineId, table, pipelineId, targetId);
    }

    /** A pipeline carrying every change of {@code tables} from one source to one target, each into its own table. */
    static String pipelineYaml(String pipelineId, String sourceId, String targetId, List<String> tables) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: %s_step, from: [ %s ], type: filter, expr: "op != 'x'" }
                serve:
                  from: %s_step
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, sourceId, pipelineId, String.join(", ", tables), pipelineId, targetId);
    }

    /**
     * A pipeline carrying every change of {@code tables} from several sources to one target, each into its own table.
     * Every table name has to belong to one of the sources alone, since a bare name is resolved across all of them.
     */
    static String pipelineYaml(String pipelineId, List<String> sourceIds, String targetId, List<String> tables) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ %s ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: %s_step, from: [ %s ], type: filter, expr: "op != 'x'" }
                serve:
                  from: %s_step
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, String.join(", ", sourceIds), pipelineId, String.join(", ", tables),
                        pipelineId, targetId);
    }

    /**
     * The same pipeline with its target written by {@code writers} writers across the cluster, for a case whose
     * subject is how wide a run is planned and where its writers run.
     */
    static String pipelineYaml(String pipelineId, String sourceId, String targetId, String table, int writers) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: %s_step, from: [ %s ], type: filter, expr: "op != 'x'" }
                serve:
                  from: %s_step
                  sync:
                    - source: %s
                      execution: { parallelism: %d }
                """
                .formatted(pipelineId, sourceId, pipelineId, table, pipelineId, targetId, writers);
    }
}
