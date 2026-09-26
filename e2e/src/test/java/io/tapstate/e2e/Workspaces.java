package io.tapstate.e2e;

import java.nio.file.Path;

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
