package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.Map;

/** Documents the project cases apply: a read source, a pipeline over it, and a write target. */
final class ProjectDocuments {

    private ProjectDocuments() {
    }

    static String source(String id) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: 10.30.0.5, database: orders, username: u, password: p }
                mode: cdc
                tables: [ orders ]
                """.formatted(id);
    }

    static String pipeline(String id, String source) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                view:
                  id: %s_view
                  from: orders
                  primary_key: id
                """.formatted(id, source, id);
    }

    /** A pipeline that writes through {@code target}, a connection it refers to and does not declare. */
    static String pipelineWritingTo(String id, String source, String target) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                serve:
                  from: orders
                  sync: [ { id: out, source: %s, write_mode: upsert } ]
                """.formatted(id, source, target);
    }

    /** A write target, standing in for the connection a cluster is created with. */
    static String target(String id) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "mongodb://10.30.0.11:27017/ods" }
                """.formatted(id);
    }

    /** Documents keyed by the file name each would have, in the order given. */
    static Map<String, String> batch(String... idsAndDocuments) {
        Map<String, String> batch = new LinkedHashMap<>();
        for (int i = 0; i < idsAndDocuments.length; i += 2) {
            batch.put(idsAndDocuments[i] + ".tap.yml", idsAndDocuments[i + 1]);
        }
        return batch;
    }

    /** The project label a stored artifact carries, read from its canonical form; null when it has none. */
    static String projectOf(ControlPlane control, String id) {
        String canonical = control.artifact(id).orElseThrow().canonicalForm();
        return io.tapstate.core.dsl.ProjectLabel.of(new io.tapstate.core.dsl.DslParser().parse(canonical));
    }
}
