package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** An unsupported view schema policy is refused at the shipped authoring boundary. */
class AViewSchemaPolicyIsRefusedByTheShippedCliIT {

    @Test
    void validationRefusesThePolicyWithItsCodedFieldPath(@TempDir Path workspace) {
        write(workspace.resolve("source.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: orders_src
                connector: mongodb
                config: { uri: "mongodb://127.0.0.1/orders" }
                mode: cdc
                tables: [ orders ]
                """);
        write(workspace.resolve("orders.tap.yml"), """
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: orders_src
                view:
                  id: order_state
                  from: orders
                  primary_key: order_id
                  schema:
                    enforce: true
                    evolution: additive
                """);

        CliOnce.Run run = CliOnce.run("validate", workspace.toString());

        assertThat(run.exitCode()).as("validate must refuse, so it cannot exit 0").isNotZero();
        assertThat(run.stdout() + run.stderr())
                .contains("dsl.unknown-field")
                .contains("view.schema");
    }

    private static void write(Path file, String content) {
        try {
            Files.writeString(file, content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
