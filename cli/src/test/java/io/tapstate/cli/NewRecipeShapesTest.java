package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three recipes that shape what they mirror: {@code reshaped-table} (a map and a filter step),
 * {@code nested-json} (one nest step over several tables) and {@code consolidated-table} (one union
 * step over the same table in several databases). Each is held to inline goldens written from
 * {@code docs/first-run/README.md} and the canonical writer's own samples, to the rules the simpler
 * recipes already keep - secrets in {@code .env}, no overwrite without {@code --force}, the flag and
 * the interactive form producing the same bytes, a missing required answer refused by naming its
 * flag - and to one more: every workspace they write passes {@code validate} as it stands.
 */
class NewRecipeShapesTest {

    private static final String ORDERS_SRC =
            """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config:
              host: db
              password: ${ORDERS_SRC_PASSWORD}
              port: "3306"
              username: u
            mode: cdc
            tables: [orders]
            """;

    private static final String RESHAPED_SYNC =
            """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: orders_src
            transforms:
              - id: reshape
                type: map
                from: [orders]
                fields:
                  id: $id
                  region: $region
                  total: $amount
                  internal_note: false
              - id: keep
                type: filter
                from: [reshape]
                expr: "after.region == 'US'"
            view:
              id: orders_view
              from: keep
              primary_key: id
            """;

    private static final String NESTED_SRC =
            """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config:
              host: db
              password: ${ORDERS_SRC_PASSWORD}
              port: "3306"
              username: u
            mode: cdc
            tables: [orders, shipments]
            """;

    private static final String NESTED_SYNC =
            """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: orders_src
            transforms:
              - id: assemble
                type: nest
                from:
                  orders: orders
                  shipments: shipments
                root:
                  from: orders
                  key: [id]
                  embed:
                    - from: shipments
                      on:
                        order_id: id
                      as: array
                      path: shipments
                      arrayKey: [id]
            view:
              id: orders_state
              from: assemble
              primary_key: id
            """;

    private static final String CONSOLIDATED_SRC_1 =
            """
            version: tapstate/v1
            kind: source
            id: orders_1_src
            connector: mysql
            config:
              host: db1
              password: ${ORDERS_1_SRC_PASSWORD}
              port: "3306"
              username: u
            mode: cdc
            tables: [orders]
            """;

    private static final String CONSOLIDATED_SRC_2 =
            """
            version: tapstate/v1
            kind: source
            id: orders_2_src
            connector: mysql
            config:
              host: db2
              password: ${ORDERS_2_SRC_PASSWORD}
              port: "3306"
              username: u
            mode: cdc
            tables: [orders]
            """;

    private static final String CONSOLIDATED_SYNC =
            """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: [orders_1_src, orders_2_src]
            transforms:
              - id: consolidate
                type: union
                from: [orders_1_src.orders, orders_2_src.orders]
            view:
              id: orders_all
              from: consolidate
              primary_key: id
            """;

    private static NewRecipeTest.Run run(Path home, Prompter prompter, String... args) {
        return NewRecipeTest.run(home, prompter, args);
    }

    private static NewRecipeTest.Run validate(Path home, Path ws) {
        return run(home, new ScriptedPrompter(), "validate", ws.toString());
    }

    private static String[] with(List<String> base, String... extra) {
        List<String> args = new ArrayList<>(base);
        args.addAll(List.of(extra));
        return args.toArray(String[]::new);
    }

    private static final List<String> MYSQL_DB = List.of(
            "--connector", "mysql", "--set", "host=db", "--set", "username=u", "--set", "password=s");

    private static void assertSameFiles(Path a, Path b, String... files) throws IOException {
        for (String file : files) {
            assertThat(Files.readString(a.resolve(file))).as(file).isEqualTo(Files.readString(b.resolve(file)));
        }
    }

    // ---- reshaped-table -------------------------------------------------------------------------

    private static NewRecipeTest.Run reshaped(Path home, Path ws, String... extra) {
        List<String> args = new ArrayList<>(List.of("new", "reshaped-table", "--yes"));
        args.addAll(MYSQL_DB);
        args.addAll(List.of("--table", "orders", "--view", "orders_view",
                "--keep", "id,region,amount", "--rename", "amount=total", "--drop", "internal_note",
                "--where", "after.region == 'US'", "-w", ws.toString()));
        return run(home, new ScriptedPrompter(), with(args, extra));
    }

    @Test
    void reshapedTableWritesTheMapAndTheFilterStepsToTheGoldens(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        NewRecipeTest.Run r = reshaped(home, ws);

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(Files.readString(ws.resolve("source/orders_src.tap.yml"))).isEqualTo(ORDERS_SRC);
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(RESHAPED_SYNC);
        assertThat(Files.readString(ws.resolve(".env"))).isEqualTo("ORDERS_SRC_PASSWORD=s\n");
        assertThat(Files.readString(ws.resolve(".gitignore"))).isEqualTo(".env\n");
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n"
                + "  source/orders_src.tap.yml  source orders_src: mysql, cdc\n"
                + "  pipeline/orders_sync.tap.yml  pipeline orders_sync: 1 source, view — assumed primary_key: id;");
    }

    /**
     * The scripted answers, in the order the questions come: the four {@code mirrored-table} asks
     * (connector, host, port as its default, database skipped, username, password, table, view), then
     * the columns to keep, the renames, the columns to drop, the row filter.
     */
    @Test
    void interactiveReshapedTableProducesTheSameFilesAsTheFlagForm(@TempDir Path home, @TempDir Path ws,
                                                                   @TempDir Path flags) throws IOException {
        ScriptedPrompter prompter = new ScriptedPrompter(
                "mysql", "db", "", "", "u", "s", "orders", "orders_view",
                "id, region, amount", "amount=total", "internal_note", "after.region == 'US'");

        NewRecipeTest.Run r = run(home, prompter, "new", "reshaped-table", "--server", "http://127.0.0.1:8080", "--user", "admin",
                "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.secretQuestions).hasSize(1);
        assertThat(reshaped(home, flags).code()).isZero();
        assertSameFiles(ws, flags, "source/orders_src.tap.yml", "pipeline/orders_sync.tap.yml", ".env", ".gitignore");
    }

    @Test
    void whatReshapedTableWritesValidates(@TempDir Path home, @TempDir Path ws) {
        assertThat(reshaped(home, ws).code()).isZero();

        NewRecipeTest.Run validated = validate(home, ws);

        assertThat(validated.code()).as(validated.all()).isZero();
        assertThat(validated.out()).contains("2 resources");
    }

    /** No reshaping answered is a mirrored table: no step, and the view reads the table itself. */
    @Test
    void reshapedTableWithNothingToReshapeIsAMirroredTable(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), with(List.of("new", "reshaped-table", "--yes"),
                "--set", "host=db", "--table", "orders", "--view", "orders_view", "-w", ws.toString()));

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: orders_sync
                source: orders_src
                view:
                  id: orders_view
                  from: orders
                  primary_key: id
                """);
    }

    /** A filter alone reads the table directly: there is no map step for it to follow. */
    @Test
    void reshapedTableWithOnlyAFilterReadsTheTable(@TempDir Path home, @TempDir Path ws) throws IOException {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), with(List.of("new", "reshaped-table", "--yes"),
                "--set", "host=db", "--table", "orders", "--where", "after.amount > 0", "-w", ws.toString()));

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: orders_sync
                source: orders_src
                transforms:
                  - id: keep
                    type: filter
                    from: [orders]
                    expr: "after.amount > 0"
                view:
                  id: orders
                  from: keep
                  primary_key: id
                """);
        assertThat(validate(home, ws).code()).isZero();
    }

    @Test
    void reshapedTableMissingTableIsAUsageErrorNamingTheFlag(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), "new", "reshaped-table", "--yes",
                "--set", "host=db", "--keep", "id", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("--table");
        assertThat(ws.resolve("source")).doesNotExist();
    }

    // ---- nested-json ----------------------------------------------------------------------------

    private static NewRecipeTest.Run nested(Path home, Path ws, String... extra) {
        List<String> args = new ArrayList<>(List.of("new", "nested-json", "--yes"));
        args.addAll(MYSQL_DB);
        args.addAll(List.of("--root", "orders", "--child", "shipments:order_id=id", "-w", ws.toString()));
        return run(home, new ScriptedPrompter(), with(args, extra));
    }

    @Test
    void nestedJsonWritesOneSourceForTwoTablesOfTheSameDatabase(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        NewRecipeTest.Run r = nested(home, ws);

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(Files.readString(ws.resolve("source/orders_src.tap.yml"))).isEqualTo(NESTED_SRC);
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(NESTED_SYNC);
        assertThat(Files.readString(ws.resolve(".env"))).isEqualTo("ORDERS_SRC_PASSWORD=s\n");
        assertThat(Files.readString(ws.resolve(".gitignore"))).isEqualTo(".env\n");
        assertThat(ws.resolve("source/shipments_src.tap.yml")).doesNotExist();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n"
                + "  source/orders_src.tap.yml  source orders_src: mysql, cdc\n"
                + "  pipeline/orders_sync.tap.yml  pipeline orders_sync: 1 source, view — assumed arrayKey: [id];");
    }

    /**
     * The scripted answers: the root's connector and connection, the root table, its key (default),
     * then one child - same database (default yes), table, the join columns, one-to-many, path
     * (default), no further table - then the view id (default).
     */
    @Test
    void interactiveNestedJsonProducesTheSameFilesAsTheFlagForm(@TempDir Path home, @TempDir Path ws,
                                                                @TempDir Path flags) throws IOException {
        ScriptedPrompter prompter = new ScriptedPrompter(
                "mysql", "db", "", "", "u", "s", "orders", "",
                "", "shipments", "order_id=id", "array", "", "",
                "");

        NewRecipeTest.Run r = run(home, prompter, "new", "nested-json", "--server", "http://127.0.0.1:8080", "--user", "admin",
                "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.secretQuestions).as("the child reuses the root's connection").hasSize(1);
        assertThat(nested(home, flags).code()).isZero();
        assertSameFiles(ws, flags, "source/orders_src.tap.yml", "pipeline/orders_sync.tap.yml", ".env", ".gitignore");
    }

    /**
     * Two children, one sharing the root's database and one not: the first joins the root's source
     * as a second table, the second gets a source of its own, and the pipeline reads both.
     */
    @Test
    void interactiveNestedJsonSplitsSourcesByDatabase(@TempDir Path home, @TempDir Path ws) throws IOException {
        ScriptedPrompter prompter = new ScriptedPrompter(
                "mysql", "db", "", "", "u", "s", "orders", "order_no",
                "", "shipments", "order_id=order_no", "array", "", "y",
                "n", "mysql", "db2", "", "", "u2", "s2", "invoices", "order_id=order_no", "object", "invoice", "",
                "");

        NewRecipeTest.Run r = run(home, prompter, "new", "nested-json", "--server", "http://127.0.0.1:8080", "--user", "admin",
                "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.secretQuestions).hasSize(2);
        assertThat(Files.readString(ws.resolve("source/orders_src.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: orders_src
                connector: mysql
                config:
                  host: db
                  password: ${ORDERS_SRC_PASSWORD}
                  port: "3306"
                  username: u
                mode: cdc
                tables: [orders, shipments]
                """);
        assertThat(Files.readString(ws.resolve("source/invoices_src.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: invoices_src
                connector: mysql
                config:
                  host: db2
                  password: ${INVOICES_SRC_PASSWORD}
                  port: "3306"
                  username: u2
                mode: cdc
                tables: [invoices]
                """);
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: orders_sync
                source: [orders_src, invoices_src]
                transforms:
                  - id: assemble
                    type: nest
                    from:
                      invoices: invoices
                      orders: orders
                      shipments: shipments
                    root:
                      from: orders
                      key: [order_no]
                      embed:
                        - from: shipments
                          on:
                            order_id: order_no
                          as: array
                          path: shipments
                          arrayKey: [id]
                        - from: invoices
                          on:
                            order_id: order_no
                          as: object
                          path: invoice
                view:
                  id: orders_state
                  from: assemble
                  primary_key: order_no
                """);
        assertThat(Files.readString(ws.resolve(".env")))
                .isEqualTo("ORDERS_SRC_PASSWORD=s\nINVOICES_SRC_PASSWORD=s2\n");
        assertThat(validate(home, ws).code()).isZero();
    }

    /** The flag form names one other database, and every child goes there. */
    @Test
    void nestedJsonFlagFormPutsChildrenOnTheOtherDatabaseWhenOneIsNamed(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        NewRecipeTest.Run r = nested(home, ws, "--child-connector", "mysql", "--child-set", "host=db2",
                "--child-set", "username=u2", "--child-set", "password=s2");

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(Files.readString(ws.resolve("source/orders_src.tap.yml"))).isEqualTo(ORDERS_SRC);
        assertThat(Files.readString(ws.resolve("source/shipments_src.tap.yml"))).isEqualTo(
                """
                version: tapstate/v1
                kind: source
                id: shipments_src
                connector: mysql
                config:
                  host: db2
                  password: ${SHIPMENTS_SRC_PASSWORD}
                  port: "3306"
                  username: u2
                mode: cdc
                tables: [shipments]
                """);
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml")))
                .contains("source: [orders_src, shipments_src]\n");
        assertThat(Files.readString(ws.resolve(".env")))
                .isEqualTo("ORDERS_SRC_PASSWORD=s\nSHIPMENTS_SRC_PASSWORD=s2\n");
        assertThat(validate(home, ws).code()).isZero();
    }

    @Test
    void whatNestedJsonWritesValidates(@TempDir Path home, @TempDir Path ws) {
        assertThat(nested(home, ws).code()).isZero();

        NewRecipeTest.Run validated = validate(home, ws);

        assertThat(validated.code()).as(validated.all()).isZero();
        assertThat(validated.out()).contains("2 resources");
    }

    @Test
    void nestedJsonMissingRootIsAUsageErrorNamingTheFlag(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), "new", "nested-json", "--yes",
                "--set", "host=db", "--child", "shipments:order_id=id", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("--root");
        assertThat(ws.resolve("source")).doesNotExist();
    }

    @Test
    void nestedJsonMissingChildIsAUsageErrorNamingTheFlag(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), "new", "nested-json", "--yes",
                "--set", "host=db", "--root", "orders", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("--child");
        assertThat(ws.resolve("source")).doesNotExist();
    }

    @Test
    void nestedJsonJsonListsTheFilesWithTheirKinds(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = nested(home, ws, "-o", "json");

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out())
                .contains("\"status\": \"created\"")
                .contains("\"recipe\": \"nested-json\"")
                .contains("\"path\": \"" + ws.resolve("source/orders_src.tap.yml") + "\"")
                .contains("\"path\": \"" + ws.resolve("pipeline/orders_sync.tap.yml") + "\"")
                .contains("\"kind\": \"source\"")
                .contains("\"kind\": \"pipeline\"")
                .contains("\"kind\": \"env\"")
                .contains("\"kind\": \"gitignore\"")
                .contains("\"assumed\": \"arrayKey: [id]\"")
                .doesNotContain("Workspace:");
    }

    // ---- consolidated-table ---------------------------------------------------------------------

    private static NewRecipeTest.Run consolidated(Path home, Path ws, String... extra) {
        return run(home, new ScriptedPrompter(), with(List.of("new", "consolidated-table", "--yes",
                "--table", "orders",
                "--db", "mysql,host=db1,username=u,password=s1",
                "--db", "mysql,host=db2,username=u,password=s2",
                "-w", ws.toString()), extra));
    }

    @Test
    void consolidatedTableWritesOneSourcePerDatabaseAndAUnionToTheGoldens(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        NewRecipeTest.Run r = consolidated(home, ws);

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(Files.readString(ws.resolve("source/orders_1_src.tap.yml"))).isEqualTo(CONSOLIDATED_SRC_1);
        assertThat(Files.readString(ws.resolve("source/orders_2_src.tap.yml"))).isEqualTo(CONSOLIDATED_SRC_2);
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(CONSOLIDATED_SYNC);
        assertThat(Files.readString(ws.resolve(".env")))
                .isEqualTo("ORDERS_1_SRC_PASSWORD=s1\nORDERS_2_SRC_PASSWORD=s2\n");
        assertThat(Files.readString(ws.resolve(".gitignore"))).isEqualTo(".env\n");
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n"
                + "  source/orders_1_src.tap.yml  source orders_1_src: mysql, cdc\n"
                + "  source/orders_2_src.tap.yml  source orders_2_src: mysql, cdc\n"
                + "  pipeline/orders_sync.tap.yml  pipeline orders_sync: 2 sources, view — assumed primary_key: id;");
    }

    /**
     * The scripted answers: the table, then two databases (connector, host, port as its default,
     * database skipped, username, password), no further database, then the view id (default).
     */
    @Test
    void interactiveConsolidatedTableProducesTheSameFilesAsTheFlagForm(@TempDir Path home, @TempDir Path ws,
                                                                       @TempDir Path flags) throws IOException {
        ScriptedPrompter prompter = new ScriptedPrompter(
                "orders",
                "mysql", "db1", "", "", "u", "s1",
                "mysql", "db2", "", "", "u", "s2", "",
                "");

        NewRecipeTest.Run r = run(home, prompter, "new", "consolidated-table", "--server", "http://127.0.0.1:8080", "--user", "admin",
                "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.secretQuestions).hasSize(2);
        assertThat(consolidated(home, flags).code()).isZero();
        assertSameFiles(ws, flags, "source/orders_1_src.tap.yml", "source/orders_2_src.tap.yml",
                "pipeline/orders_sync.tap.yml", ".env", ".gitignore");
    }

    /**
     * Two sources carrying the same table name is exactly the case a bare {@code from: [orders]} is
     * ambiguous in, so the union addresses each as {@code <source>.<table>}; this is where that is
     * held to what the reference closure accepts.
     */
    @Test
    void whatConsolidatedTableWritesValidates(@TempDir Path home, @TempDir Path ws) {
        assertThat(consolidated(home, ws).code()).isZero();

        NewRecipeTest.Run validated = validate(home, ws);

        assertThat(validated.code()).as(validated.all()).isZero();
        assertThat(validated.out()).contains("3 resources");
    }

    @Test
    void consolidatedTableRefusesFewerThanTwoDatabases(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), "new", "consolidated-table", "--yes",
                "--table", "orders", "--db", "mysql,host=db1", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("--db").contains("two");
        assertThat(ws.resolve("source")).doesNotExist();
    }

    @Test
    void consolidatedTableMissingTableIsAUsageErrorNamingTheFlag(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = run(home, new ScriptedPrompter(), "new", "consolidated-table", "--yes",
                "--db", "mysql,host=db1", "--db", "mysql,host=db2", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("--table");
        assertThat(ws.resolve("source")).doesNotExist();
    }

    @Test
    void consolidatedTableRefusesToOverwriteBeforeWritingAnything(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Files.createDirectories(ws.resolve("source"));
        Files.writeString(ws.resolve("source/orders_2_src.tap.yml"), "# mine\n");

        NewRecipeTest.Run r = consolidated(home, ws);

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.artifact-exists");
        assertThat(ws.resolve("source/orders_1_src.tap.yml")).doesNotExist();
        assertThat(ws.resolve("pipeline")).doesNotExist();
        assertThat(ws.resolve(".env")).doesNotExist();
        assertThat(Files.readString(ws.resolve("source/orders_2_src.tap.yml"))).isEqualTo("# mine\n");
    }

    @Test
    void consolidatedTableRewritesUnderForce(@TempDir Path home, @TempDir Path ws) throws IOException {
        assertThat(consolidated(home, ws).code()).isZero();
        Files.writeString(ws.resolve("pipeline/orders_sync.tap.yml"), "# mine now\n");

        NewRecipeTest.Run forced = consolidated(home, ws, "--force");

        assertThat(forced.code()).as(forced.all()).isZero();
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(CONSOLIDATED_SYNC);
        assertThat(forced.out())
                .contains("  pipeline/orders_sync.tap.yml  pipeline orders_sync: 2 sources, view (replaced) — assumed")
                .contains("  .env  secrets for the files above; not committed (updated)\n");
    }
}
