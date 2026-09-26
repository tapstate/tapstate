package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A hundred tables written through one sink share the sink's writers, and every write a writer makes holds one table.
 *
 * <p>A sink's width is the number of writers for the whole sink, not for each table it writes: a hundred tables and
 * the default width are four writers, not four hundred, and four connections, not four hundred. What keeps that from
 * mixing the tables up is that a writer hands its connector one table at a time - a connector writes a batch into
 * the table the batch names, and a batch naming two would be one table's rows written into the other's.
 *
 * <p>Read from the connector's own account of what it wrote, which names the writer and the batch of every row and the
 * table each row carries, and from the plan the run was submitted under.
 */
class AHundredTablesShareTheSinksWritersIT {

    private static final String PIPELINE = "hundred_tables_pipe";
    private static final String SOURCE = "hundred_tables_src";
    private static final String TARGET = "hundred_tables_tgt";
    private static final int TABLES = 100;
    private static final long ROWS_EACH = 3;
    private static final int WRITERS = 4;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aHundredTablesAreWrittenByTheSinksFourWritersOneTableABatch(@TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        WriteWitness written = new WriteWitness(directory.resolve("written"));
        List<String> tables = IntStream.range(0, TABLES).mapToObj(i -> String.format("t%03d", i)).toList();

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_hundred_tables_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-hundred-tables")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                for (String table : tables) {
                    files.seed(sourceAddress, table, SeedRows.generated(ROWS_EACH));
                }
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source, tables, Map.of()));
                resources.put(TARGET + ".tap.yml",
                        Workspaces.targetYaml(TARGET, target, Map.of("write_witness", written.directory().toString())));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, tables));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(2),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                // Waited for on the connector's account rather than on the tables: a batch that mixed two tables
                // puts one table's rows into the other's, so the tables would never both come out whole, and the
                // wait would end on a timeout rather than on the batch that did it. Counted as distinct ids of
                // each table, by the table each row carries: a row can be written more than once.
                Await.until("every table's rows to be written", Duration.ofMinutes(3),
                        () -> idsWrittenByTable(written).size() == TABLES && idsWrittenByTable(written).values()
                                .stream().allMatch(ids -> ids.size() == ROWS_EACH),
                        () -> "tables with every row written: " + idsWrittenByTable(written).values().stream()
                                .filter(ids -> ids.size() == ROWS_EACH).count() + " of " + TABLES);

                List<WriteWitness.Written> rows = written.rows();
                Map<String, Set<String>> tablesByBatch = rows.stream().collect(Collectors.groupingBy(
                        WriteWitness.Written::batchId, Collectors.mapping(WriteWitness.Written::rowTable,
                                Collectors.toSet())));
                assertThat(tablesByBatch.values())
                        .as("every batch a writer handed its connector held rows of one table")
                        .allSatisfy(inOneBatch -> assertThat(inOneBatch).hasSize(1));
                assertThat(tables)
                        .as("and every table came out whole")
                        .allSatisfy(table -> assertThat(files.count(targetAddress, table)).isEqualTo(ROWS_EACH));

                ControlPlane.Plan plan = Await.answered("the status to carry the run's plan",
                        () -> control.executionPlan(PIPELINE));
                assertThat(plan.nodeRequested(WRITERS).effective())
                        .as("the sink's width is its writers for every table, not for each").isEqualTo(WRITERS);
                Set<String> writers = rows.stream().map(WriteWitness.Written::writerId).collect(Collectors.toSet());
                assertThat(writers)
                        .as("the hundred tables were written by the sink's writers, no more of them than its width")
                        .hasSizeLessThanOrEqualTo(WRITERS);
                assertThat(writers).as("and shared between them").hasSizeGreaterThan(1);
            }
        }
    }

    /** The distinct ids written so far of each table, by the table each row carries. */
    private static Map<String, Set<String>> idsWrittenByTable(WriteWitness written) {
        return written.rows().stream().collect(Collectors.groupingBy(WriteWitness.Written::rowTable,
                Collectors.mapping(WriteWitness.Written::id, Collectors.toSet())));
    }
}
