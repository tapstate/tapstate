package io.tapstate.app;

import io.tapstate.cli.Cli;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ArtifactOutcome;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ConsolidatedTableRecipeStartTest {

    @Test
    void theConsolidatedTableRecipeCanStartWithTheSameTableInTwoDatabases(@TempDir Path workspace)
            throws IOException {
        CommandLine cli = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        StringWriter errors = new StringWriter();
        cli.setOut(new PrintWriter(output));
        cli.setErr(new PrintWriter(errors));
        int exit = cli.execute("new", "consolidated-table", "--yes", "--table", "m_orders",
                "--db", "mysql,host=db1,database=bdoe_src,username=u,password=test",
                "--db", "mysql,host=db2,database=bdoe_src2,username=u,password=test",
                "-w", workspace.toString());
        assertThat(exit).as(output + errors.toString()).isZero();

        List<ArtifactDraft> drafts = new ArrayList<>();
        for (String file : List.of("source/m_orders_1_src.tap.yml", "source/m_orders_2_src.tap.yml",
                "pipeline/m_orders_sync.tap.yml")) {
            drafts.add(new ArtifactDraft(file, Files.readString(workspace.resolve(file))));
        }
        InMemoryStorePort store = new InMemoryStorePort();
        AuditGate audit = new AuditGate(record -> {}, Clock.systemUTC());
        ApplyService apply = new ApplyService(TapstateCatalog::load, store.artifacts(), audit,
                store.schemas(), PlanAdvisories.none(), new StoreBackedDerivedSchemas(store, audit));
        assertThat(apply.apply("recipe-test", drafts).outcomes())
                .hasSize(3).allSatisfy(outcome ->
                        assertThat(outcome.change()).isEqualTo(ArtifactOutcome.Change.CREATED));

        // Discovery and the deployment's managed view store are prerequisites of a real start.
        for (String source : List.of("m_orders_1_src", "m_orders_2_src")) {
            store.schemas().save(new DiscoveredSourceModel(source, "mysql", 1L,
                    new SourceModel(List.of(new SourceTable("m_orders",
                            List.of(new SourceField("id", "bigint", TapstateType.INT64)),
                            List.of("id"), List.of())))));
        }
        store.artifacts().save(new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: 'mongodb://view-store/tapstate' }
                """.formatted(ViewTargetResolver.STATE_STORE_SOURCE_ID)));
        OpenRingGenerations.forSources(store, "m_orders_1_src", "m_orders_2_src");
        assertThat(((SourceResource) store.artifacts().get("m_orders_1_src").orElseThrow()).config())
                .containsEntry("database", "bdoe_src");
        assertThat(((SourceResource) store.artifacts().get("m_orders_2_src").orElseThrow()).config())
                .containsEntry("database", "bdoe_src2");

        assertThatCode(() -> new StoreBackedDagSource(store)
                .prepareStart("m_orders_sync", "tapstate").plan().build(null))
                .as("the recipe's qualified union inputs must pass start preparation and build its view DAG")
                .doesNotThrowAnyException();
    }
}
