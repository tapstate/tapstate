package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The sink-side rename gate (ADR-0016 §8, X4): a rename must name every target table it produces, and no two
 * source tables written to one connection may resolve onto the same target name. Both are judged here, at
 * validate time, over the tables the pipeline's sources declare — a rename that only fails once rows are
 * flowing has already created the wrong table, or merged two of them.
 */
class RenameRulesTest {

    @Test
    void rejects_a_rename_map_that_names_a_blank_target_table() {
        Throwable thrown = catchThrowable(() -> workspace(
                rename(Map.of("orders", "  "), null, null, null), "orders"));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("serve.sync[0].rename.map");
    }

    @Test
    void rejects_a_case_transform_that_leaves_no_target_table_name() {
        Throwable thrown = catchThrowable(() -> workspace(
                rename(null, RenameCase.CAMEL, null, null), "___"));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("serve.sync[0].rename");
    }

    @Test
    void rejects_two_source_tables_that_resolve_onto_one_target_table() {
        Throwable thrown = catchThrowable(() -> workspace(
                rename(null, RenameCase.LOWER, null, null), "Orders", "ORDERS"));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.COMPOSITION);
        assertThat(ex.path()).isEqualTo("serve.sync[0].rename");
    }

    @Test
    void rejects_two_sync_elements_writing_one_target_table_to_one_connection() {
        List<Resource> batch = batch(List.of("orders"),
                List.of(new SyncElement("a", "tgt", null, rename(Map.of("orders", "ods_orders"), null, null, null),
                                null, null),
                        new SyncElement("b", "tgt", null, rename(null, null, "ods_", null), null, null)),
                List.of("tgt"));

        Throwable thrown = catchThrowable(() -> Workspace.of(batch));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.COMPOSITION);
        assertThat(ex.path()).isEqualTo("serve.sync[1].rename");
    }

    @Test
    void allows_two_sync_elements_writing_one_target_table_to_different_connections() {
        List<Resource> batch = batch(List.of("orders"),
                List.of(new SyncElement("a", "tgt_one", null, rename(null, null, "ods_", null), null, null),
                        new SyncElement("b", "tgt_two", null, rename(null, null, "ods_", null), null, null)),
                List.of("tgt_one", "tgt_two"));

        assertThatCode(() -> Workspace.of(batch)).doesNotThrowAnyException();
    }

    @Test
    void allows_an_explicit_map_that_agrees_with_the_bulk_rules() {
        assertThatCode(() -> workspace(
                rename(Map.of("ORDERS", "ods_orders"), RenameCase.LOWER, "ods_", null),
                "ORDERS", "ORDER_ITEMS"))
                .doesNotThrowAnyException();
    }

    @Test
    void leaves_a_dynamic_table_selector_to_the_runtime() {
        List<Resource> batch = new ArrayList<>();
        batch.add(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.regex(".*")), null, null, null));
        batch.add(new SourceResource("tgt", null, "mysql", Map.of("host", "h"), null, null, null, null, null));
        batch.add(new PipelineResource("p", null, List.of(SourceRef.bare("src")), null, null,
                new ServeBlock.Inline(null, FromRef.regex(".*"),
                        List.of(new SyncElement("a", "tgt", null, rename(null, RenameCase.CAMEL, null, null),
                                null, null)),
                        null, null),
                null, null));

        assertThatCode(() -> Workspace.of(batch)).doesNotThrowAnyException();
    }

    // ---- fixtures ----------------------------------------------------------------------

    private static RenameSpec rename(Map<String, String> map, RenameCase caseMode, String prefix, String suffix) {
        return new RenameSpec(map, caseMode, prefix, suffix);
    }

    /** One source of the given tables, feeding one sync element that renames by {@code spec}. */
    private static Workspace workspace(RenameSpec spec, String... tables) {
        return Workspace.of(batch(List.of(tables),
                List.of(new SyncElement("a", "tgt", null, spec, null, null)), List.of("tgt")));
    }

    private static List<Resource> batch(List<String> tables, List<SyncElement> sync, List<String> targets) {
        List<Resource> batch = new ArrayList<>();
        batch.add(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                tables.stream().map(TableRef::literal).map(t -> (TableRef) t).toList(), null, null, null));
        for (String target : targets) {
            batch.add(new SourceResource(target, null, "mysql", Map.of("host", "h"), null, null, null, null, null));
        }
        batch.add(new PipelineResource("p", null, List.of(SourceRef.bare("src")), null, null,
                new ServeBlock.Inline(null, FromRef.regex(".*"), sync, null, null), null, null));
        return batch;
    }
}
