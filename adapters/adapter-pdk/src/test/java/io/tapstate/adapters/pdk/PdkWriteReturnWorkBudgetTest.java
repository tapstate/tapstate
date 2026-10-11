package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkWriteReturnWorkBudgetTest {
    @Test
    void appended_cold_origin_activity_extends_the_actual_report_and_retains_old_ordinals() throws Exception {
        var budget = budget();
        try (var span = budget.enter(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS)) { span.success(); }
        Map<String, Object> actual = evidence(budget);
        assertThat(((Map<?, ?>) actual.get("activities"))).hasSize(48);
        assertThat(((Map<?, ?>) actual.get("activities")).containsKey("COLD_ORIGINS")).isTrue();
        assertThat(PdkWriteReturnWorkBudget.Activity.BOOKKEEPING.ordinal()).isEqualTo(46);
        assertThat(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS.ordinal()).isEqualTo(47);
        Shape shape = new Shape(); shape.visit(actual);
        // Each appended activity contributes one map, its four counters and one root activity key.
        assertThat(staticLong("REPORT_MAPS")).isEqualTo(1 + 4 + 1 + 1 + 48 + 16);
        assertThat(staticLong("REPORT_ENTRIES")).isEqualTo(47 + 4 * 8 + 4 + 48 * 5 + 16 * 2);
        assertThat(staticLong("REPORT_NODES")).isEqualTo(1 + 355 + 16 + 32);
        assertThat(shape.maps).isLessThanOrEqualTo(staticLong("REPORT_MAPS"));
        assertThat(shape.entries).isLessThanOrEqualTo(staticLong("REPORT_ENTRIES"));
        assertThat(count(actual, "COLD_ORIGINS", "completed")).isEqualTo(1);
        assertThat(PdkWriteReturnWorkBudget.SOURCE_FORMULA_SHA256)
                .isEqualTo("5e7e3babad7f8e2bd19f22c8ce0aac1ba5164e72e3867d5957cc568a6bfa4c89")
                .isNotEqualTo("5248e6d4304d26ccfd6744b0b05cc6206cd7de0d31b3bb057a9b4569b19776cb")
                .isNotEqualTo("66db340eda98ee81382063031665532e1348d93ff0e5de90a67a39b674d9bada");
    }

    @Test
    void transport_recipe_binds_independent_raw_row_and_derived_frame_priors() {
        assertThat(PdkWriteReturnProbe.MIN_STORED_FRAME_BYTES).isEqualTo(111);
        assertThat(PdkWriteReturnProbe.MAX_LEDGER_BYTES).isEqualTo(2097152);
        assertThat(PdkWriteReturnProbe.MAX_PAGE_HEADER_BYTES).isEqualTo(672);
        assertThat(PdkWriteReturnProbe.MAX_NONEMPTY_PAGES).isEqualTo(67);
        assertThat(PdkWriteReturnProbe.MAX_STORED_BYTES).isEqualTo(2052128);
        assertThat(PdkWriteReturnProbe.MAX_FRAMES).isEqualTo(18487);
        assertThat(PdkWriteReturnProbe.MAX_RECORDS).isEqualTo(262144);
        assertThat(PdkWriteReturnProbe.MAX_BATCH_RECORDS).isEqualTo(512);
        assertThat(PdkWriteReturnProbe.MAX_CALL_RECORDS).isEqualTo(1024);
        assertThat(PdkWriteReturnWorkBudget.FORMULA).contains(PdkWriteReturnWorkBudget.TRANSPORT_SOURCE);
        assertThat(PdkWriteReturnWorkBudget.TRANSPORT_SOURCE).contains("admitted-storage-only-not-future-total-calls");
        // The source definition independently includes the fixed descriptor's bootstrap append/hash
        // transfer; admission does not substitute successful retained frames for complete attempt counts.
        assertThat(PdkWriteReturnWorkBudget.Activity.CONFIG.upper).containsExactly(1, 396952, 0, 0, 132752, 16, 265504, 265504);
        assertThat(PdkWriteReturnWorkBudget.Activity.RECEIPT_PIECE.upper).containsExactly(0, 32, 0, 1, 524288, 8, 524288, 128);
    }

    @Test
    void cold_source_bound_and_failed_attempts_are_independent_of_observed_class_bytes() {
        var budget = budget();
        try (var span = budget.enter(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS)) {
            budget.material(4, 21); span.success();
        }
        try (var failed = budget.enter(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS)) { }
        Map<String, Object> actual = evidence(budget);
        assertThat(count(actual, "COLD_ORIGINS", "invocations")).isEqualTo(2);
        assertThat(count(actual, "COLD_ORIGINS", "completed")).isEqualTo(1);
        assertThat(count(actual, "COLD_ORIGINS", "failed")).isEqualTo(1);
        assertThat(count(actual, "COLD_ORIGINS", "pending")).isZero();
        assertThat(vector(actual, "actualMaterial", "bytes")).isEqualTo(21);
        assertThat(vector(actual, "sourceUpper", "bytes")).isGreaterThanOrEqualTo(
                Math.multiplyExact(2, PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS.upper[4]));
        assertThat(actual).containsEntry("state", "UNKNOWN").containsEntry("sourceUpperIsActualWork", false)
                .containsEntry("structuralBudgetQualified", false).containsEntry("wholeMethodCostQualified", false);
        long[] copy = PdkReturnOrigins.perClassUpper(); copy[4] = 0;
        assertThat(PdkWriteReturnWorkBudget.Activity.COLD_ORIGINS.upper[4]).isPositive();
    }

    @Test
    void actual_serializer_collection_requests_fit_both_report_and_final_tail_allocations() throws Exception {
        Map<String, Object> tree = evidence(budget());
        AtomicLong requests = new AtomicLong();
        String actual = JsonWriter.write(countRequests(tree, requests));
        assertThat(parse(actual)).isEqualTo(tree);
        // This observed subset omits builder, character, boxing and hash calls. Even this subset
        // exceeds the former 32-request report/tail allocation, without relying on source text.
        assertThat(requests.get()).isGreaterThan(32);
        assertThat(PdkWriteReturnWorkBudget.Activity.MBEAN_BUDGET.upper[1]).isGreaterThanOrEqualTo(requests.get());
        var field = PdkWriteReturnWorkBudget.class.getDeclaredField("SEAL_TAIL_UPPER");
        assertThat(field.trySetAccessible()).isTrue();
        assertThat(((long[]) field.get(null))[1]).isGreaterThanOrEqualTo(requests.get());
    }

    @Test
    void full_rosters_printable_escapes_and_wide_counters_fit_independent_report_shape_and_work_bounds() throws Exception {
        var budget = budget();
        for (int i = 0; i < 16; i++) budget.epoch(i, "\\\"".repeat(256));
        for (int i = 0; i < 32; i++) budget.gap(i + "\\\"".repeat(63));
        for (String name : List.of("attempts", "completed", "failed", "upper", "material")) {
            var field = PdkWriteReturnWorkBudget.class.getDeclaredField(name);
            assertThat(field.trySetAccessible()).isTrue();
            java.util.Arrays.fill((long[]) field.get(budget), Long.MAX_VALUE);
        }
        Map<String, Object> actual = evidence(budget);
        Shape shape = new Shape(); shape.visit(actual);
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(staticList("ROOT_FIELDS"));
        @SuppressWarnings("unchecked") Map<String, Object> activities = (Map<String, Object>) actual.get("activities");
        assertThat(activities.keySet()).containsExactlyInAnyOrderElementsOf(staticList("ACTIVITY_NAMES"));
        assertThat(shape.maps).isEqualTo(staticLong("REPORT_MAPS"));
        assertThat(shape.entries).isEqualTo(staticLong("REPORT_ENTRIES"));
        assertThat(shape.nodes).isEqualTo(staticLong("REPORT_NODES"));
        assertThat(shape.keyChars).isEqualTo(staticLong("REPORT_KEY_CHARS"));
        assertThat(shape.keyChars + shape.stringChars).isLessThanOrEqualTo(staticLong("REPORT_STRING_CHARS"));
        assertThat((long) JsonWriter.write(actual).length()).isLessThanOrEqualTo(staticLong("REPORT_JSON_CHARS"));
        assertThat(staticLong("REPORT_JSON_CHARS")).isLessThanOrEqualTo(65536);
        assertThat(((Map<?, ?>) actual.get("activities")).get("BOOKKEEPING")).isNotNull();
    }

    @Test
    void bookkeeping_allocation_covers_observed_checked_scans_pairing_and_late_mapping_requests() throws Exception {
        long units = 8;
        long mutationMath = 3 + units;
        long basisMathAndOrdinals = 2 + 2 + 2 * units;
        long mappingWritesIncludingRefusal = 3;
        long pairingAndNotification = 2 + 6 + 2;
        long reasonReads = 2 * 128;
        assertThat(PdkWriteReturnWorkBudget.Activity.BOOKKEEPING.upper[1]).isGreaterThanOrEqualTo(
                mutationMath + basisMathAndOrdinals + mappingWritesIncludingRefusal + pairingAndNotification + reasonReads);
        assertThat(PdkWriteReturnWorkBudget.Activity.BOOKKEEPING.upper[4]).isGreaterThanOrEqualTo(8 + 2 * 4);
        var budget = budget();
        var span = budget.enter(PdkWriteReturnWorkBudget.Activity.CLOCK_READ); span.success(); span.close();
        Map<String, Object> actual = evidence(budget);
        assertThat(actual).containsEntry("pending", 0L);
        assertThat(count(actual, "CLOCK_READ", "completed")).isEqualTo(1);
        assertThat(vector(actual, "sourceUpper", "requests")).isGreaterThanOrEqualTo(
                ((Long) actual.get("bookkeepingInvocations")) * PdkWriteReturnWorkBudget.Activity.BOOKKEEPING.upper[1]);
    }

    @Test
    void invalid_epoch_or_reason_material_refuses_before_unbounded_report_material_is_retained() {
        var budget = budget();
        budget.epoch(1, "x".repeat(513));
        budget.gap("x".repeat(10000));
        Map<String, Object> actual = evidence(budget);
        assertThat(actual.get("epochs")).asList().isEmpty();
        assertThat(actual.get("gaps")).asList().contains("EPOCH_WINDOW_BOUND", "GAP_REASON_UNAVAILABLE");
        assertThat(actual).containsEntry("omitted", 1L).containsEntry("state", "UNKNOWN");
    }

    @Test
    void default_and_nonliteral_true_do_not_construct_an_enabled_ledger() {
        String previous = System.getProperty(PdkWriteReturnWorkBudget.PROPERTY);
        try {
            System.clearProperty(PdkWriteReturnWorkBudget.PROPERTY);
            assertThat(PdkWriteReturnWorkBudget.enabled()).isNull();
            for (String disabled : List.of("false", "TRUE", " true", "1")) {
                System.setProperty(PdkWriteReturnWorkBudget.PROPERTY, disabled);
                assertThat(PdkWriteReturnWorkBudget.enabled()).isNull();
            }
        } finally { restore(PdkWriteReturnWorkBudget.PROPERTY, previous); }
    }

    @Test
    void fresh_actual_attempts_are_distinct_from_caller_limits_and_source_upper() {
        var budget = budget();
        budget.observe(PdkWriteReturnWorkBudget.Activity.INACTIVE_BEGIN, 3);
        Map<String, Object> before = evidence(budget);
        assertThat(count(before, "INACTIVE_BEGIN", "invocations")).isEqualTo(3);
        assertThat(count(before, "IDENTITY_ROW", "invocations")).isZero();
        assertThat(vector(before, "actualMaterial", "bytes")).isZero();
        assertThat(vector(before, "sourceUpper", "bytes")).isPositive();
        assertThat(vector(before, "limits", "bytes")).isEqualTo(Long.MAX_VALUE);
        budget.observe(PdkWriteReturnWorkBudget.Activity.IDENTITY_ROW, 2);
        budget.material(2, 2);
        assertThat(count(before, "IDENTITY_ROW", "invocations")).isZero();
        assertThat(vector(evidence(budget), "actualMaterial", "records")).isEqualTo(2);
        assertThat(before).containsEntry("sourceUpperIsActualWork", false)
                .containsEntry("limitsIndependentlyVerified", false).containsEntry("structuralBudgetQualified", false);
    }

    @Test
    void malformed_foreign_duplicate_and_raised_material_inputs_refuse_credit() {
        String valid = JsonWriter.write(config());
        for (String raw : List.of(valid.replace("\"schemaVersion\":1", "\"schemaVersion\":1.0"),
                valid.replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"schemaVersion\":1"),
                valid.replace(PdkWriteReturnWorkBudget.SOURCE_FORMULA_SHA256, "0".repeat(64)),
                valid.replace("\"primaryKeys\":2", "\"primaryKeys\":3"),
                valid.replace("\"connectorHashBytes\":134217728", "\"connectorHashBytes\":134217729"))) {
            var refused = PdkWriteReturnWorkBudget.testing(encode(raw), 17, 23);
            assertThat(evidence(refused)).containsEntry("state", "UNKNOWN").containsEntry("structuralBudgetQualified", false);
            assertThat(evidence(refused).get("gaps")).asList().contains("INPUT_BINDING_UNAVAILABLE");
        }
        assertThat(evidence(PdkWriteReturnWorkBudget.testing(null, 17, 23))).containsEntry("state", "UNKNOWN");
        assertThat(evidence(PdkWriteReturnWorkBudget.testing("A".repeat(30000), 17, 23))).containsEntry("state", "UNKNOWN");
    }

    @Test
    void arithmetic_overflow_and_over_budget_are_sticky_without_throwing_into_a_write() {
        var budget = budget();
        budget.observe(PdkWriteReturnWorkBudget.Activity.IDENTITY_ROW, Long.MAX_VALUE);
        budget.observe(PdkWriteReturnWorkBudget.Activity.IDENTITY_ROW);
        assertThat(evidence(budget)).containsEntry("overflow", true).containsEntry("state", "UNKNOWN");
        Map<String, Object> config = config();
        @SuppressWarnings("unchecked") Map<String, Object> limits = (Map<String, Object>) config.get("limits");
        limits.put("bytes", 0L);
        var small = PdkWriteReturnWorkBudget.testing(encode(JsonWriter.write(config)), 17, 23);
        small.material(4, 1);
        assertThat(evidence(small)).containsEntry("overBudget", true).containsEntry("state", "UNKNOWN");
        assertThat(vector(evidence(small), "actualMaterial", "bytes")).isEqualTo(1);
    }

    @Test
    void untrusted_depth_numeric_token_and_node_rosters_are_refused_before_recursive_parsing() {
        for (String raw : List.of("[".repeat(1000) + "0" + "]".repeat(1000),
                "[" + "0,".repeat(1000) + "0]", "1".repeat(2000))) {
            assertThat(evidence(PdkWriteReturnWorkBudget.testing(encode(raw), 17, 23)))
                    .containsEntry("state", "UNKNOWN");
        }
    }

    @Test
    void common_clock_hash_zero_progress_is_one_attempt_and_closes_the_actual_stream() throws Exception {
        AtomicLong reads = new AtomicLong(), closes = new AtomicLong();
        InputStream stalled = new InputStream() {
            @Override public int read() { throw new AssertionError("the buffer read is the actual interface"); }
            @Override public int read(byte[] bytes) { reads.incrementAndGet(); return 0; }
            @Override public void close() { closes.incrementAndGet(); }
        };
        var hash = PdkBenchmarkClock.class.getDeclaredMethod("hash", InputStream.class, int.class);
        assertThat(hash.trySetAccessible()).isTrue();
        assertThatThrownBy(() -> hash.invoke(null, stalled, 8)).hasCauseInstanceOf(IOException.class);
        assertThat(reads.get()).isEqualTo(1); assertThat(closes.get()).isEqualTo(1);
        assertThat(PdkWriteReturnWorkBudget.CLOCK_SNAPSHOT_REQUESTS_UPPER).isEqualTo(
                2 * (PdkWriteReturnWorkBudget.CLOCK_HASH_MATERIAL_UPPER + 5) + 2053 + 1024);
    }

    @Test
    void connector_size_admission_precedes_hash_reads_and_actual_bytes_remain_separate(@TempDir Path directory) throws Exception {
        Map<String, Object> config = config();
        @SuppressWarnings("unchecked") Map<String, Object> materials = (Map<String, Object>) config.get("materialLimits");
        materials.put("connectorHashBytes", 4L);
        var budget = PdkWriteReturnWorkBudget.testing(encode(JsonWriter.write(config)), 17, 23);
        Path file = directory.resolve("connector.bin"); Files.writeString(file, "large");
        var hash = PdkMongoWriteScope.class.getDeclaredMethod("fileDigest", Path.class, PdkWriteReturnWorkBudget.class);
        assertThat(hash.trySetAccessible()).isTrue();
        assertThatThrownBy(() -> hash.invoke(null, file, budget)).hasCauseInstanceOf(RuntimeException.class);
        assertThat(count(evidence(budget), "HASH_READ", "invocations")).isZero();
        assertThat(vector(evidence(budget), "actualMaterial", "bytes")).isZero();
        Files.writeString(file, "four");
        var admitted = PdkWriteReturnWorkBudget.testing(encode(JsonWriter.write(config)), 17, 23);
        assertThat(hash.invoke(null, file, admitted)).isEqualTo(java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest("four".getBytes(StandardCharsets.US_ASCII))));
        assertThat(count(evidence(admitted), "HASH_READ", "invocations")).isEqualTo(2);
        assertThat(vector(evidence(admitted), "actualMaterial", "bytes")).isEqualTo(4);
    }

    @Test
    void inactive_active_rejected_and_late_attempts_survive_without_extra_clock_reads() {
        var budget = budget();
        AtomicLong clocks = new AtomicLong();
        var probe = new PdkWriteReturnProbe(clocks::incrementAndGet, false, budget);
        var writer = probe.writer("writer");
        var row = TapInsertRecordEvent.create().table("target").after(Map.of("id", 7));
        var table = new TapTable("target").add(new TapField("id", "int").primaryKeyPos(1));
        assertThat(writer.begin("source", table, List.of(row))).isNull();
        assertThat(clocks.get()).isZero();
        assertThat(probe.start("measured")).isTrue();
        var call = writer.begin("source", table, List.of(row));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.completed(null);
        assertThat(probe.stop()).isTrue();
        byte[] page = probe.read(0); probe.read(0); probe.read(-1);
        probe.getPid(); probe.getJvmStartTimeMillis(); probe.getState(); probe.getWindow();
        probe.getCompletedCalls(); probe.getFailedCalls(); probe.getReportedRecords();
        probe.getOpenCalls(); probe.getRetainedBytes(); probe.getCostStages();
        Map<String, Object> actual = evidence(budget);
        assertThat(clocks.get()).isEqualTo(4); // Begin, two actual exits, and the unchanged return observation.
        assertThat(count(actual, "CLOCK_READ", "invocations")).isEqualTo(4);
        assertThat(count(actual, "INACTIVE_BEGIN", "invocations")).isEqualTo(1);
        assertThat(count(actual, "ACTIVE_BEGIN", "invocations")).isEqualTo(1);
        assertThat(count(actual, "CALLBACK", "invocations")).isEqualTo(2);
        assertThat(count(actual, "CALLBACK_EXIT", "invocations")).isEqualTo(2);
        assertThat(count(actual, "COMPLETE", "invocations")).isEqualTo(2);
        assertThat(count(actual, "MBEAN_PAGE", "invocations")).isEqualTo(3);
        assertThat(count(actual, "REJECTION", "invocations")).isGreaterThanOrEqualTo(4);
        assertThat(count(actual, "PAGE_FRAME_VISIT", "invocations")).isEqualTo(4);
        assertThat(ByteBuffer.wrap(page).getInt(4)).isEqualTo(3);
        assertThat(probe.getReportedRecords()).isEqualTo(1);
    }

    @Test
    void invalid_key_shape_is_rejected_before_identity_piece_allocation() {
        var budget = budget();
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet, false, budget);
        var table = new TapTable("target");
        for (int i = 0; i < 3; i++) table.add(new TapField("k" + i, "int").primaryKeyPos(i + 1));
        probe.start("measured");
        var call = probe.writer("writer").begin("source", table,
                List.of(TapInsertRecordEvent.create().table("target").after(Map.of("k0", 1, "k1", 2, "k2", 3))));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
        Map<String, Object> actual = evidence(budget);
        assertThat(count(actual, "KEY_SHAPE", "invocations")).isEqualTo(1);
        assertThat(count(actual, "IDENTITY_PIECE", "invocations")).isZero();
        assertThat(count(actual, "RECEIPT_PIECE", "invocations")).isZero();
        assertThat(probe.getState()).isEqualTo("UNKNOWN:UNSUPPORTED_KEY_SHAPE");
    }

    @Test
    void live_pending_and_failure_facts_remain_unknown_without_a_fabricated_completion() {
        var budget = budget();
        var span = budget.enter(PdkWriteReturnWorkBudget.Activity.CLOCK_READ);
        Map<String, Object> pending = evidence(budget);
        assertThat(pending).containsEntry("pending", 1L).containsEntry("state", "UNKNOWN");
        assertThat(count(pending, "CLOCK_READ", "completed")).isZero();
        span.close();
        Map<String, Object> failed = evidence(budget);
        assertThat(failed).containsEntry("pending", 0L).containsEntry("state", "UNKNOWN");
        assertThat(count(failed, "CLOCK_READ", "failed")).isEqualTo(1);
        assertThat(count(pending, "CLOCK_READ", "failed")).isZero();
    }

    @Test
    void epoch_roster_is_finite_and_does_not_drop_later_actual_activity() {
        var budget = budget();
        for (int i = 1; i <= 17; i++) budget.epoch(i, "measured-" + i);
        budget.observe(PdkWriteReturnWorkBudget.Activity.INACTIVE_BEGIN);
        Map<String, Object> actual = evidence(budget);
        assertThat((List<?>) actual.get("epochs")).hasSize(16);
        assertThat(actual.get("gaps")).asList().contains("EPOCH_ROSTER_OVERFLOW");
        assertThat(count(actual, "INACTIVE_BEGIN", "invocations")).isEqualTo(1);
        assertThat(actual).containsEntry("state", "UNKNOWN");
        assertThat(budget.snapshot().getBytes(StandardCharsets.US_ASCII).length).isLessThanOrEqualTo(65536);
    }

    @Test
    void shutdown_body_and_companion_bind_actual_identity_nonce_and_complete_close(@TempDir Path directory) throws Exception {
        var budget = budget();
        var actor = budget.writer();
        var write = actor.write(); write.success(); write.close();
        actor.closeAttempt(); actor.closed(true);
        Path body = directory.toRealPath().resolve("body.json");
        AtomicLong unregisters = new AtomicLong();
        assertThat(budget.arm(body.toString(), "owned-nonce", unregisters::incrementAndGet, false)).isTrue();
        budget.finish(TimeUnit.SECONDS.toNanos(1));
        Map<String, Object> actual = parse(Files.readString(body));
        ByteBuffer companion = companion(body);
        assertThat(actual).containsEntry("stage", "SHUTDOWN_HOOK").containsEntry("pid", 17L)
                .containsEntry("jvmStartTimeMillis", 23L).containsEntry("processExitObserved", false)
                .containsEntry("sealTailIncludedInSourceUpper", true).containsEntry("state", "RECORDED_STRUCTURAL_DIAGNOSTIC");
        assertThat(unregisters.get()).isEqualTo(1);
        assertThat(companion.getInt(12)).isEqualTo(1);
        assertThat(companion.getLong(16)).isEqualTo(17); assertThat(companion.getLong(24)).isEqualTo(23);
        assertThat(companion.getLong(32)).isEqualTo(Files.size(body));
        assertThat(companion.getLong(40)).isZero(); assertThat(companion.getLong(184)).isEqualTo(2);
        assertThat(companion.getLong(192)).isZero(); assertThat(companion.getLong(200)).isEqualTo(1);
        byte[] hash = new byte[32]; companion.get(80, hash);
        assertThat(hash).isEqualTo(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(body)));
        assertThat(actual).containsEntry("crashDurabilityQualified", false).containsEntry("structuralBudgetQualified", false);
    }

    @Test
    void any_post_body_mutation_invalidates_the_shared_seal_and_preserves_the_original_body(@TempDir Path directory) throws Exception {
        var budget = budget();
        Path body = directory.toRealPath().resolve("late.json");
        assertThat(budget.arm(body.toString(), "owned", null, false)).isTrue();
        budget.finish(TimeUnit.SECONDS.toNanos(1));
        byte[] original = Files.readAllBytes(body);
        assertThat(companion(body).getInt(12)).isEqualTo(1);
        budget.observe(PdkWriteReturnWorkBudget.Activity.CALLBACK_WRAPPER);
        ByteBuffer after = companion(body);
        assertThat(after.getInt(12)).isEqualTo(2); assertThat(after.getLong(40)).isPositive();
        assertThat(Files.readAllBytes(body)).isEqualTo(original);
    }

    @Test
    void incomplete_writer_or_operation_cannot_be_sealed_as_completed(@TempDir Path directory) throws Exception {
        var budget = budget();
        var actor = budget.writer();
        var write = actor.write();
        Path body = directory.toRealPath().resolve("pending.json");
        assertThat(budget.arm(body.toString(), "owned", null, false)).isTrue();
        budget.finish(TimeUnit.MILLISECONDS.toNanos(25));
        assertThat(companion(body).getInt(12)).isNotEqualTo(1);
        Map<String, Object> pending = evidence(budget);
        assertThat(pending).containsEntry("pending", 1L).containsEntry("openWriters", 1L).containsEntry("state", "UNKNOWN");
        write.success(); write.close(); actor.closed(true);
        assertThat(companion(body).getInt(12)).isNotEqualTo(1);
    }

    @Test
    void a_blocked_external_finalizer_has_one_bounded_owner_wait_and_cannot_later_gain_credit(@TempDir Path directory) throws Exception {
        var budget = budget();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<Thread> finalizer = new AtomicReference<>();
        Path body = directory.toRealPath().resolve("blocked.json");
        assertThat(budget.arm(body.toString(), "owned", () -> {
            finalizer.set(Thread.currentThread());
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }, false)).isTrue();
        try {
            budget.finish(TimeUnit.MILLISECONDS.toNanos(25));
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(companion(body).getInt(12)).isEqualTo(2);
            assertThat(evidence(budget)).containsEntry("state", "UNKNOWN");
        } finally {
            release.countDown();
            Thread worker = finalizer.get();
            assertThat(worker).isNotNull();
            worker.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(worker.isAlive()).isFalse();
        }
    }

    @Test
    void create_new_and_one_shot_arm_preserve_existing_parent_owned_files(@TempDir Path directory) throws Exception {
        Path body = directory.toRealPath().resolve("existing.json");
        Files.writeString(body, "original");
        var refused = budget();
        assertThat(refused.arm(body.toString(), "owned", null, false)).isFalse();
        assertThat(Files.readString(body)).isEqualTo("original");
        assertThat(refused.arm(directory.toRealPath().resolve("other.json").toString(), "owned", null, false)).isFalse();
        assertThat(evidence(refused)).containsEntry("state", "UNKNOWN");
        var companionExists = budget();
        Path next = directory.toRealPath().resolve("new.json");
        Files.writeString(Path.of(next + ".seal"), "original-seal");
        assertThat(companionExists.arm(next.toString(), "owned", null, false)).isFalse();
        assertThat(Files.readString(Path.of(next + ".seal"))).isEqualTo("original-seal");
    }

    @Test
    void a_body_path_claimed_after_arm_refuses_the_actual_close_seal_without_overwrite(@TempDir Path directory) throws Exception {
        var budget = budget();
        Path body = directory.toRealPath().resolve("raced.json");
        assertThat(budget.arm(body.toString(), "owned", null, false)).isTrue();
        Files.writeString(body, "parent-owned");
        budget.finish(TimeUnit.SECONDS.toNanos(1));
        assertThat(Files.readString(body)).isEqualTo("parent-owned");
        assertThat(companion(body).getInt(12)).isEqualTo(2);
        assertThat(companion(body).getLong(200)).isZero();
        assertThat(companion(body).getLong(192)).isZero(); // No mapping force was attempted.
    }

    private static PdkWriteReturnWorkBudget budget() {
        return PdkWriteReturnWorkBudget.testing(encode(JsonWriter.write(config())), 17, 23);
    }
    private static Map<String, Object> config() {
        var limits = new LinkedHashMap<String, Object>();
        for (String unit : PdkWriteReturnWorkBudget.UNITS) limits.put(unit, Long.MAX_VALUE);
        var material = new LinkedHashMap<String, Object>();
        material.put("connectorHashBytes", 134217728L); material.put("runtimeArguments", 128L);
        material.put("runtimeArgumentBytes", 2093056L); material.put("primaryKeys", 2L);
        var config = new LinkedHashMap<String, Object>();
        config.put("schemaVersion", 1); config.put("definitionId", PdkWriteReturnWorkBudget.DEFINITION_ID);
        config.put("sourceFormulaSha256", PdkWriteReturnWorkBudget.SOURCE_FORMULA_SHA256);
        config.put("inputSha256", "a".repeat(64)); config.put("runId", "owned-run");
        config.put("limits", limits); config.put("materialLimits", material);
        return config;
    }
    private static String encode(String raw) { return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8)); }
    private static Map<String, Object> evidence(PdkWriteReturnWorkBudget budget) { return parse(budget.snapshot()); }
    @SuppressWarnings("unchecked") private static Map<String, Object> parse(String raw) { return (Map<String, Object>) JsonReader.parse(raw); }
    private static long count(Map<String, Object> receipt, String activity, String field) {
        return (Long) ((Map<?, ?>) ((Map<?, ?>) receipt.get("activities")).get(activity)).get(field);
    }
    private static long vector(Map<String, Object> receipt, String vector, String field) { return (Long) ((Map<?, ?>) receipt.get(vector)).get(field); }
    private static ByteBuffer companion(Path body) throws Exception {
        byte[] raw = Files.readAllBytes(Path.of(body + ".seal"));
        assertThat(raw).hasSize(256);
        return ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
    }
    private static void restore(String name, String previous) { if (previous == null) System.clearProperty(name); else System.setProperty(name, previous); }

    @SuppressWarnings("unchecked") private static List<String> staticList(String name) throws Exception {
        var field = PdkWriteReturnWorkBudget.class.getDeclaredField(name);
        assertThat(field.trySetAccessible()).isTrue(); return (List<String>) field.get(null);
    }
    private static long staticLong(String name) throws Exception {
        var field = PdkWriteReturnWorkBudget.class.getDeclaredField(name);
        assertThat(field.trySetAccessible()).isTrue(); return ((Number) field.get(null)).longValue();
    }
    private static final class Shape {
        long maps, entries, nodes, keyChars, stringChars;
        void visit(Object value) {
            nodes++;
            if (value instanceof Map<?, ?> map) {
                maps++; entries += map.size();
                for (var entry : map.entrySet()) { keyChars += ((String) entry.getKey()).length(); visit(entry.getValue()); }
            } else if (value instanceof List<?> list) { for (Object child : list) visit(child); }
            else if (value instanceof String text) stringChars += text.length();
        }
    }
    @SuppressWarnings("unchecked") private static Object countRequests(Object value, AtomicLong requests) {
        if (value instanceof Map<?, ?> raw) {
            Map<String, Object> map = (Map<String, Object>) raw;
            return new AbstractMap<String, Object>() {
                @Override public Set<Entry<String, Object>> entrySet() {
                    requests.incrementAndGet();
                    return new AbstractSet<>() {
                        @Override public int size() { return map.size(); }
                        @Override public Iterator<Entry<String, Object>> iterator() {
                            requests.incrementAndGet(); var delegate = map.entrySet().iterator();
                            return new Iterator<>() {
                                @Override public boolean hasNext() { requests.incrementAndGet(); return delegate.hasNext(); }
                                @Override public Entry<String, Object> next() {
                                    requests.incrementAndGet(); var entry = delegate.next();
                                    return new Entry<>() {
                                        @Override public String getKey() { requests.incrementAndGet(); return entry.getKey(); }
                                        @Override public Object getValue() { requests.incrementAndGet(); return countRequests(entry.getValue(), requests); }
                                        @Override public Object setValue(Object ignored) { throw new UnsupportedOperationException(); }
                                    };
                                }
                            };
                        }
                    };
                }
            };
        }
        if (value instanceof List<?> list) {
            return new AbstractList<Object>() {
                @Override public Object get(int index) { return countRequests(list.get(index), requests); }
                @Override public int size() { return list.size(); }
                @Override public Iterator<Object> iterator() {
                    requests.incrementAndGet(); var delegate = list.iterator();
                    return new Iterator<>() {
                        @Override public boolean hasNext() { requests.incrementAndGet(); return delegate.hasNext(); }
                        @Override public Object next() { requests.incrementAndGet(); return countRequests(delegate.next(), requests); }
                    };
                }
            };
        }
        return value;
    }
}
