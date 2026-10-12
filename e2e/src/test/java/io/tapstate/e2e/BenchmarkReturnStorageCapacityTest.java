package io.tapstate.e2e;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnStorageCapacityTest {
    private static final String STATE = "RECORDED_SCOPE_UNQUALIFIED";
    private static final String ACK = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT";

    @Test void more_than_512_complete_calls_are_decoded_without_dropping_rows_or_synthesizing_batches() throws Exception {
        List<byte[]> pages = pages(600);
        var assembly = new BenchmarkWriteReturnAssembly("window");
        for (byte[] page : pages) assembly.add(page);
        var calls = assembly.finish(600, 600);
        assertThat(calls).hasSize(600);
        assertThat(calls.stream().mapToInt(call -> call.rows().size()).sum()).isEqualTo(600);
        assertThat(calls.stream().map(call -> call.rows().getFirst().keys().getFirst()).toList())
                .containsExactlyElementsOf(java.util.stream.IntStream.rangeClosed(1, 600).boxed().toList());
        assertThat(pages.size()).isLessThanOrEqualTo(512);
        assertThat(pages.stream().mapToInt(page -> page.length).sum()).isLessThanOrEqualTo(2097152);
    }

    @Test void scoped_result_copy_is_independent_of_the_1024_rows_per_call_limit() throws Exception {
        var assembly = new BenchmarkWriteReturnAssembly("window"); var pages = pages(1200);
        for (byte[] page : pages) assembly.add(page);
        var calls = assembly.finish(1200, 1200);
        var upper = BenchmarkReturnCaptureMaterialBounds.upper();
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(upper.keySet());
        try (var ignored = BenchmarkReturnWorkBudget.activate(new BenchmarkReturnWorkBudget.Definition("storage-capacity-control",
                "a".repeat(64), Map.of(), upper, excluded), "storage-capacity-control")) {
            var capture = new BenchmarkWriteReturnCapture.Result(calls, List.of(),
                    new BenchmarkWriteReturnReader.Summary("window", STATE, 1200, 0, 1200, 0, 0), List.of());
            assertThat(capture.calls()).hasSize(1200);
            assertThat(capture.calls().getFirst().totalRows()).isEqualTo(1);
            assertThatThrownBy(() -> BenchmarkReturnCaptureMaterialBounds.copy(java.util.Collections.nCopies(1025,
                    capture.calls().getFirst().rows().getFirst()), 1024)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void per_page_piece_roster_refuses_before_allocating_a_global_sized_call_list() throws Exception {
        byte[] header = page(0, 591, 591, List.of());
        assertThat(header.length).isLessThan(65536);
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(header))
                .isInstanceOf(AssertionError.class).hasMessageContaining("page piece roster");
    }

    @Test void global_storage_piece_limit_is_separate_from_the_fixed_total_row_limit() throws Exception {
        byte[] header = page(0, 0, BenchmarkReturnLocalSourceWork.MAX_STORED_FRAMES + 1, List.of());
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(header)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("completion cursor");
        var assembly = new BenchmarkWriteReturnAssembly("window"); assembly.add(pages(1).getFirst());
        assertThatThrownBy(() -> assembly.finish(1, 262145)).isInstanceOf(AssertionError.class);
        assertThat(BenchmarkReturnLocalSourceWork.maximumFrameRows()).isEqualTo(262144);
        var rows = BenchmarkReturnLocalSourceWork.bounds(new BenchmarkReturnLocalSourceWork.Capacity(96000, 48000, 960));
        assertThat(rows.get(BenchmarkReturnWorkBudget.Activity.ASSOCIATION).records()).isEqualTo(262144);
        assertThat(rows.get(BenchmarkReturnWorkBudget.Activity.DECODE).frames()).isEqualTo(590);
    }

    @Test void source_header_reservation_encloses_all_nonempty_pages_without_counting_initial_validation() {
        assertThat(BenchmarkReturnLocalSourceWork.MIN_STORED_FRAME_BYTES).isEqualTo(111);
        assertThat(BenchmarkReturnLocalSourceWork.HEADER_PAGE_RESERVATION).isEqualTo(265);
        assertThat(BenchmarkReturnLocalSourceWork.STORED_LEDGER_BYTES + 265 * 672).isEqualTo(8388608);
        assertThat((long) BenchmarkReturnLocalSourceWork.MAX_STORED_FRAMES * 111).isLessThanOrEqualTo(8210528);
        assertThat((long) (BenchmarkReturnLocalSourceWork.MAX_STORED_FRAMES + 1) * 111).isGreaterThan(8210528);
        assertThat((long) BenchmarkReturnLocalSourceWork.MAX_PAGE_FRAMES * 111 + 32).isLessThanOrEqualTo(65536);
        assertThat((long) (BenchmarkReturnLocalSourceWork.MAX_PAGE_FRAMES + 1) * 111 + 32).isGreaterThan(65536);
    }

    private static List<byte[]> pages(int count) throws Exception {
        var pages = new ArrayList<byte[]>();
        //100 pieces make a small real page. This fixture is not a workload batching condition.
        for (int from = 0; from < count; from += 100) {
            int to = Math.min(count, from + 100); var pieces = new ArrayList<byte[]>();
            for (int index = from; index < to; index++) pieces.add(piece(index + 1));
            pages.add(page(from, to, count, pieces));
        }
        return List.copyOf(pages);
    }
    private static byte[] page(int cursor, int next, int total, List<byte[]> pieces) throws Exception {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(0x57525031); out.writeInt(3); out.writeLong(1);
        out.writeInt(cursor); out.writeInt(next); out.writeInt(total); text(out, "window"); text(out, STATE);
        for (byte[] piece : pieces) { out.writeInt(piece.length); out.write(piece); }
        out.close(); return bytes.toByteArray();
    }
    private static byte[] piece(int sequence) throws Exception {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeLong(sequence); out.writeInt(1); out.writeInt(1); out.writeInt(0); out.writeInt(1);
        out.writeLong(100); out.writeLong(150); out.writeLong(200); out.writeBoolean(true); out.writeInt(1);
        out.writeLong(1); out.writeLong(0); out.writeLong(0); out.writeInt(0); out.writeInt(0);
        text(out, ""); text(out, ACK); text(out, "writer"); text(out, "stream"); text(out, "target");
        out.writeByte(1); text(out, "id"); out.writeInt(1); out.writeByte(1); out.writeInt(sequence);
        out.close(); return bytes.toByteArray();
    }
    private static void text(DataOutputStream out, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeShort(bytes.length); out.write(bytes);
    }
}
