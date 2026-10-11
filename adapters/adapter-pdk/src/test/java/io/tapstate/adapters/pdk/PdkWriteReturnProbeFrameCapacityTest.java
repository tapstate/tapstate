package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PdkWriteReturnProbeFrameCapacityTest {
    @Test
    void more_than_512_complete_calls_keep_every_original_96000_row_receipt() throws Exception {
        AtomicLong clock = new AtomicLong();
        var probe = new PdkWriteReturnProbe(clock::incrementAndGet);
        var writer = probe.writer("writer");
        var table = new TapTable("target").add(new TapField("id", "int").primaryKeyPos(1));
        assertThat(probe.start("complete-96000")).isTrue();
        // A controlled legal distribution, not a minimum batch size required of a future caller.
        for (int call = 0; call < 600; call++) {
            var rows = new ArrayList<TapRecordEvent>(160);
            for (int index = 0; index < 160; index++) rows.add(TapInsertRecordEvent.create().table("target")
                    .after(Map.of("id", call * 160 + index)));
            var ticket = writer.begin("source", table, rows);
            ticket.callback(new WriteListResult<>(160L, 0L, 0L)); ticket.callbackExited(); ticket.returned(null);
        }
        assertThat(probe.stop()).isTrue();
        assertThat(probe.getState()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
        assertThat(probe.getCompletedCalls()).isEqualTo(600);
        assertThat(probe.getReportedRecords()).isEqualTo(96000);
        assertThat(probe.getFailedCalls()).isZero(); assertThat(probe.getOpenCalls()).isZero();
        assertThat(clock).hasValue(1800);
        int cursor = 0, retainedCalls = 0, expectedKey = 0, rawBytes = 0;
        while (true) {
            byte[] bytes = probe.read(cursor); rawBytes = Math.addExact(rawBytes, bytes.length);
            assertThat(bytes.length).isLessThanOrEqualTo(64 * 1024);
            var page = new DataInputStream(new ByteArrayInputStream(bytes));
            assertThat(page.readInt()).isEqualTo(0x57525031); assertThat(page.readInt()).isEqualTo(3);
            page.readLong(); assertThat(page.readInt()).isEqualTo(cursor);
            int next = page.readInt(), total = page.readInt(); text(page); text(page);
            assertThat(total).isEqualTo(600); assertThat(next).isGreaterThan(cursor);
            for (int index = cursor; index < next; index++) {
                int length = page.readInt(); var frame = new DataInputStream(new ByteArrayInputStream(page.readNBytes(length)));
                assertThat(frame.readLong()).isEqualTo(index + 1L); frame.readInt();
                assertThat(frame.readInt()).isEqualTo(160); assertThat(frame.readInt()).isZero(); assertThat(frame.readInt()).isEqualTo(1);
                long began = frame.readLong(), exit = frame.readLong(), returned = frame.readLong();
                assertThat(exit).isGreaterThanOrEqualTo(began); assertThat(returned).isGreaterThanOrEqualTo(exit);
                assertThat(frame.readUnsignedByte()).isEqualTo(1); assertThat(frame.readInt()).isEqualTo(1);
                assertThat(frame.readLong()).isEqualTo(160); assertThat(frame.readLong()).isZero(); assertThat(frame.readLong()).isZero();
                assertThat(frame.readInt()).isZero(); assertThat(frame.readInt()).isZero();
                assertThat(text(frame)).isEmpty(); text(frame); text(frame); text(frame); text(frame);
                assertThat(frame.readUnsignedByte()).isEqualTo(1); assertThat(text(frame)).isEqualTo("id");
                assertThat(frame.readInt()).isEqualTo(160);
                for (int row = 0; row < 160; row++) { assertThat(frame.readUnsignedByte()).isEqualTo(1);
                    assertThat(frame.readInt()).isEqualTo(expectedKey++); }
                assertThat(frame.available()).isZero(); retainedCalls++;
            }
            assertThat(page.available()).isZero(); cursor = next; if (cursor == total) break;
        }
        assertThat(retainedCalls).isEqualTo(600); assertThat(expectedKey).isEqualTo(96000);
        assertThat(rawBytes).isLessThanOrEqualTo(2 * 1024 * 1024);
    }
    private static String text(DataInputStream input) throws Exception {
        return new String(input.readNBytes(input.readUnsignedShort()), StandardCharsets.UTF_8);
    }
}
