package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.spi.transform.TransformPort;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A transform's unit of work is one row through its port, and that is what it times: three rows in are
 * three units, whatever each produced, and a row that produced nothing is a unit like any other.
 */
class ATransformTimesEachRowThroughItsPortTest {

    private static Envelope row(long id) {
        return Envelope.insert(id, "orders", Map.of("id", id), null);
    }

    @Test
    @DisplayName("each row through the port is one unit of the transform stage")
    void eachRowThroughThePortIsOneUnit() throws Exception {
        AtomicInteger seen = new AtomicInteger();
        TransformPort dropEverySecond = event -> seen.incrementAndGet() % 2 == 0 ? List.of() : List.of(event);
        TransformProcessor processor = new TransformProcessor(dropEverySecond);
        processor.init(new TestOutbox(new int[] {1024}, 1024), new TestProcessorContext());

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(row(1L), row(2L), row(3L)));
        processor.process(0, inbox);

        assertThat(processor.timing().stage()).isEqualTo(Stage.TRANSFORM);
        // Three units: the dropped row went through the port too, and cost what it cost.
        assertThat(processor.timing().value().count()).isEqualTo(3L);
        assertThat(processor.timing().value().bucketCounts().stream().mapToLong(Long::longValue).sum())
                .isEqualTo(3L);
    }
}
