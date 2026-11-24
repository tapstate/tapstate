package io.tapstate.core.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ExecutionFailureRecorderTest {
    @Test void oldProvidersAcceptNewProcessorCallsWithoutLosingTheOriginalBinding() {
        var coordinate = new AtomicReference<String>();
        var failure = new AtomicReference<Throwable>();
        ExecutionFailureRecorder old = (pipeline, job, identity) -> {
            coordinate.set(pipeline + ":" + job + ":" + identity);
            return failure::set;
        };
        var original = new IllegalStateException("original");
        old.forProcessor("orders", 7, 11, "run-a").accept(original);
        assertThat(coordinate.get()).isEqualTo("orders:7:run-a");
        assertThat(failure.get()).isSameAs(original);
        old.forExecution("legacy", 3, null).accept(original);
        assertThat(coordinate.get()).isEqualTo("legacy:3:null");
        ExecutionFailureRecorder nullable = (pipeline, job, identity) -> null;
        assertThat(nullable.forProcessor(null, 0, 0, null)).isNull();
    }
}
