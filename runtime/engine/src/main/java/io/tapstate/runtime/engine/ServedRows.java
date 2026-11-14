package io.tapstate.runtime.engine;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.common.ReservedField;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * The gate every row passes on its way to a target: product bookkeeping kept under the reserved
 * field is removed here, and only what is meant for consumers is left on the row.
 *
 * <p>It wraps the writer a sink is handed, at the one point where a sink obtains a writer, so no
 * path to a target goes around it and no step upstream has to be trusted to have cleaned up. A row
 * that carries nothing reserved, which is nearly every row, is passed through as the same object.
 */
final class ServedRows implements SinkWriter {

    private final SinkWriter delegate;
    private final boolean deliver;

    private ServedRows(SinkWriter delegate, boolean deliver) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.deliver = deliver;
    }

    /**
     * {@code writer} behind the gate. With {@code deliver}, entries meant for consumers stay on the
     * row; without it the reserved field is removed whole, for a target that accepts no field it did
     * not declare.
     */
    static SinkWriter over(SinkWriter writer, boolean deliver) {
        return writer instanceof ServedRows gated && gated.deliver == deliver
                ? writer
                : new ServedRows(writer, deliver);
    }

    @Override
    public CompletionStage<WriteResult> write(List<Envelope> records) {
        return delegate.write(served(records, deliver));
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** The batch as a target may receive it; the same list when no row carries anything reserved. */
    static List<Envelope> served(List<Envelope> records, boolean deliver) {
        List<Envelope> copies = null;
        for (int index = 0; index < records.size(); index++) {
            Envelope record = records.get(index);
            Envelope served = served(record, deliver);
            if (served != record && copies == null) {
                copies = new ArrayList<>(records);
            }
            if (copies != null) {
                copies.set(index, served);
            }
        }
        return copies == null ? records : copies;
    }

    private static Envelope served(Envelope record, boolean deliver) {
        Map<String, Object> before = ReservedField.served(record.before(), deliver);
        Map<String, Object> after = ReservedField.served(record.after(), deliver);
        if (before == record.before() && after == record.after()) {
            return record;
        }
        return new Envelope(record.op(), record.ts(), record.src(), before, after, record.schema(),
                record.positions(), record.removed());
    }
}
