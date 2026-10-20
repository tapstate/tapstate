package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestStateStore;
import io.tapstate.runtime.engine.StageTimer;
import java.io.*;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A bounded startup source: only retained handoff identities traverse the resolver's keyed edge. */
public final class NestHandoffRecovery extends AbstractProcessor implements Staged {
    private static final int PAGE_SIZE = 64;

    @FunctionalInterface
    public interface Pages extends Serializable {
        List<NestStateStore.Pending> read(String after, int limit);
    }

    public record Key(Object value) implements Serializable {
        public Key { Objects.requireNonNull(value, "value"); }
    }

    private final Pages pages;
    private final ArrayDeque<NestStateStore.Pending> pending = new ArrayDeque<>();
    private String after;
    private StageTimer timer = StageTimer.none(Stage.NEST);

    public NestHandoffRecovery(Pages pages) { this.pages = Objects.requireNonNull(pages, "pages"); }
    @Override public Stage stage() { return Stage.NEST; }
    @Override public boolean isCooperative() { return false; }
    @Override protected void init(com.hazelcast.jet.core.Processor.Context context) {
        timer = StageTimer.of(stage(), context);
    }

    @Override public boolean complete() {
        long started = timer.begin();
        boolean worked = !pending.isEmpty();
        try {
            if (pending.isEmpty()) {
                var page = pages.read(after, PAGE_SIZE);
                if (page.size() > PAGE_SIZE) throw new IllegalStateException("pending page exceeded its limit");
                if (page.isEmpty()) return true;
                worked = true;
                String previous = after;
                for (var row : page) {
                    if (previous != null && row.cursor().compareTo(previous) <= 0) throw unreadable(null);
                    previous = row.cursor();
                }
                after = previous;
                pending.addAll(page);
            }
            while (!pending.isEmpty()) {
                if (!tryEmit(new Key(decodeKey(pending.peek().key())))) return false;
                pending.remove();
            }
            return false;
        } finally {
            if (worked) timer.end(started);
        }
    }

    /** The original key travels with the pending marker; its canonical cold name is unchanged. */
    public static byte[] encodeKey(Object key) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(key); }
        catch (IOException invalid) { throw new IllegalStateException("could not serialize a Nest key", invalid); }
        return bytes.toByteArray();
    }

    private static Object decodeKey(byte[] bytes) {
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            Object key = in.readObject();
            if (!(key instanceof List<?>)) throw unreadable(null);
            NestStateKeys.nameOf(key);
            return key;
        } catch (IOException | ClassNotFoundException | IllegalArgumentException invalid) { throw unreadable(invalid); }
    }

    static TapstateException unreadable(Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "nest-state", "field", "pending-handoff"), cause);
    }
}
