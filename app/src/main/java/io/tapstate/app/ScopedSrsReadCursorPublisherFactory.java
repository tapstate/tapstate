package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.spi.store.SrsConsumerRun;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongConsumer;

/** A reader reports only its original consumer; recovery uses a retained ring floor rather than read-ahead. */
final class ScopedSrsReadCursorPublisherFactory implements SrsReadCursorPublisherFactory {
    private static final long serialVersionUID = 1L;
    private final SrsConsumerRun run;
    private final String table;
    ScopedSrsReadCursorPublisherFactory(SrsConsumerRun run,String table) {
        this.run = Objects.requireNonNull(run,"run"); this.table = Objects.requireNonNull(table,"table");
        if (table.isBlank()) throw new IllegalArgumentException("blank original consumer table");
    }
    @Override public boolean requiresDurableLog() { return true; }
    @Override public LongConsumer resolve(HazelcastInstance member) {
        var bound = ScopedSrsConsumerAccess.bind(member,run);
        return sequence -> {
            bound.requireClaim(null);
            if (!bound.store().advanceReadSequence(run.context(),run.membershipRevision(),table,sequence,run.originalClaim())) throw ScopedSrsConsumerAccess.refused(run);
        };
    }
    @Override public io.tapstate.runtime.srs.SrsEventModelVerifier modelVerifier(HazelcastInstance member,String ring) {
        if (ring == null || ring.isBlank()) throw new IllegalArgumentException("blank original ring");
        var bound = ScopedSrsConsumerAccess.bind(member,run); var models = bound.models().guardedBy(bound.requireActive());
        return (item,sequence) -> {
            bound.requireClaim(null); var progress = bound.progress();
            if (!progress.cursor().perTableSeq().containsKey(table)) throw ScopedSrsConsumerAccess.refused(run);
            if (sequence < 0 || sequence == Long.MAX_VALUE || item.epoch() < 1 || item.epoch() == Long.MAX_VALUE
                    || item.schemaVer() == Long.MAX_VALUE || item.captureFence() == null) throw unavailable(ring,item);
            var expected = new io.tapstate.spi.store.SrsModelKey(run.miningChainId(),table,run.scope().cacheIncarnation(),item.epoch(),item.schemaVer());
            var model = models.requireEvent(expected,ring,sequence,item.captureFence());
            if (model == null || !expected.equals(model.key()) || !ring.equals(model.ringName())) throw unavailable(ring,item);
            bound.requireClaim(null);
        };
    }
    private static io.tapstate.core.common.TapstateException unavailable(String ring,io.tapstate.runtime.srs.SrsItem item) {
        return new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.SRS_EVENT_MODEL_UNAVAILABLE,
                java.util.Map.of("ring",ring,"epoch",String.valueOf(item.epoch()),"version",String.valueOf(item.schemaVer())),null);
    }
    @Override public Optional<ChainPosition> confirmedPosition(HazelcastInstance member) {
        var held = progress(member);
        return Optional.ofNullable(held.cursor().sinkAckedByTable().get(table))
                .filter(position -> position.order().epoch() > 0 && position.order().seq() >= 0 && position.order().seq() < Long.MAX_VALUE);
    }
    private io.tapstate.spi.store.SrsConsumerProgress progress(HazelcastInstance member) {
        var held = ScopedSrsConsumerAccess.bind(member,run).progress();
        if (!held.cursor().perTableSeq().containsKey(table) || !held.ringDoneThrough().containsKey(table)) throw ScopedSrsConsumerAccess.refused(run);
        return held;
    }
    @Override public OptionalLong recoverySequence(HazelcastInstance member) {
        var held = progress(member); var floor = held.ringDoneThrough().get(table);
        if (floor == null) throw ScopedSrsConsumerAccess.refused(run);
        return OptionalLong.of(floor);
    }
}
