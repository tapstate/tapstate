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
    @Override public LongConsumer resolve(HazelcastInstance member) {
        var bound = ScopedSrsConsumerAccess.bind(member,run);
        return sequence -> {
            bound.requireClaim(null);
            if (!bound.store().advanceReadSequence(run.context(),run.membershipRevision(),table,sequence,run.originalClaim())) throw ScopedSrsConsumerAccess.refused(run);
        };
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
