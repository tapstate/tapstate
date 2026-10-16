package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Publishes one handover with its identifying entry last, so partial writes are not collectible. */
final class NestMigrationWriter {
    private NestMigrationWriter() { }

    record Written(int pieces, long changes) { }

    static Optional<Written> publish(ParkedSubtree.At at, Iterable<NestElement> input,
            NestStore<ParkedSubtree> parking, long migrationBatch, long parkingLimit) {
        if (migrationBatch < 1 || parkingLimit < 1) {
            throw new IllegalArgumentException("migration bounds must be positive");
        }
        Iterator<NestElement> rows = input.iterator();
        if (!rows.hasNext()) return Optional.empty();
        ParkedSubtree held = parking.load(at);
        // Local history disappears on restart. The stored pieces, read one at a time, are the count
        // that decides whether another move fits; a missing promised piece is never an empty one.
        long parked = 0;
        if (held != null) {
            parked = held.changes().size();
            refuse(at, parked, parkingLimit);
            for (int piece = 1; piece <= held.batches(); piece++) {
                parked = Math.addExact(parked, readPiece(parking, at, piece).changes().size());
                refuse(at, parked, parkingLimit);
            }
        }
        int size = (int) Math.min(Integer.MAX_VALUE, Math.min(migrationBatch, parkingLimit));
        List<NestElement> first = held == null ? null : held.changes();
        int pieces = held == null ? 0 : held.batches();
        List<NestElement> chunk = new ArrayList<>();
        while (rows.hasNext()) {
            NestElement row = rows.next();
            parked = Math.addExact(parked, 1L);
            refuse(at, parked, parkingLimit);
            chunk.add(row);
            if (chunk.size() == size) {
                if (first == null) first = List.copyOf(chunk);
                else parking.save(at.piece(++pieces), new ParkedSubtree(chunk));
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) {
            if (first == null) first = List.copyOf(chunk);
            else parking.save(at.piece(++pieces), new ParkedSubtree(chunk));
        }
        // Only this identifying entry makes the pieces collectible. A refusal or a limit failure
        // before it leaves the donor intact and any partial pieces outside the visible handover.
        parking.save(at, new ParkedSubtree(first, pieces), held == null);
        return Optional.of(new Written(pieces, parked));
    }

    static ParkedSubtree readPiece(NestStore<ParkedSubtree> parking, ParkedSubtree.At at, int piece) {
        ParkedSubtree value = parking.load(at.piece(piece));
        if (value == null) {
            throw new TapstateException(NestError.MIGRATION_HANDOVER_UNAVAILABLE,
                    Map.of("address", NestStateKeys.nameOf(at), "piece", piece), null);
        }
        return value;
    }

    static void refuse(ParkedSubtree.At at, long changes, long limit) {
        if (changes > limit) {
            throw new TapstateException(NestError.MIGRATION_PARKING_LIMIT_EXCEEDED,
                    Map.of("address", NestStateKeys.nameOf(at), "changes", changes, "limit", limit), null);
        }
    }
}
