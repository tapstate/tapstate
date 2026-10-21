package io.tapstate.runtime.engine.nest;

import io.tapstate.runtime.engine.ProcessorBufferBounds;
import java.util.List;
import java.util.Map;

/** Transient staging of the concrete Nest roles, using the limits supplied to their real processors. */
final class NestBufferBounds {
    private NestBufferBounds() { }

    static ProcessorBufferBounds resolver(NestVertex vertex, NestSettings settings) {
        long pending = settings.pendingAllowedIn(vertex.mapName());
        long parked = settings.parkingAllowedIn(vertex.mapName());
        return new ProcessorBufferBounds(Map.of(
                "released-pending-read", pending,
                "released-pending-output", pending,
                "pending-parking-copy", pending,
                "parked-change-read", parked,
                "collected-parked-output", parked,
                "parked-merge-work-list", parked,
                "parked-merge-result-list", parked,
                "moving-key-pair", 2L,
                "folded-mapping-entries", (long) DrainFolding.MAX_KEYS_HELD), Map.of(), List.of(
                "nest cached state entries are governed by entries-in-memory=" + settings.entriesHeldInMemory(),
                "resolver deletion, holding, vacated and takeover bookkeeping is separate from transient row staging",
                "nested payload bytes and durable state are not a total heap guarantee"));
    }

    static ProcessorBufferBounds assembler(NestVertex vertex, List<EmbedSlot> slots, NestSettings settings) {
        long elements = settings.elementsAllowedIn(vertex.mapName());
        long parked = settings.parkingAllowedIn(vertex.mapName());
        long chunk = Math.min(settings.migrationBatchIn(vertex.mapName()), parked);
        // Every live root or element can name one row per compiled reference slot. A folded group
        // queries these together; expired windows query only one document before observing backpressure.
        long references = Math.multiplyExact(Math.multiplyExact((long) DrainFolding.MAX_KEYS_HELD,
                Math.addExact(elements, 1L)), referenceSlots(slots));
        return new ProcessorBufferBounds(Map.of(
                "folded-document-entries", (long) DrainFolding.MAX_KEYS_HELD,
                "folded-output-documents", (long) DrainFolding.MAX_KEYS_HELD,
                "reference-mirror-read", references,
                "migration-identifying-part-read", parked,
                "migration-part-read-or-identifying-copy", parked,
                "migration-working-chunk", chunk,
                "migration-publication-copy", chunk,
                "current-migration-row", 1L,
                "current-rendered-document", 1L), Map.of(), List.of(
                "nest cached state entries are governed by entries-in-memory=" + settings.entriesHeldInMemory(),
                "assembler windows, deleted roots, waiting keys and handover bookkeeping are separate from transient row staging",
                "migration traversal retains cursors and produces one row at a time",
                "nested payload bytes and durable state are not a total heap guarantee"));
    }

    static ProcessorBufferBounds lookup(NestLookup lookup, NestSettings settings) {
        return new ProcessorBufferBounds(Map.of(
                "referrer-wakes", settings.referrersAllowedIn(lookup.mapName()),
                "current-filed-row", 1L,
                "first-filing-and-settlement", 2L), Map.of("lookup-read-ahead-rows", 1L), List.of(
                "lookup reference buckets and filed-key metadata are separate from transient row staging"));
    }

    private static long referenceSlots(List<EmbedSlot> slots) {
        long count = 0;
        for (EmbedSlot slot : slots) {
            count = Math.addExact(count, slot.isReference() ? 1L : referenceSlots(slot.children()));
        }
        return count;
    }
}
