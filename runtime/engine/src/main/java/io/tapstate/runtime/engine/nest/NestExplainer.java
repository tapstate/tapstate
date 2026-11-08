package io.tapstate.runtime.engine.nest;

import io.tapstate.core.lifecycle.NestTree;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reads what compiling a nest step inferred into a {@link NestTree}: the declared embeds walked side by side
 * with the slots the compiler built for them, which say whether each one is owned or points at a shared row.
 * The two lists are built from the same declared embeds in the same order, so walking them together pairs
 * each embed with its own slot.
 */
public final class NestExplainer {

    private NestExplainer() {
    }

    /** The tree of {@code step}, compiled as {@code topology}, under the limits {@code settings} sets. */
    public static NestTree tree(String pipelineId, String step, TransformBody.Nest nest, NestTopology topology,
            NestSettings settings) {
        Objects.requireNonNull(nest, "nest");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(settings, "settings");
        if (topology.isPassthrough()) {
            return new NestTree(pipelineId, step, nest.root().from(), keyOf(nest.root().key()), 0L, List.of());
        }
        NestVertex assembler = topology.assembler();
        long elementLimit = nest.maxElementsPerDocument() != null ? nest.maxElementsPerDocument()
                : settings.elementsAllowedIn(assembler.mapName());
        return new NestTree(pipelineId, step, nest.root().from(), assembler.partitionKey(), elementLimit,
                levels(nest.root().embed(), topology.slots(), "", settings));
    }

    private static List<NestTree.Level> levels(List<Embed> embeds, List<EmbedSlot> slots, String above,
            NestSettings settings) {
        List<NestTree.Level> levels = new ArrayList<>();
        if (embeds == null) {
            return levels;
        }
        for (int i = 0; i < embeds.size(); i++) {
            Embed embed = embeds.get(i);
            EmbedSlot slot = i < slots.size() ? slots.get(i) : null;
            String path = embed.as() == EmbedAs.FLAT || embed.path() == null ? above
                    : above.isEmpty() ? embed.path() : above + "." + embed.path();
            boolean owned = slot == null || !slot.isReference();
            long referrers = owned ? 0L : settings.referrersAllowedIn(slot.lookupMap());
            levels.add(new NestTree.Level(path, embed.from(), embed.as().yaml(), owned,
                    embed.on() == null ? Map.of() : embed.on(), embed.key(), referrers,
                    levels(embed.embed(), slot == null ? List.of() : slot.children(), path, settings)));
        }
        return levels;
    }

    private static List<String> keyOf(List<String> key) {
        return key == null ? List.of() : key;
    }
}
