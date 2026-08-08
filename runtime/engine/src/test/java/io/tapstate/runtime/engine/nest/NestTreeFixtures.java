package io.tapstate.runtime.engine.nest;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Nest trees written so a test reads as a tree rather than as a wall of constructor arguments. */
final class NestTreeFixtures {

    private NestTreeFixtures() {
    }

    /** A nest tree rooted at {@code alias} and keyed by {@code key}, with the given embeds hanging off it. */
    static TransformBody.Nest nest(String alias, List<String> key, Embed... embeds) {
        return nest(new NestRoot(alias, key, null, null, List.of(embeds)));
    }

    /** A nest tree over a root built by the caller, for the cases that need a mode or key tracking on it. */
    static TransformBody.Nest nest(NestRoot root) {
        return new TransformBody.Nest(null, null, root);
    }

    /** An embed of {@code alias}'s rows, joining its own {@code childField} onto the parent's {@code parentField}. */
    static Embed embed(String alias, String childField, String parentField, EmbedAs as, String path,
            List<String> arrayKey, Embed... children) {
        return new Embed(alias, Map.of(childField, parentField), as, path, arrayKey, null, null,
                children.length == 0 ? null : List.of(children));
    }

    /**
     * An embed joining on several columns at once, given as child/parent pairs in the order they are
     * written. The order is the point of the fixture: a composite key has to line up with the parent's
     * regardless of which order the author happened to declare it in.
     */
    static Embed compositeEmbed(String alias, String path, List<String> arrayKey, String... onPairs) {
        Map<String, String> on = new LinkedHashMap<>();
        for (int i = 0; i < onPairs.length; i += 2) {
            on.put(onPairs[i], onPairs[i + 1]);
        }
        return new Embed(alias, on, EmbedAs.ARRAY, path, arrayKey, null, null, null);
    }

    /** The same embed with key tracking turned on, for the cases about the append-mode conflict. */
    static Embed tracking(Embed embed) {
        return new Embed(embed.from(), embed.on(), embed.as(), embed.path(), embed.arrayKey(),
                embed.ignoreUpdates(), true, embed.embed());
    }

    /**
     * The table behind each fixture alias and the key it declares. An alias absent from this map is a
     * stream the tree never names, and asking for one is a wiring bug rather than an authoring error.
     */
    static Function<String, NestTable> tables() {
        Map<String, NestTable> known = new LinkedHashMap<>();
        known.put("customer", new NestTable("customers", List.of("customer_id")));
        known.put("policy", new NestTable("policies", List.of("policy_id")));
        known.put("claim", new NestTable("claims", List.of("claim_id")));
        known.put("document", new NestTable("documents", List.of("document_id")));
        known.put("order", new NestTable("orders", List.of("order_id")));
        known.put("item", new NestTable("items", List.of("item_id")));
        known.put("profile", new NestTable("profiles", List.of("customer_id")));
        known.put("keyless", new NestTable("keyless_rows", List.of()));
        return known::get;
    }
}
