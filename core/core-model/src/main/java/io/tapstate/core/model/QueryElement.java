package io.tapstate.core.model;

import java.util.Objects;

/**
 * One {@code serve.query} element. No {@code backend} means parallel egress from the view store;
 * {@code backend: <sync-id>} was to mean an API served on that sink.
 *
 * <p><strong>{@code backend} has no consumer today.</strong> The grammar accepts it and the canonical
 * writer emits it back, and that is the whole of what happens: nothing resolves the sync id it names,
 * nothing validates it, and no runtime reads it. So a document may carry a {@code backend} naming a
 * sync element that does not exist, and be accepted. The rule that would refuse that is not written,
 * deliberately: it would guard a capability that is not built, and the question of whether the
 * API-on-sink form belongs in this grammar version at all is a product one, open at the time of
 * writing. Whoever settles it either builds the form and adds the rule, or removes the field.
 */
@Doc("One query exposed by a serve resource. Today only parallel egress from the view store is served.")
public record QueryElement(
        @Doc(value = "The kind of query this element exposes.", required = true)
        QueryType type,
        @Doc("Reserved: the sync id whose sink would serve this query as an API. Nothing reads it yet — "
                + "it is accepted, stored and written back unchanged, and naming a sync that does not "
                + "exist is not refused. Omit it for parallel egress from the view store.")
        String backend) {

    public QueryElement {
        Objects.requireNonNull(type, "type");
    }
}
