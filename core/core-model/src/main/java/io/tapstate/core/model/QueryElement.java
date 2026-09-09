package io.tapstate.core.model;

import java.util.Objects;

/**
 * One {@code serve.query} element (§8). No {@code backend} = parallel egress
 * from the view store (A); {@code backend: <sync-id>} = API on sink (B).
 */
@Doc("One query exposed by a serve resource, either parallel egress from the view store or an API served on a sink.")
public record QueryElement(
        @Doc(value = "The kind of query this element exposes.", required = true)
        QueryType type,
        @Doc("The sync id whose sink serves this query as an API; omit for parallel egress from the view store.")
        String backend) {

    public QueryElement {
        Objects.requireNonNull(type, "type");
    }
}
