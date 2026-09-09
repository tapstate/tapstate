package io.tapstate.core.model;

/** Pull-side publish surface type (§8); runtime = GA. */
@Doc("The kind of pull-based query surface a served resource exposes to consumers.")
public enum QueryType {
    @Doc("A RESTful HTTP endpoint queried over standard request/response semantics.")
    REST("rest"),
    @Doc("A GraphQL endpoint where consumers shape responses with their own queries.")
    GRAPHQL("graphql"),
    @Doc("A Model Context Protocol endpoint exposing the resource to AI tools and agents.")
    MCP("mcp");

    private final String yaml;

    QueryType(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
