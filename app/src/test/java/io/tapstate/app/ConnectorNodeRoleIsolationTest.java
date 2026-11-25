package io.tapstate.app;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Accepted source and sink node names must retain separate private connector identities. */
class ConnectorNodeRoleIsolationTest {
    @Test
    void sourceAndViewWithTheSameLogicalIdKeepSeparatePrivateNotes() {
        assertDistinctNotes("""
                view:
                  id: shared
                  from: shared.orders
                  primary_key: id
                """);
    }

    @Test
    void sourceAndSyncWithTheSameLogicalIdKeepSeparatePrivateNotes() {
        assertDistinctNotes("""
                serve:
                  id: publish
                  from: shared.orders
                  sync:
                    - id: shared
                      source: shared
                """);
    }

    private static void assertDistinctNotes(String sink) {
        DslParser parser = new DslParser();
        Resource source = parser.parse("""
                version: tapstate/v1
                kind: source
                id: shared
                connector: mock
                mode: snapshot
                tables: [orders]
                """);
        Resource pipeline = parser.parse("version: tapstate/v1\nkind: pipeline\nid: role_flow\nsource: shared\n" + sink);
        List<Resource> resources = List.of(source, pipeline);
        assertThat(Workspace.of(resources)).isNotNull();
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().saveAll(resources);
        Set<String> notes = new LinkedHashSet<>();
        new StoreBackedDagSource(store).stateHeldBy("role_flow").forEach(holding -> holding.namespaces().stream()
                .filter(namespace -> namespace.startsWith("pdk.state.")).forEach(notes::add));
        assertThat(notes).as("source and sink are distinct connector nodes even when their logical names match").hasSize(2);
    }
}
