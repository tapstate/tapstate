package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ContextManagerTest {

    @Test
    void createAndEditPreserveStableIdentityAndAuthReference(@TempDir Path home) {
        AtomicInteger ids = new AtomicInteger();
        List<UUID> generated = List.of(
                UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"),
                UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed"));
        ContextManager manager = new ContextManager(ContextConfigStore.underHome(home),
                () -> generated.get(ids.getAndIncrement()));

        ContextDefinition created = manager.create("dev",
                List.of(URI.create("https://tapstate.example.com")), true);
        ContextDefinition edited = manager.edit("dev",
                List.of(URI.create("https://new.example.com")), false);

        assertThat(created.id()).isEqualTo(generated.get(0));
        assertThat(created.authRef()).isEqualTo(generated.get(1));
        assertThat(edited.id()).isEqualTo(created.id());
        assertThat(edited.authRef()).isEqualTo(created.authRef());
        assertThat(edited.seeds()).containsExactly(URI.create("https://new.example.com"));
        assertThat(edited.tls().verify()).isFalse();
        assertThat(ContextConfigStore.underHome(home).load().contexts()).containsEntry("dev", edited);
    }

    @Test
    void bindingIsCanonicalAndExactAndNeverInherited(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectories(home.resolve("real/orders"));
        Path child = Files.createDirectory(workspace.resolve("pipeline"));
        Path alias = home.resolve("orders-link");
        Files.createSymbolicLink(alias, workspace);
        ContextManager manager = new ContextManager(ContextConfigStore.underHome(home));
        manager.create("dev", List.of(URI.create("https://tapstate.example.com")), true);

        manager.bind(alias, "dev");

        assertThat(manager.contextBoundExactlyTo(workspace)).contains("dev");
        assertThat(manager.contextBoundExactlyTo(alias)).contains("dev");
        assertThat(manager.contextBoundExactlyTo(child)).isEmpty();
        assertThat(manager.contextBoundExactlyTo(workspace.getParent())).isEmpty();
        assertThat(manager.unbind(alias)).isEqualTo(Optional.of("dev"));
        assertThat(manager.contextBoundExactlyTo(workspace)).isEmpty();
    }

    @Test
    void lastContextOnlyOrdersSuggestionsAndDeleteExposesImpact(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextManager manager = new ContextManager(ContextConfigStore.underHome(home));
        manager.create("alpha", List.of(URI.create("https://alpha.example.com")), true);
        ContextDefinition dev = manager.create("dev", List.of(URI.create("https://dev.example.com")), true);
        manager.bind(workspace, "dev");

        assertThat(manager.contextBoundExactlyTo(home)).isEmpty();
        manager.choose("dev");
        assertThat(manager.suggestions()).extracting(ContextManager.ContextChoice::name)
                .containsExactly("dev", "alpha");
        assertThat(manager.contextBoundExactlyTo(home)).isEmpty();

        ContextManager.DeletionImpact impact = manager.previewDelete("dev");
        assertThat(impact.authRef()).isEqualTo(dev.authRef());
        assertThat(impact.workspaceBindings()).containsExactly(workspace.toRealPath());
        manager.delete("dev");
        ContextConfig remaining = ContextConfigStore.underHome(home).load();
        assertThat(remaining.contexts()).containsOnlyKeys("alpha");
        assertThat(remaining.workspaceBindings()).isEmpty();
        assertThat(remaining.lastContext()).isNull();
    }
}
