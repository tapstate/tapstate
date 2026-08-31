package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContextResolverTest {

    @Test
    void resolvesOnlyTheStrictSourcePrecedence(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders")).toRealPath();
        ContextConfig config = config(workspace, "bound", "recent");
        ContextResolver resolver = new ContextResolver(() -> config,
                name -> "TAPSTATE_CONTEXT".equals(name) ? "environment" : null);

        assertThat(resolver.resolve("temporary:8080", null, workspace))
                .contains(new ResolvedContext.Temporary("temporary:8080"));
        assertNamed(resolver.resolve(null, "explicit", workspace), "explicit",
                ResolvedContext.Source.EXPLICIT);
        assertNamed(resolver.resolve(null, null, workspace), "environment",
                ResolvedContext.Source.ENVIRONMENT);

        ContextResolver noEnvironment = new ContextResolver(() -> config, name -> null);
        assertNamed(noEnvironment.resolve(null, null, workspace), "bound",
                ResolvedContext.Source.WORKSPACE_BINDING);
    }

    @Test
    void temporaryConnectDoesNotLoadOrPersistContextState(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        AtomicInteger reads = new AtomicInteger();
        ContextResolver resolver = new ContextResolver(() -> {
            reads.incrementAndGet();
            return config(null, null, "recent");
        }, name -> "environment");

        assertThat(resolver.resolve("temporary:8080", null, workspace))
                .contains(new ResolvedContext.Temporary("temporary:8080"));
        assertThat(reads).hasValue(0);
        assertThat(home.resolve(".tapstate/config.yaml")).doesNotExist();
    }

    @Test
    void connectAndContextConflictWithoutReadingOrWritingConfig(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextConfigStore store = ContextConfigStore.underHome(home);
        store.save(config(workspace.toRealPath(), "bound", "recent"));
        String before = Files.readString(home.resolve(".tapstate/config.yaml"));
        ContextResolver resolver = new ContextResolver(store, name -> null);

        assertThatThrownBy(() -> resolver.resolve("temporary:8080", "explicit", workspace))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-source-conflict"));
        assertThat(Files.readString(home.resolve(".tapstate/config.yaml"))).isEqualTo(before);
    }

    @Test
    void exactCanonicalBindingNeverInheritsToParentOrChild(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectories(home.resolve("real/orders"));
        Path child = Files.createDirectory(workspace.resolve("pipeline"));
        Path alias = home.resolve("orders-link");
        Files.createSymbolicLink(alias, workspace);
        ContextConfig config = config(workspace.toRealPath(), "bound", "recent");
        ContextResolver resolver = new ContextResolver(() -> config, name -> null);

        assertNamed(resolver.resolve(null, null, workspace), "bound",
                ResolvedContext.Source.WORKSPACE_BINDING);
        assertNamed(resolver.resolve(null, null, alias), "bound",
                ResolvedContext.Source.WORKSPACE_BINDING);
        assertThat(resolver.resolve(null, null, child)).isEmpty();
        assertThat(resolver.resolve(null, null, workspace.getParent())).isEmpty();
    }

    @Test
    void lastContextIsNeverAResolverFallback(@TempDir Path home) throws IOException {
        Path unbound = Files.createDirectory(home.resolve("unbound"));
        ContextResolver resolver = new ContextResolver(() -> config(null, null, "recent"), name -> null);

        assertThat(resolver.resolve(null, null, unbound)).isEmpty();
    }

    @Test
    void aNamedSourceMustNameAnExistingContext(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextResolver resolver = new ContextResolver(() -> config(null, null, "recent"),
                name -> "missing");

        assertThatThrownBy(() -> resolver.resolve(null, null, workspace))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-not-found"));
    }

    @Test
    void anExplicitUnknownContextFailsWithTheSameCodedDiagnostic(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextResolver resolver = new ContextResolver(() -> config(null, null, "recent"),
                name -> null);

        assertThatThrownBy(() -> resolver.resolve(null, "missing", workspace))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.context-not-found"));
    }

    private static void assertNamed(Optional<ResolvedContext> resolution, String name,
                                    ResolvedContext.Source source) {
        assertThat(resolution).containsInstanceOf(ResolvedContext.Named.class);
        ResolvedContext.Named named = (ResolvedContext.Named) resolution.orElseThrow();
        assertThat(named.name()).isEqualTo(name);
        assertThat(named.source()).isEqualTo(source);
    }

    private static ContextConfig config(Path workspace, String binding, String lastContext) {
        Map<String, ContextDefinition> contexts = Map.of(
                "explicit", definition("explicit", 1),
                "environment", definition("environment", 2),
                "bound", definition("bound", 3),
                "recent", definition("recent", 4));
        Map<String, String> bindings = workspace == null ? Map.of() : Map.of(workspace.toString(), binding);
        return new ContextConfig(ContextConfig.CURRENT_VERSION, lastContext, contexts, bindings);
    }

    private static ContextDefinition definition(String name, int suffix) {
        return new ContextDefinition(
                UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff" + suffix),
                List.of(URI.create("https://" + name + ".example.com")),
                new ContextTls(true),
                UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed".substring(0, 35) + suffix));
    }
}
