package io.tapstate.cli;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** The single overlay that owns input before the active view and root shortcuts. */
sealed interface WorkbenchOverlayState
        permits WorkbenchOverlayState.More,
                WorkbenchOverlayState.ContextPicker,
                WorkbenchOverlayState.ContextCreate,
                WorkbenchOverlayState.SourceCreate,
                WorkbenchOverlayState.Confirm,
                WorkbenchOverlayState.Login,
                WorkbenchOverlayState.Actions,
                WorkbenchOverlayState.Help {

    record More(int selectedIndex) implements WorkbenchOverlayState {
        public More {
            if (selectedIndex < 0 || selectedIndex > 2) {
                throw new IllegalArgumentException("More selection is outside the menu");
            }
        }
    }

    record ContextPicker(
            List<WorkbenchActionGateway.ContextOption> contexts,
            int selectedIndex,
            boolean pending,
            Optional<String> message,
            Optional<WorkbenchOverlayState> previous) implements WorkbenchOverlayState {
        public ContextPicker {
            contexts = List.copyOf(contexts);
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(previous, "previous");
            if (selectedIndex < 0 || selectedIndex > contexts.size()) {
                throw new IllegalArgumentException("Context selection is outside the menu");
            }
        }

        ContextPicker(
                List<WorkbenchActionGateway.ContextOption> contexts,
                int selectedIndex,
                boolean pending,
                Optional<String> message) {
            this(contexts, selectedIndex, pending, message, Optional.empty());
        }

        ContextPicker select(int index) {
            if (pending) {
                return this;
            }
            int selected = Math.clamp(index, 0, contexts.size());
            return selected == selectedIndex
                    ? this
                    : new ContextPicker(contexts, selected, false, message, previous);
        }

        ContextPicker asPending() {
            return new ContextPicker(
                    contexts, selectedIndex, true, Optional.of("Connecting..."), previous);
        }
    }

    record ContextCreate(
            Stage stage,
            String name,
            String server,
            boolean verifyTls,
            boolean pending,
            Optional<String> message,
            Optional<WorkbenchOverlayState> previous) implements WorkbenchOverlayState {
        public ContextCreate {
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(server, "server");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(previous, "previous");
        }

        ContextCreate(
                Stage stage,
                String name,
                String server,
                boolean verifyTls,
                boolean pending,
                Optional<String> message) {
            this(stage, name, server, verifyTls, pending, message, Optional.empty());
        }

        enum Stage {
            NAME,
            SERVER,
            VERIFY_TLS
        }
    }

    record SourceCreate(
            WorkbenchActionGateway.SourceCatalog catalog,
            Stage stage,
            int selectedIndex,
            String filter,
            String connector,
            String mode,
            String tables,
            String id,
            Map<String, String> config,
            Optional<String> canonicalYaml,
            boolean pending,
            Optional<String> message) implements WorkbenchOverlayState {
        public SourceCreate {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(filter, "filter");
            Objects.requireNonNull(connector, "connector");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(tables, "tables");
            Objects.requireNonNull(id, "id");
            config = Map.copyOf(config);
            Objects.requireNonNull(canonicalYaml, "canonicalYaml");
            Objects.requireNonNull(message, "message");
        }

        SourceCreate(
                WorkbenchActionGateway.SourceCatalog catalog,
                Stage stage,
                int selectedIndex,
                String filter,
                String connector,
                String mode,
                String tables,
                String id,
                Optional<String> canonicalYaml,
                boolean pending,
                Optional<String> message) {
            this(catalog, stage, selectedIndex, filter, connector, mode, tables, id, Map.of(), canonicalYaml, pending, message);
        }

        SourceCreate(
                WorkbenchActionGateway.SourceCatalog catalog,
                Stage stage,
                int selectedIndex,
                String connector,
                String mode,
                String tables,
                String id,
                Optional<String> canonicalYaml,
                boolean pending,
                Optional<String> message) {
            this(catalog, stage, selectedIndex, "", connector, mode, tables, id, Map.of(), canonicalYaml, pending, message);
        }

        enum Stage {
            CONNECTOR,
            MODE,
            TABLES,
            CONFIG,
            ID,
            PREVIEW
        }
    }

    record Confirm(
            Intent intent,
            String title,
            String message,
            boolean pending,
            Optional<WorkbenchOverlayState> previous) implements WorkbenchOverlayState {
        public Confirm {
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(title, "title");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(previous, "previous");
        }

        Confirm asPending() {
            return new Confirm(intent, title, message, true, previous);
        }

        sealed interface Intent permits Intent.DeleteContext, Intent.CreateSource, Intent.DiscardChanges {
            record DeleteContext(String contextName) implements Intent {
                public DeleteContext {
                    Objects.requireNonNull(contextName, "contextName");
                }
            }

            record CreateSource(WorkbenchActionGateway.SourceCreateRequest request) implements Intent {
                public CreateSource {
                    Objects.requireNonNull(request, "request");
                }
            }

            enum DiscardChanges implements Intent {
                INSTANCE
            }
        }
    }

    record Login(
            String contextName,
            Stage stage,
            String server,
            String username,
            SecretBuffer password,
            boolean pending,
            Optional<String> message,
            Optional<WorkbenchOverlayState> previous) implements WorkbenchOverlayState {
        public Login {
            Objects.requireNonNull(contextName, "contextName");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(server, "server");
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(previous, "previous");
        }

        Login(
                String contextName,
                Stage stage,
                String server,
                String username,
                SecretBuffer password,
                boolean pending,
                Optional<String> message) {
            this(contextName, stage, server, username, password, pending, message, Optional.empty());
        }

        enum Stage {
            SERVER,
            USERNAME,
            PASSWORD
        }
    }

    record Actions(List<Action> actions, int selectedIndex) implements WorkbenchOverlayState {
        public Actions {
            actions = List.copyOf(actions);
            if (actions.isEmpty() || selectedIndex < 0 || selectedIndex >= actions.size()) {
                throw new IllegalArgumentException("Action selection is outside the menu");
            }
        }

        Actions select(int index) {
            int selected = Math.clamp(index, 0, actions.size() - 1);
            return selected == selectedIndex ? this : new Actions(actions, selected);
        }

        enum Action {
            CONTEXT("🧭  Context", "Choose or create a context"),
            AUTHENTICATION("🔐  Authentication", "Sign in to the selected server"),
            NEW_SOURCE("✨  New Source", "Create a local source artifact"),
            REFRESH("↻  Refresh", "Load the latest workspace snapshot"),
            SHELL(">_  Shell (F6)", "Open the embedded command session");

            private final String label;
            private final String description;

            Action(String label, String description) {
                this.label = label;
                this.description = description;
            }

            String label() {
                return label;
            }

            String description() {
                return description;
            }
        }
    }

    enum Help implements WorkbenchOverlayState {
        INSTANCE
    }
}
