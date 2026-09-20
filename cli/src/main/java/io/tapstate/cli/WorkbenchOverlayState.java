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
                WorkbenchOverlayState.SourceYamlEditor,
                WorkbenchOverlayState.PipelineCreate,
                WorkbenchOverlayState.PipelineYamlEditor,
                WorkbenchOverlayState.Confirm,
                WorkbenchOverlayState.Login,
                WorkbenchOverlayState.Actions,
                WorkbenchOverlayState.LogLevel,
                WorkbenchOverlayState.Help {

    record LogLevel(String pipelineId, int selectedIndex) implements WorkbenchOverlayState {
        static final List<String> LEVELS = List.of("ERROR", "WARN", "INFO", "DEBUG", "TRACE");

        public LogLevel {
            Objects.requireNonNull(pipelineId, "pipelineId");
            if (selectedIndex < 0 || selectedIndex >= LEVELS.size()) {
                throw new IllegalArgumentException("Log level selection is outside the menu");
            }
        }

        LogLevel select(int index) {
            return new LogLevel(pipelineId, Math.clamp(index, 0, LEVELS.size() - 1));
        }

        String selectedLevel() {
            return LEVELS.get(selectedIndex);
        }
    }

    record More(int selectedIndex) implements WorkbenchOverlayState {
        static final List<String> ENTRIES = List.of(
                "🧭  Context", "🔐  Authentication", "📜  Logs", "🔎  Inspect", "?  Help");

        public More {
            if (selectedIndex < 0 || selectedIndex >= ENTRIES.size()) {
                throw new IllegalArgumentException("More selection is outside the menu");
            }
        }

        More select(int index) {
            return new More(Math.clamp(index, 0, ENTRIES.size() - 1));
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

    record SourceYamlEditor(
            SourceCreate source,
            WorkbenchWorkspaceState.Document document) implements WorkbenchOverlayState {
        public SourceYamlEditor {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(document, "document");
        }
    }

    record PipelineCreate(
            List<String> sourceIds,
            Stage stage,
            int selectedIndex,
            String sourceId,
            String id,
            Optional<String> canonicalYaml,
            boolean pending,
            Optional<String> message) implements WorkbenchOverlayState {
        public PipelineCreate {
            sourceIds = List.copyOf(sourceIds);
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(sourceId, "sourceId");
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(canonicalYaml, "canonicalYaml");
            Objects.requireNonNull(message, "message");
        }

        enum Stage {
            SOURCE,
            ID,
            PREVIEW
        }
    }

    record PipelineYamlEditor(
            PipelineCreate pipeline,
            WorkbenchWorkspaceState.Document document) implements WorkbenchOverlayState {
        public PipelineYamlEditor {
            Objects.requireNonNull(pipeline, "pipeline");
            Objects.requireNonNull(document, "document");
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

        sealed interface Intent permits Intent.DeleteContext, Intent.CreateSource, Intent.CreatePipeline,
                Intent.ApplySources, Intent.ApplyPipelines, Intent.ChangePipelineLifecycle,
                Intent.Quit, Intent.DiscardChanges, Intent.DiscardSourceYaml, Intent.DiscardPipelineYaml {
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

            record CreatePipeline(WorkbenchActionGateway.PipelineCreateRequest request) implements Intent {
                public CreatePipeline {
                    Objects.requireNonNull(request, "request");
                }
            }

            record ApplySources(WorkbenchActionGateway.SourceApplyRequest request) implements Intent {
                public ApplySources {
                    Objects.requireNonNull(request, "request");
                }
            }

            record ApplyPipelines(WorkbenchActionGateway.PipelineApplyRequest request) implements Intent {
                public ApplyPipelines {
                    Objects.requireNonNull(request, "request");
                }
            }

            record ChangePipelineLifecycle(WorkbenchActionGateway.PipelineLifecycleRequest request) implements Intent {
                public ChangePipelineLifecycle {
                    Objects.requireNonNull(request, "request");
                }
            }

            enum Quit implements Intent {
                INSTANCE
            }

            enum DiscardChanges implements Intent {
                INSTANCE
            }

            record DiscardSourceYaml(SourceYamlEditor editor) implements Intent {
                public DiscardSourceYaml {
                    Objects.requireNonNull(editor, "editor");
                }
            }

            record DiscardPipelineYaml(PipelineYamlEditor editor) implements Intent {
                public DiscardPipelineYaml {
                    Objects.requireNonNull(editor, "editor");
                }
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

    record Actions(List<Action> actions, int selectedIndex, Optional<String> message) implements WorkbenchOverlayState {
        public Actions {
            actions = List.copyOf(actions);
            Objects.requireNonNull(message, "message");
            if (actions.isEmpty() || selectedIndex < 0 || selectedIndex >= actions.size()) {
                throw new IllegalArgumentException("Action selection is outside the menu");
            }
        }

        Actions(List<Action> actions, int selectedIndex) {
            this(actions, selectedIndex, Optional.empty());
        }

        Actions select(int index) {
            int selected = Math.clamp(index, 0, actions.size() - 1);
            return selected == selectedIndex ? this : new Actions(actions, selected, message);
        }

        enum Action {
            CONTEXT("🧭  Context", "Choose or create a context"),
            AUTHENTICATION("🔐  Authentication", "Sign in to the selected server"),
            NEW_SOURCE("✨  New Source", "Create a local source artifact"),
            NEW_PIPELINE("⚡  New Pipeline", "Create a local pipeline artifact"),
            APPLY_SELECTED_PIPELINE("☁️  Apply Selected Pipeline", "Synchronize the selected local pipeline"),
            START_PIPELINE("▶  Start Pipeline", "Start the selected remote pipeline"),
            PAUSE_PIPELINE("Ⅱ  Pause Pipeline", "Pause the selected remote pipeline"),
            RESUME_PIPELINE("▶  Resume Pipeline", "Resume the selected remote pipeline"),
            STOP_PIPELINE("■  Stop Pipeline", "Stop the selected remote pipeline"),
            APPLY_SELECTED_SOURCE("☁️  Apply Selected Source", "Synchronize the selected local source"),
            APPLY_WORKSPACE_SOURCES("☁️  Apply Workspace Sources", "Synchronize all valid local sources"),
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
