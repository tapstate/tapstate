package io.tapstate.cli;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** The single overlay that owns input before the active view and root shortcuts. */
sealed interface WorkbenchOverlayState
        permits WorkbenchOverlayState.More,
                WorkbenchOverlayState.ContextPicker,
                WorkbenchOverlayState.ContextCreate,
                WorkbenchOverlayState.Confirm,
                WorkbenchOverlayState.Login,
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

        sealed interface Intent permits Intent.DeleteContext, Intent.DiscardChanges {
            record DeleteContext(String contextName) implements Intent {
                public DeleteContext {
                    Objects.requireNonNull(contextName, "contextName");
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

    enum Help implements WorkbenchOverlayState {
        INSTANCE
    }
}
