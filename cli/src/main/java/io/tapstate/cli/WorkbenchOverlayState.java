package io.tapstate.cli;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** The single overlay that owns input before the active view and root shortcuts. */
sealed interface WorkbenchOverlayState
        permits WorkbenchOverlayState.More,
                WorkbenchOverlayState.ContextPicker,
                WorkbenchOverlayState.Login,
                WorkbenchOverlayState.Help {

    record More(int selectedIndex) implements WorkbenchOverlayState {
        public More {
            if (selectedIndex < 0 || selectedIndex > 1) {
                throw new IllegalArgumentException("More selection is outside the menu");
            }
        }
    }

    record ContextPicker(
            List<WorkbenchActionGateway.ContextOption> contexts,
            int selectedIndex,
            boolean pending,
            Optional<String> message) implements WorkbenchOverlayState {
        public ContextPicker {
            contexts = List.copyOf(contexts);
            Objects.requireNonNull(message, "message");
            if (contexts.isEmpty() ? selectedIndex != -1 : selectedIndex < 0 || selectedIndex >= contexts.size()) {
                throw new IllegalArgumentException("Context selection is outside the menu");
            }
        }

        ContextPicker select(int index) {
            if (pending || contexts.isEmpty()) {
                return this;
            }
            int selected = Math.clamp(index, 0, contexts.size() - 1);
            return selected == selectedIndex ? this : new ContextPicker(contexts, selected, false, message);
        }

        ContextPicker asPending() {
            return new ContextPicker(contexts, selectedIndex, true, Optional.of("Connecting..."));
        }
    }

    record Login(
            String contextName,
            Stage stage,
            String username,
            SecretBuffer password,
            boolean pending,
            Optional<String> message) implements WorkbenchOverlayState {
        public Login {
            Objects.requireNonNull(contextName, "contextName");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
            Objects.requireNonNull(message, "message");
        }

        enum Stage {
            USERNAME,
            PASSWORD
        }
    }

    enum Help implements WorkbenchOverlayState {
        INSTANCE
    }
}
