package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/** Minimal verified user identity; it contains no credential or external profile fields. */
public record CurrentUserView(String mode, String principal, List<String> scopes) {
    public CurrentUserView {
        mode = Objects.requireNonNull(mode, "mode");
        principal = Objects.requireNonNull(principal, "principal");
        scopes = List.copyOf(scopes);
    }
}
