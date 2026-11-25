package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/** Current authenticated identity and optional display attributes. */
public record CurrentUserView(String mode, String principal, List<String> scopes, String username, String email) {
    public CurrentUserView(String mode, String principal, List<String> scopes) {
        this(mode, principal, scopes, null, null);
    }
    public CurrentUserView {
        mode = Objects.requireNonNull(mode, "mode");
        principal = Objects.requireNonNull(principal, "principal");
        scopes = List.copyOf(scopes);
    }
}
