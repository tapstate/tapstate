package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The human login flow: verify a username / password pair and, on success, mint a short-lived access
 * token carrying the user's capability grade. The service holds no state — it composes the user store,
 * the password verifier and the token issuer, each a port bound at the assembly root.
 *
 * <p>Two properties are load-bearing and must not be "simplified" away:
 *
 * <ul>
 *   <li><b>No user enumeration.</b> A missing user and a wrong password fail with the same coded
 *       error carrying no argument, so a caller cannot tell an existing username from an absent one.
 *   <li><b>Timing parity.</b> Both failure paths run exactly one password verification — the absent
 *       user is compared against a fixed dummy hash whose result is discarded — so the two cannot be
 *       told apart by how long the call takes.
 * </ul>
 */
public final class LoginService {

    /**
     * A fixed, valid password hash compared against when no user matches, so the absent-user path
     * performs the same one verification as the wrong-password path (timing parity). It is a real
     * hash of a throwaway value — the verification runs the genuine algorithm — and is a credential
     * for no account; its result is always discarded.
     */
    private static final String DUMMY_HASH = "$2a$10$k1wbIrmNyFAPwPVPSVa/zecw2BCEnBwVS2GbrmgzxFUOqW9dk4TCW";

    private final UserStore userStore;
    private final PasswordHasher passwordHasher;
    private final AccessTokenService accessTokens;

    public LoginService(UserStore userStore, PasswordHasher passwordHasher, TokenSigner tokenSigner) {
        this(userStore, passwordHasher, new AccessTokenService(tokenSigner, java.time.Clock.systemUTC()));
    }

    public LoginService(UserStore userStore, PasswordHasher passwordHasher, AccessTokenService accessTokens) {
        this.userStore = Objects.requireNonNull(userStore, "userStore");
        this.passwordHasher = Objects.requireNonNull(passwordHasher, "passwordHasher");
        this.accessTokens = Objects.requireNonNull(accessTokens, "accessTokens");
    }

    /**
     * Verifies the credentials and returns a signed session token on success. Throws the shared
     * {@code control.auth-failed} error — with no identifying argument — when the user is absent or
     * the password does not match.
     */
    public String login(String username, String password) {
        return loginGrant(username, password).token();
    }

    /** Verifies the credentials and returns the full short-lived access-token grant on success. */
    public AccessTokenGrant loginGrant(String username, String password) {
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");

        Optional<User> found = userStore.find(username);
        // Run exactly one verification on every path. When the user is absent the password is checked
        // against a fixed dummy hash and the result discarded, so present and absent cost the same.
        String hashToCheck = found.map(User::passwordHash).orElse(DUMMY_HASH);
        boolean passwordOk = passwordHasher.matches(password, hashToCheck);

        if (found.isEmpty() || !passwordOk) {
            // One code for both cases, no argument: the failure never reveals whether the user exists.
            throw new TapstateException(ControlError.AUTH_FAILED, Map.of(), null);
        }

        Scope scope = scopeOf(found.get().role());
        return accessTokens.issue(username, scope);
    }

    /**
     * Maps a stored role string to its capability grade. Reached only after the password has already
     * verified, so an unrecognized role is not a user authentication outcome but a data-integrity
     * fault seeded into the store: it crashes bare (an invariant violation) rather than being
     * laundered into a user-facing auth error that would hide the defect.
     */
    private static Scope scopeOf(String role) {
        return switch (role.toLowerCase(Locale.ROOT)) {
            case "read" -> Scope.READ;
            case "write" -> Scope.WRITE;
            case "admin" -> Scope.ADMIN;
            default -> throw new IllegalStateException("unrecognized stored user role: " + role);
        };
    }
}
