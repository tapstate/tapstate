package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The human login flow: look a user up, verify the presented password, and on success mint a session
 * token carrying the user's capability grade. The two failure modes — no such user, and a wrong
 * password — must be indistinguishable, both to the caller (one shared error code carrying no
 * identifying argument) and to a timing attacker (both paths run exactly one password verification).
 * An unrecognized stored role is a data fault, not a login outcome, and crashes bare rather than
 * being dressed up as an authentication failure.
 *
 * <p>All three collaborators are test fakes: a role is stored against a deterministic fake hash, the
 * fake verifier counts its calls, and the fake signer round-trips its issued token so the minted
 * token's subject and scope can be asserted.
 */
class LoginServiceTest {

    @Test
    void wrongPasswordIsRejectedWithTheAuthFailedCodeAndNoLeak() {
        FakeUserStore users = new FakeUserStore();
        CountingHasher hasher = new CountingHasher();
        users.save(new User("alice", hasher.hash("correct"), "admin"));
        LoginService login = new LoginService(users, hasher, new FakeSigner());

        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> login.login("alice", "wrong"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("TS0210002");
        // No argument may reveal whether the username existed or which check failed.
        assertThat(thrown.args()).isEmpty();
    }

    @Test
    void unknownUserIsRejectedWithTheExactSameCodeAsAWrongPassword() {
        FakeUserStore users = new FakeUserStore();
        CountingHasher hasher = new CountingHasher();
        users.save(new User("alice", hasher.hash("correct"), "admin"));
        LoginService login = new LoginService(users, hasher, new FakeSigner());

        TapstateException wrongPassword = catchThrowableOfType(TapstateException.class,
                () -> login.login("alice", "wrong"));
        TapstateException unknownUser = catchThrowableOfType(TapstateException.class,
                () -> login.login("nobody", "whatever"));

        assertThat(unknownUser).isNotNull();
        assertThat(unknownUser.code().code()).isEqualTo("TS0210002");
        assertThat(unknownUser.args()).isEmpty();
        // Identical code for both: an attacker learns only "authentication failed", never existence.
        assertThat(unknownUser.code().code()).isEqualTo(wrongPassword.code().code());
    }

    @Test
    void bothFailurePathsRunExactlyOnePasswordVerification() {
        FakeUserStore users = new FakeUserStore();
        CountingHasher hasher = new CountingHasher();
        users.save(new User("alice", hasher.hash("correct"), "admin"));
        LoginService login = new LoginService(users, hasher, new FakeSigner());

        hasher.matchCalls = 0;
        catchThrowableOfType(TapstateException.class, () -> login.login("alice", "wrong"));
        assertThat(hasher.matchCalls).as("wrong-password path runs one verification").isEqualTo(1);

        hasher.matchCalls = 0;
        catchThrowableOfType(TapstateException.class, () -> login.login("nobody", "whatever"));
        assertThat(hasher.matchCalls).as("absent-user path also runs one verification").isEqualTo(1);
    }

    @Test
    void correctCredentialsMintATokenThatRoundTripsToTheSubjectAndScope() {
        FakeUserStore users = new FakeUserStore();
        CountingHasher hasher = new CountingHasher();
        users.save(new User("alice", hasher.hash("correct"), "write"));
        FakeSigner signer = new FakeSigner();
        LoginService login = new LoginService(users, hasher, signer);

        String token = login.login("alice", "correct");

        assertThat(token).isNotBlank();
        Optional<VerifiedToken> verified = signer.verify(token);
        assertThat(verified).contains(new VerifiedToken("alice", Scope.WRITE));
    }

    @Test
    void theStoredRoleIsMappedToItsGradeCaseInsensitively() {
        assertThat(scopeFor("read")).isEqualTo(Scope.READ);
        assertThat(scopeFor("write")).isEqualTo(Scope.WRITE);
        assertThat(scopeFor("admin")).isEqualTo(Scope.ADMIN);
        assertThat(scopeFor("ADMIN")).isEqualTo(Scope.ADMIN);
        assertThat(scopeFor("Read")).isEqualTo(Scope.READ);
    }

    @Test
    void anUnrecognizedStoredRoleCrashesBareRatherThanAsAnAuthFailure() {
        FakeUserStore users = new FakeUserStore();
        CountingHasher hasher = new CountingHasher();
        users.save(new User("alice", hasher.hash("correct"), "superuser"));
        LoginService login = new LoginService(users, hasher, new FakeSigner());

        // The credentials are correct; the role is the fault. It must not be laundered into a coded
        // user-facing auth error — a seeded data defect surfaces bare so it is not hidden.
        Throwable thrown = catchThrowableOfType(RuntimeException.class,
                () -> login.login("alice", "correct"));
        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        assertThat(thrown).isNotInstanceOf(TapstateException.class);
    }

    /** Drives one successful login with the given stored role and reads back the minted scope. */
    private static Scope scopeFor(String role) {
        FakeUserStore users = new FakeUserStore();
        CountingHasher hasher = new CountingHasher();
        users.save(new User("alice", hasher.hash("correct"), role));
        FakeSigner signer = new FakeSigner();
        LoginService login = new LoginService(users, hasher, signer);

        String token = login.login("alice", "correct");
        return signer.verify(token).orElseThrow().scope();
    }

    /** An in-memory user store keyed by username. */
    private static final class FakeUserStore implements UserStore {
        private final Map<String, User> users = new HashMap<>();

        @Override
        public Optional<User> find(String username) {
            return Optional.ofNullable(users.get(username));
        }

        @Override
        public void save(User user) {
            users.put(user.username(), user);
        }

        @Override
        public boolean isEmpty() {
            return users.isEmpty();
        }
    }

    /** A deterministic password verifier that also counts verification calls, to witness timing parity. */
    private static final class CountingHasher implements PasswordHasher {
        int matchCalls;

        @Override
        public String hash(String raw) {
            return "hash:" + raw;
        }

        @Override
        public boolean matches(String raw, String storedHash) {
            matchCalls++;
            return storedHash.equals(hash(raw));
        }
    }

    /** A signer whose token is a reversible encoding of its claims, so a test can round-trip it. */
    private static final class FakeSigner implements TokenSigner {
        @Override
        public String issue(String subject, Scope scope) {
            return subject + "|" + scope.name();
        }

        @Override
        public Optional<VerifiedToken> verify(String token) {
            int bar = token.indexOf('|');
            if (bar < 0) {
                return Optional.empty();
            }
            return Optional.of(new VerifiedToken(token.substring(0, bar), Scope.valueOf(token.substring(bar + 1))));
        }
    }
}
