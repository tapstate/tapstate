package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The zero-user localhost bootstrap exception: a brand-new server (its user table empty) accepts, from
 * a loopback caller only, the one-time creation of the first admin user; once any user exists the
 * channel closes. Two guards are load-bearing and are asserted here, not just their happy path:
 *
 * <ul>
 *   <li><b>Origin before emptiness.</b> A non-loopback caller is refused before the store is even
 *       consulted, so a remote caller learns nothing about whether the server is still un-bootstrapped
 *       — the two refusals carry distinct codes, and the remote one is returned regardless of table
 *       state.
 *   <li><b>No audit, no execute.</b> Creating the first admin is an audited write; if its audit record
 *       cannot be written the admin is not created (the standard audit-gate guarantee, reused rather
 *       than re-implemented).
 * </ul>
 *
 * <p>All collaborators are in-memory fakes: the user store records saves and reports emptiness, the
 * hasher is a deterministic stand-in for the adaptive hash, and the audit store either records or
 * fails on demand.
 */
class BootstrapServiceTest {

    private static final Instant FIXED = Instant.parse("2026-07-08T09:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED, ZoneOffset.UTC);

    @Test
    void aRemoteCallerIsRefusedAndNoUserIsCreated() {
        FakeUserStore users = new FakeUserStore();
        RecordingAuditStore audit = new RecordingAuditStore();
        BootstrapService bootstrap = new BootstrapService(users, new FakeHasher(), new AuditGate(audit, FIXED_CLOCK));

        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> bootstrap.createFirstAdmin(CallerOrigin.REMOTE, "alice", "s3cret"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("TS0210017");
        // The refusal must echo nothing back: a payload here could encode table state, which is exactly
        // what checking origin before emptiness exists to withhold from a remote caller.
        assertThat(thrown.args()).as("the remote refusal carries no state-revealing payload").isEmpty();
        assertThat(users.saved).as("a refused bootstrap creates no user").isEmpty();
        assertThat(audit.records).as("a refused bootstrap leaves no audit record").isEmpty();
    }

    @Test
    void theOriginIsCheckedBeforeEmptinessSoARemoteCallerLearnsNothingAboutTheTable() {
        // The store already has a user: were emptiness checked first, this would surface as "closed".
        // The remote caller must instead be refused as "forbidden" — the same answer it gets on an empty
        // store — so it cannot tell an un-bootstrapped server from a bootstrapped one.
        FakeUserStore users = new FakeUserStore();
        users.save(new User("existing", "hash:pw", "admin"));
        BootstrapService bootstrap = new BootstrapService(
                users, new FakeHasher(), new AuditGate(new RecordingAuditStore(), FIXED_CLOCK));

        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> bootstrap.createFirstAdmin(CallerOrigin.REMOTE, "alice", "s3cret"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code())
                .as("origin is rejected before the table is consulted, so no table state leaks")
                .isEqualTo("TS0210017");
        assertThat(thrown.args())
                .as("the refusal echoes nothing back, so it cannot carry table state either")
                .isEmpty();
    }

    @Test
    void aNonEmptyStoreClosesTheChannelEvenFromLoopback() {
        FakeUserStore users = new FakeUserStore();
        users.save(new User("existing", "hash:pw", "admin"));
        BootstrapService bootstrap = new BootstrapService(
                users, new FakeHasher(), new AuditGate(new RecordingAuditStore(), FIXED_CLOCK));

        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("TS0210018");
        assertThat(thrown.args()).as("the closed-channel refusal carries no payload").isEmpty();
        assertThat(users.saved).as("the channel is closed; no second user is written").hasSize(1);
    }

    @Test
    void aLoopbackCallerOnAnEmptyStoreCreatesTheFirstAdmin() {
        FakeUserStore users = new FakeUserStore();
        FakeHasher hasher = new FakeHasher();
        BootstrapService bootstrap = new BootstrapService(
                users, hasher, new AuditGate(new RecordingAuditStore(), FIXED_CLOCK));

        bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret");

        Optional<User> created = users.find("alice");
        assertThat(created).isPresent();
        assertThat(created.get().role()).as("the first user is created with admin capability").isEqualTo("admin");
    }

    @Test
    void theFirstAdminPasswordIsStoredHashedNeverRaw() {
        FakeUserStore users = new FakeUserStore();
        FakeHasher hasher = new FakeHasher();
        BootstrapService bootstrap = new BootstrapService(
                users, hasher, new AuditGate(new RecordingAuditStore(), FIXED_CLOCK));

        bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret");

        String stored = users.find("alice").orElseThrow().passwordHash();
        assertThat(stored).isEqualTo(hasher.hash("s3cret"));
        assertThat(stored).as("the raw password is never persisted").isNotEqualTo("s3cret");
    }

    @Test
    void theFirstAdminCreationIsAudited() {
        FakeUserStore users = new FakeUserStore();
        RecordingAuditStore audit = new RecordingAuditStore();
        BootstrapService bootstrap = new BootstrapService(users, new FakeHasher(), new AuditGate(audit, FIXED_CLOCK));

        bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret");

        assertThat(audit.records).hasSize(1);
        AuditRecord record = audit.records.get(0);
        assertThat(record.operationId()).isEqualTo("user.create");
        assertThat(record.principal()).isEqualTo("alice");
        assertThat(record.resourceId()).isEqualTo("alice");
        assertThat(record.timestamp()).isEqualTo(FIXED);
    }

    @Test
    void theChannelClosesAfterTheFirstAdminIsCreated() {
        FakeUserStore users = new FakeUserStore();
        BootstrapService bootstrap = new BootstrapService(
                users, new FakeHasher(), new AuditGate(new RecordingAuditStore(), FIXED_CLOCK));

        bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret");

        // Same loopback caller, immediately after: the exception has closed because a user now exists.
        TapstateException second = catchThrowableOfType(TapstateException.class,
                () -> bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "bob", "another"));
        assertThat(second).isNotNull();
        assertThat(second.code().code()).isEqualTo("TS0210018");
        assertThat(users.find("bob")).as("no second admin is created once the channel has closed").isEmpty();
    }

    @Test
    void aFailedAuditRefusesTheCreationSoNoAdminIsWritten() {
        FakeUserStore users = new FakeUserStore();
        BootstrapService bootstrap = new BootstrapService(
                users, new FakeHasher(), new AuditGate(new FailingAuditStore(), FIXED_CLOCK));

        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret"));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code())
                .as("no audit, no execute: a failed audit write refuses the creation")
                .isEqualTo("TS0210015");
        assertThat(users.saved).as("the admin is not created when its audit record could not be written").isEmpty();
    }

    @Test
    void concurrentBootstrapsAreSerializedSoOnlyTheFirstAdminIsCreated() throws InterruptedException {
        // The channel is check-then-act (isEmpty() then save()); under concurrency two loopback callers
        // could both pass the emptiness check and each create a different first admin. The service must
        // serialize the whole check-and-create so only one wins. This is witnessed deterministically: the
        // first caller parks inside save() while holding the single-flight guard, so the second caller
        // cannot even reach its own emptiness check until the first has committed and the table is no
        // longer empty — it must then be turned away as bootstrap-closed.
        CountDownLatch firstReachedSave = new CountDownLatch(1);
        CountDownLatch releaseSave = new CountDownLatch(1);
        BlockingUserStore users = new BlockingUserStore(firstReachedSave, releaseSave);
        BootstrapService bootstrap = new BootstrapService(
                users, new FakeHasher(), new AuditGate(new RecordingAuditStore(), FIXED_CLOCK));

        AtomicReference<TapstateException> secondOutcome = new AtomicReference<>();
        Thread first = new Thread(() -> bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "alice", "s3cret"));
        Thread second = new Thread(() -> {
            try {
                bootstrap.createFirstAdmin(CallerOrigin.LOOPBACK, "bob", "another");
            } catch (TapstateException refused) {
                secondOutcome.set(refused);
            }
        });

        first.start();
        firstReachedSave.await(); // the first caller now holds the guard, parked inside save()
        second.start();
        // Give the second caller time to run. Serialized, it is parked acquiring the guard and has not yet
        // read the table; unserialized, it would read the still-empty table here and create a second admin.
        Thread.sleep(150);
        releaseSave.countDown(); // let the first caller commit and release the guard
        first.join();
        second.join();

        assertThat(users.saved.keySet()).as("only the first admin is created").containsExactly("alice");
        assertThat(secondOutcome.get()).as("the second caller finds the channel already closed").isNotNull();
        assertThat(secondOutcome.get().code().code()).isEqualTo("TS0210018");
    }

    /** An in-memory user store keyed by username that also reports emptiness for the bootstrap guard. */
    private static final class FakeUserStore implements UserStore {
        private final Map<String, User> saved = new HashMap<>();

        @Override
        public Optional<User> find(String username) {
            return Optional.ofNullable(saved.get(username));
        }

        @Override
        public void save(User user) {
            saved.put(user.username(), user);
        }

        @Override
        public boolean isEmpty() {
            return saved.isEmpty();
        }
    }

    /** A deterministic stand-in for the adaptive password hash. */
    private static final class FakeHasher implements PasswordHasher {
        @Override
        public String hash(String raw) {
            return "hash:" + raw;
        }

        @Override
        public boolean matches(String raw, String storedHash) {
            return storedHash.equals(hash(raw));
        }
    }

    /** An audit store that captures every record written through it. */
    private static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

    /** An audit store that always fails, standing in for an unavailable audit backend. */
    private static final class FailingAuditStore implements AuditStore {
        @Override
        public void record(AuditRecord record) {
            throw new IllegalStateException("audit backend down");
        }
    }

    /**
     * A thread-safe user store whose {@code save} parks — it signals it was reached, then waits for the
     * test to release it — so a test can pin one caller inside the commit and observe how a concurrent
     * caller behaves. {@code isEmpty} reflects only committed saves.
     */
    private static final class BlockingUserStore implements UserStore {
        final Map<String, User> saved = new ConcurrentHashMap<>();
        private final CountDownLatch reachedSave;
        private final CountDownLatch release;

        BlockingUserStore(CountDownLatch reachedSave, CountDownLatch release) {
            this.reachedSave = reachedSave;
            this.release = release;
        }

        @Override
        public Optional<User> find(String username) {
            return Optional.ofNullable(saved.get(username));
        }

        @Override
        public void save(User user) {
            reachedSave.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while parked in save", e);
            }
            saved.put(user.username(), user);
        }

        @Override
        public boolean isEmpty() {
            return saved.isEmpty();
        }
    }
}
