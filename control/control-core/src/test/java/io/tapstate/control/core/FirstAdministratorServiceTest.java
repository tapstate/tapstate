package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Password-port and authorization composition only; actual hash/login and CONFIG atomicity have separate fixtures. */
class FirstAdministratorServiceTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T00:00:00Z"), ZoneOffset.UTC);
    private static final class Administrators implements FirstAdminStore {
        final StorageControlAccessTest.States states;
        final Administrators root;
        final Runnable active;
        User saved;
        boolean audited;
        Administrators(StorageControlAccessTest.States states) { this.states=states; this.root=this; this.active=() -> {}; }
        Administrators(Administrators root,Runnable active) { this.states=root.states; this.root=root; this.active=active; }
        @Override public FirstAdminStore guardedBy(Runnable guard) { return new Administrators(root,guard); }
        @Override public Optional<Status> load() { throw new UnsupportedOperationException(); }
        @Override public Status create(StorageSetupScope expected,User user) {
            active.run(); assertThat(root.audited).isTrue();
            root.saved=user; states.firstAdminComplete=true;
            return new Status(expected,1,Phase.COMPLETE,user.username());
        }
    }
    private static final PasswordHasher HASHER = new PasswordHasher() {
        @Override public String hash(String raw) { return "fixture-hash:"+raw; }
        @Override public boolean matches(String raw,String hash) { return hash.equals(hash(raw)); }
    };
    @Test void firstAdminUsesItsOwnPermissionAuditAndTheAtomicInsertPortThenClosesPermanently() {
        var states=new StorageControlAccessTest.States(StorageSetupStateStore.Purpose.FIRST_ADMIN);
        var permission=StorageControlAccessTest.permission(states,StorageSetupStateStore.Purpose.FIRST_ADMIN);
        var store=new Administrators(states);
        var service=new FirstAdministratorService(store,HASHER,new AuditGate(record -> store.audited=true,CLOCK));
        assertThat(service.create(permission,"admin","password").phase()).isEqualTo(FirstAdminStore.Phase.COMPLETE);
        assertThat(store.saved.passwordHash()).isEqualTo("fixture-hash:password"); assertThat(store.saved.role()).isEqualTo("admin");
        assertThatThrownBy(() -> service.create(permission,"other","other-password")).isInstanceOf(TapstateException.class);
        assertThat(store.saved.username()).isEqualTo("admin");
    }
    @Test void setupPurposeAndAuditFailureCannotCreateAUser() {
        var setupStates=new StorageControlAccessTest.States(StorageSetupStateStore.Purpose.SETUP);
        var setup=StorageControlAccessTest.permission(setupStates,StorageSetupStateStore.Purpose.SETUP);
        var store=new Administrators(setupStates);
        var service=new FirstAdministratorService(store,HASHER,new AuditGate(record -> { throw new IllegalStateException("fixture audit unavailable"); },CLOCK));
        assertThatThrownBy(() -> service.create(setup,"admin","password")).isInstanceOf(TapstateException.class);
        assertThat(store.saved).isNull();
        var firstStates=new StorageControlAccessTest.States(StorageSetupStateStore.Purpose.FIRST_ADMIN);
        var first=StorageControlAccessTest.permission(firstStates,StorageSetupStateStore.Purpose.FIRST_ADMIN);
        assertThatThrownBy(() -> service.create(first,"admin","password")).isInstanceOf(TapstateException.class);
        assertThat(store.saved).isNull();
    }
}
