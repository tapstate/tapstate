package io.tapstate.control.restapi;

/** A read-only, detail-free entry decision. It grants no storage or installation authority. */
public interface StorageEntryStatus {
    enum Entry { LOGIN, SETUP_REQUIRED, RESTART_REQUIRED, FIRST_ADMIN_REQUIRED, UNAVAILABLE }
    record Summary(String mode, Entry entry) {}
    Summary read();
}
