package io.tapstate.spi.store;

import java.util.Set;

/** A bounded, consistent CONFIG snapshot; no runtime records or managed source credentials are exported. */
public interface ConfigurationExportStore {
    record Selection(Set<String> resourceIds, Set<String> usernames) {
        public Selection {
            resourceIds = Set.copyOf(resourceIds); usernames = Set.copyOf(usernames);
            if (resourceIds.size() + usernames.size() < 1 || resourceIds.size() + usernames.size() > ConfigurationImportStore.MAX_OBJECTS
                    || resourceIds.stream().anyMatch(String::isBlank) || usernames.stream().anyMatch(String::isBlank))
                throw new IllegalArgumentException("invalid export selection");
        }
    }
    /** defaultSourceId is only a required mapping hint, never authority to replace a destination source. */
    record Snapshot(StorageBinding.Mode mode, String defaultSourceId, ConfigurationImportStore.Payload payload) {
        @Override public String toString() { return "ConfigurationExportSnapshot[redacted]"; }
    }
    ConfigurationExportStore guardedBy(Runnable requireActive);
    Snapshot snapshot(ConfigurationImportStore.Scope scope, Selection selection);
}
