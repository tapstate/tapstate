package io.tapstate.spi.store;

import io.tapstate.core.model.Resource;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/** Configuration-only import admission and atomic publication; caller authorization and audit run outside it. */
public interface ConfigurationImportStore {
    int MAX_OBJECTS = 64;
    int MAX_PAYLOAD_BYTES = 4 * 1024 * 1024;
    int MAX_OBJECT_BYTES = 1024 * 1024;
    int MAX_RECEIPT_BYTES = 16 * 1024;
    int MAX_PENDING = 16;
    int MAX_RESULTS = 256;
    int MAX_ATTEMPTS = 3;
    Duration PREFLIGHT_LIFETIME = Duration.ofMinutes(30);

    record Scope(String clusterId,long bindingVersion,StorageBinding.Mode mode,String organizationId) {
        public Scope {
            Objects.requireNonNull(clusterId,"clusterId"); Objects.requireNonNull(mode,"mode");
            if (clusterId.isBlank() || bindingVersion < 1 || bindingVersion == Long.MAX_VALUE
                    || (mode == StorageBinding.Mode.CLOUD ? organizationId == null || organizationId.isBlank() : organizationId != null))
                throw new IllegalArgumentException("invalid configuration import scope");
        }
    }
    /** Canonical, already validated definitions; transport keys and source-managed authority are excluded. */
    record Payload(List<Resource> resources,List<User> users,List<PipelineLayout> layouts,
            Map<String,String> referenceHashes,Map<String,String> defaultSourceMapping) {
        public Payload {
            resources = List.copyOf(resources); users = List.copyOf(users); layouts = List.copyOf(layouts);
            referenceHashes = Map.copyOf(referenceHashes); defaultSourceMapping = Map.copyOf(defaultSourceMapping);
        }
    }
    record Ticket(String id,Scope scope,String inputHash,String mappingHash,long destinationRevision,
            Instant createdAt,Instant expiresAt) {
        public Ticket {
            Objects.requireNonNull(scope,"scope"); Objects.requireNonNull(createdAt,"createdAt"); Objects.requireNonNull(expiresAt,"expiresAt");
            if (!UUID.fromString(id).toString().equals(id) || !hash(inputHash) || !hash(mappingHash)
                    || destinationRevision < 0 || destinationRevision >= Long.MAX_VALUE - 1
                    || !createdAt.plus(PREFLIGHT_LIFETIME).equals(expiresAt)) throw new IllegalArgumentException("invalid import ticket");
        }
    }
    /** A small immutable result, without credentials, definitions, account hashes or runtime state. */
    record Receipt(String id,String inputHash,long configurationRevision,Map<String,String> resourceHashes,
            Set<String> usernames,Set<String> layoutIds) {
        public Receipt {
            if (!UUID.fromString(id).toString().equals(id) || !hash(inputHash)
                    || configurationRevision < 1 || configurationRevision == Long.MAX_VALUE) throw new IllegalArgumentException("invalid import receipt");
            resourceHashes = Collections.unmodifiableMap(new TreeMap<>(resourceHashes));
            usernames = Collections.unmodifiableSet(new TreeSet<>(usernames)); layoutIds = Collections.unmodifiableSet(new TreeSet<>(layoutIds));
            if (resourceHashes.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || !hash(entry.getValue()))
                    || usernames.stream().anyMatch(String::isBlank) || layoutIds.stream().anyMatch(String::isBlank))
                throw new IllegalArgumentException("invalid import receipt identities");
        }
    }
    enum Phase { PENDING, EXPIRED, COMMITTING, COMMITTED, ABORTED }
    record Status(Ticket ticket,Phase phase,int attempts,Receipt receipt) {
        public Status {
            Objects.requireNonNull(ticket,"ticket"); Objects.requireNonNull(phase,"phase");
            if (attempts < 0 || attempts > MAX_ATTEMPTS || ((phase == Phase.PENDING || phase == Phase.EXPIRED) && attempts != 0)
                    || ((phase == Phase.COMMITTING || phase == Phase.COMMITTED || phase == Phase.ABORTED) && attempts < 1)
                    || ((phase == Phase.COMMITTED) != (receipt != null))
                    || (receipt != null && (!ticket.id().equals(receipt.id()) || !ticket.inputHash().equals(receipt.inputHash())
                            || receipt.configurationRevision() != ticket.destinationRevision()+1))) throw new IllegalArgumentException("invalid import status");
        }
    }
    ConfigurationImportStore guardedBy(Runnable requireActive);

    /** Register a server-generated id and hashes only; registration grants no runtime work. */
    Status preflight(Scope scope,Payload payload);

    /** Missing ids stay unknown; a query never registers or replays an import. */
    Optional<Status> find(Scope scope,String id);

    /** Commit creates definitions, users, layouts, one revision and its original receipt in one CONFIG transaction. */
    Receipt commit(Ticket original,Payload payload);

    private static boolean hash(String text) { return text != null && text.matches("[0-9a-f]{64}"); }
}
