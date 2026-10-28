package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.runtime.srs.CaptureObservation;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.store.WorkloadClaimType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Member-local observer ownership for physical capture openings. Lifecycle calls only publish bounded
 * local state; a separate background thread closes packet queues and reaps actually drained bindings.
 * This is not a registration RPC, authority oracle, coverage barrier or Pipeline-execution receipt.
 * Control callers must establish those facts before registering a binding against its exact context.
 */
public final class ValidationCaptureObservations implements CaptureObservation.Factory, AutoCloseable {
    public record Limits(int openings, ValidationCaptureHintRegistry.Limits registry) {
        public Limits {
            Objects.requireNonNull(registry, "registry");
            if (openings < 1 || openings > 128 || openings * (registry.bytes() + 160L * 1024) > 8L * 1024 * 1024
                    || openings * registry.registrations() * registry.perBinding().keyBytes() > 64L * 1024 * 1024) {
                throw invalid("hint.observationLimits");
            }
        }
    }
    public record Status(CaptureObservation.Context context, ValidationCaptureHintRegistry.Snapshot registry) {}
    private static final System.Logger LOG = System.getLogger(ValidationCaptureObservations.class.getName());
    private final Limits limits;
    private final String clusterId;
    private final String nodeId;
    private final String bootId;
    private final ReentrantLock control = new ReentrantLock();
    private final Map<String, Opening> retained = new LinkedHashMap<>();
    private volatile List<Opening> current = List.of();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread maintenance;

    /** Missing member identity leaves observation unavailable without inventing an owner for capture. */
    public ValidationCaptureObservations(String clusterId, String nodeId, String bootId, Limits limits) {
        this(clusterId, nodeId, bootId, limits, true);
    }
    ValidationCaptureObservations(String clusterId, String nodeId, String bootId, Limits limits, boolean background) {
        this.clusterId = clusterId; this.nodeId = nodeId; this.bootId = bootId;
        this.limits = Objects.requireNonNull(limits, "limits");
        maintenance = background && available() ? new Thread(() -> {
            while (!closed.get() || !current.isEmpty()) {
                maintain();
                LockSupport.parkNanos(250_000_000L);
            }
        }, "validation-capture-cleanup") : null;
        if (maintenance != null) { maintenance.setDaemon(true); maintenance.start(); }
    }
    public boolean available() { return present(clusterId) && present(nodeId) && present(bootId); }

    @Override public CaptureObservation.Hooks open(CaptureObservation.Context context) {
        Objects.requireNonNull(context, "context");
        if (!available()) return CaptureObservation.NONE.open(context);
        if (closed.get() || !control.tryLock()) throw invalid("hint.observationUnavailable");
        try {
            if (closed.get()) throw invalid("hint.observationClosed");
            if (retained.containsKey(context.openingId())) throw invalid("hint.openingCollision");
            if (retained.size() == limits.openings()) throw invalid("hint.openingCapacity");
            for (String value : List.of(context.captureId(), context.openingId(), context.chainId(),
                    context.sourceId(), context.pipelineId(), context.consumerId())) {
                if (!present(value)) throw invalid("hint.observationIdentity");
                ValidationError.fieldName(value, "hint.observationIdentity");
            }
            if (context.tables().size() > 4096) throw invalid("hint.observationTables");
            var fence = context.fence();
            if (fence != null && (fence.key().type() != WorkloadClaimType.CAPTURE
                    || !fence.key().clusterId().equals(clusterId) || !fence.owner().nodeId().equals(nodeId)
                    || !fence.owner().bootId().equals(bootId))) throw invalid("hint.observationOwner");
            // Pipeline route/key revisions belong to individual admitted senders, not this physical tail.
            var scope = new ValidationCaptureHintRegistry.Scope(clusterId, context.captureId(), context.openingId(),
                    context.sourceId(), nodeId, bootId, context.epoch(), fence, Set.copyOf(context.tables()));
            var opening = new Opening(context, new ValidationCaptureHintRegistry(scope, limits.registry()));
            retained.put(context.openingId(), opening);
            current = List.copyOf(retained.values());
            return opening;
        } finally { control.unlock(); }
    }

    /** Metadata only. A listed physical opening does not establish the caller's registration authority. */
    public List<Status> status() {
        return current.stream().map(opening -> new Status(opening.context, opening.registry.snapshot())).toList();
    }
    public ValidationCaptureHintRegistry.Registration register(CaptureObservation.Context expected,
                                                               ValidationCaptureHintTap.Binding binding) {
        Objects.requireNonNull(expected, "expected");
        Opening selected = current.stream().filter(opening -> opening.context.equals(expected)).findFirst()
                .orElseThrow(() -> invalid("hint.originalOpeningUnavailable"));
        if (closed.get() || selected.retired.get()) throw invalid("hint.originalOpeningUnavailable");
        return selected.registry.register(selected.registry.scope(), binding);
    }
    /** Only the native control registrar uses alias routes, after current store authority checks. */
    ValidationCaptureHintRegistry.Registration register(ValidationCaptureRoute route,CaptureObservation.Context expected,
            ValidationCaptureHintTap.Binding binding) {
        return register(route,expected,binding,ignored->{});
    }
    ValidationCaptureHintRegistry.Registration register(ValidationCaptureRoute route,CaptureObservation.Context expected,
            ValidationCaptureHintTap.Binding binding,java.util.function.Consumer<ValidationCaptureHintRegistry.Registration> originalOwner) {
        route.requireOpening(expected);
        Opening selected=current.stream().filter(value->value.context.equals(expected)).findFirst().orElseThrow(()->invalid("hint.originalOpeningUnavailable"));
        if(closed.get()||selected.retired.get())throw invalid("hint.originalOpeningUnavailable");
        return selected.registry.registerAlias(selected.registry.scope(),route.alias(),binding,originalOwner);
    }
    @Override public void failed(CaptureObservation.Context context, String phase, RuntimeException failure) {
        // A callback from an older opening can only invalidate that exact retained object.
        for (Opening opening : current) if (opening.context.equals(context)) opening.retire(failure);
    }

    /** Background/control only. Never call from capture, registration locks or a source lifecycle hook. */
    void maintain() {
        for (Opening opening : current) {
            try {
                if (closed.get()) opening.retire(null);
                if (opening.retired.get() || opening.registry.snapshot().closed()) opening.registry.close();
                opening.registry.reap();
                if (opening.retired.get() && opening.registry.snapshot().registrations() == 0) {
                    control.lock();
                    try {
                        retained.remove(opening.context.openingId(), opening);
                        current = List.copyOf(retained.values());
                    } finally { control.unlock(); }
                }
            } catch (RuntimeException failure) {
                opening.retire(failure);
                var coded = new TapstateException(CaptureError.OBSERVER_FAILED,
                        Map.of("captureId", opening.context.captureId(), "openingId", opening.context.openingId(),
                                "phase", "cleanup"), failure);
                LOG.log(System.Logger.Level.WARNING, coded.toString(), coded);
            } catch (Error fatal) {
                for (Opening retainedOpening : current) retainedOpening.retire(fatal);
                throw fatal;
            }
        }
    }
    @Override public void close() {
        control.lock();
        try {
            closed.set(true);
            for (Opening opening : current) opening.retire(null);
        } finally { control.unlock(); }
        // Maintenance retains reservations and continues until actual callback/request exit. Shutdown
        // neither blocks application teardown nor treats cancellation as proof of physical drain.
        if (maintenance == null) maintain();
    }

    private final class Opening implements CaptureObservation.Hooks {
        private final CaptureObservation.Context context;
        private final ValidationCaptureHintRegistry registry;
        private final AtomicBoolean retired = new AtomicBoolean();
        private Opening(CaptureObservation.Context context, ValidationCaptureHintRegistry registry) {
            this.context = context; this.registry = registry;
        }
        @Override public CaptureListener wrap(CaptureListener original) { return registry.install(original); }
        @Override public void retire(Throwable reason) {
            retired.set(true);
            registry.invalidate(reason == null ? invalid("hint.captureOpeningClosed") : reason);
        }
    }
    private static boolean present(String text) { return text != null && !text.isBlank(); }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
