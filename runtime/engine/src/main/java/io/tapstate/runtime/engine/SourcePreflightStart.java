package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceKeyMovePreflight;
import io.tapstate.spi.sink.TargetKeyConstraints;
import io.tapstate.spi.transform.TransformPort;
import java.io.Serializable;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Binds downstream routing checks and prepared target evidence to this execution's source. */
final class SourcePreflightStart implements ProcessorMetaSupplier {
    private static final long serialVersionUID = 1L;
    private final ProcessorMetaSupplier delegate;
    private final List<FunctionEx<Object, Object>> routes;
    private final List<Target> targets;
    private final Prefix prefix;
    private transient SourceKeyMovePreflight.Check prepared;
    private transient Context context;

    record Target(String node, Map<String, SinkTarget> streams, PreparesTargets preparation) implements Serializable {}

    record SerialPort(SupplierEx<? extends TransformPort> factory, boolean canExpand) implements Serializable {}

    record Prefix(String terminal, List<SerialPort> ports) implements Serializable {
        Prefix {
            ports = List.copyOf(ports);
        }
    }

    private SourcePreflightStart(ProcessorMetaSupplier delegate, List<FunctionEx<Object, Object>> routes,
            List<Target> targets, Prefix prefix) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.routes = List.copyOf(routes);
        this.targets = List.copyOf(targets);
        this.prefix = prefix;
    }

    static ProcessorMetaSupplier of(ProcessorMetaSupplier delegate, List<FunctionEx<Object, Object>> routes,
            List<Target> targets, Prefix prefix) {
        return new SourcePreflightStart(delegate, routes, targets, prefix);
    }

    @Override public void init(Context context) throws Exception {
        this.context = context;
        var claims = new ArrayList<Map<String, SinkLaneClaims>>();
        for (var target : targets) {
            var evidence = PreparedSinkLaneRouters.evidence(context, target.node(), target.preparation());
            var byStream = new java.util.LinkedHashMap<String, SinkLaneClaims>();
            target.streams().forEach((stream, table) -> {
                if (table.keyed()) byStream.put(stream, new SinkLaneClaims(target.node(), table,
                        evidence.getOrDefault(stream, TargetKeyConstraints.unknown())));
            });
            claims.add(Map.copyOf(byStream));
        }
        prepared = new Prepared(routes, List.copyOf(claims));
        delegate.init(context);
    }

    @Override public boolean initIsCooperative() { return targets.isEmpty() && delegate.initIsCooperative(); }

    @Override public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        var suppliers = delegate.get(addresses);
        var check = Objects.requireNonNull(prepared, "source preflight is unprepared");
        var serial = prefix;
        return address -> new Checked(suppliers.apply(address), check, serial);
    }

    @Override public int preferredLocalParallelism() { return delegate.preferredLocalParallelism(); }
    @Override public Permission getRequiredPermission() { return delegate.getRequiredPermission(); }
    @Override public Map<String, String> getTags() { return delegate.getTags(); }
    @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }
    @Override public boolean isReusable() { return delegate.isReusable(); }

    @Override public void close(Throwable failure) throws Exception {
        try {
            delegate.close(failure);
        } finally {
            if (context != null) targets.forEach(target -> PreparedSinkLaneRouters.forget(context, target.node()));
        }
    }

    private record Prepared(List<FunctionEx<Object, Object>> routes,
            List<Map<String, SinkLaneClaims>> targets) implements SourceKeyMovePreflight.Check {
        @Override public void validate(Envelope row) {
            routes.forEach(route -> route.apply(row));
            for (var target : targets) {
                var claims = target.get(row.src());
                if (claims != null && claims.requiresReservation(row)) claims.describe(row, true);
            }
        }
    }

    private static final class Checked implements ProcessorSupplier {
        private static final long serialVersionUID = 1L;
        private final ProcessorSupplier delegate;
        private final SourceKeyMovePreflight.Check check;
        private final Prefix prefix;

        private Checked(ProcessorSupplier delegate, SourceKeyMovePreflight.Check check, Prefix prefix) {
            this.delegate = delegate;
            this.check = check;
            this.prefix = prefix;
        }

        @Override public void init(Context context) throws Exception { delegate.init(context); }
        @Override public boolean initIsCooperative() { return delegate.initIsCooperative(); }
        @Override public Collection<? extends Processor> get(int count) {
            var processors = delegate.get(count);
            for (var processor : processors) {
                if (processor instanceof SourceKeyMovePreflight source) {
                    source.withKeyMovePreflight(prefix == null ? check
                            : new SerialTransformPreflight(prefix.ports(), check));
                }
            }
            return processors;
        }
        @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }
        @Override public void close(Throwable failure) throws Exception { delegate.close(failure); }
        @Override public List<Permission> permissions() { return delegate.permissions(); }
    }
}
