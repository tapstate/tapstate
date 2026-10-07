package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Binds frozen source markers to their own table log and every persisted target writer. */
final class BenchmarkTableCaptureSet implements AutoCloseable {
    private record Source(BenchmarkWorkloadDefinitions.SourceChain logical, String physical,
            long epoch, BenchmarkTableAckGate.Binding binding, Document runIdentity) { }
    private final BenchmarkForkEnvironment fork;
    private final StoreDocuments documents;
    private final BenchmarkTableTerminalObserver observer;
    private final Map<String, Source> sources;
    private final List<Map<String, Object>> receipts = new ArrayList<>();

    static BenchmarkTableCaptureSet open(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkForkEnvironment fork) {
        StoreDocuments documents = StoreDocuments.at(fork.storeUri());
        try {
            Map<String, Source> sources = new LinkedHashMap<>();
            List<BenchmarkTableTerminalObserver.Marker> markers = new ArrayList<>();
            for (var chain : workload.sourceChains()) {
                var association = physical(fork.control().positionRead(chain.pipelineId()), chain);
                Document root = documents.chain(association.chainId());
                if (root == null || !(root.get("epoch") instanceof Long || root.get("epoch") instanceof Integer)
                        || ((Number) root.get("epoch")).longValue() < 1) {
                    throw new AssertionError("benchmark source has no actual capture epoch");
                }
                String consumer = SrsConsumerId.of(chain.pipelineId(), chain.sourceId()).value();
                var binding = BenchmarkTableAckGate.bind(association.chainId(), chain.pipelineId(),
                        chain.sourceId(), chain.table(), documents.consumerOffset(association.chainId(), consumer));
                Source source = new Source(chain, association.chainId(), ((Number) root.get("epoch")).longValue(), binding,
                        documents.benchmarkRunIdentity(chain.pipelineId()));
                sources.put(chain.id(), source);
                String ring = SrsRingbuffer.ringName(source.physical(), chain.table());
                for (var marker : BenchmarkMeasuredEndMarkers.forChain(workload, chain).entrySet()) {
                    markers.add(measured(workload, chain, marker.getKey(), marker.getValue(), ring));
                }
                markers.add(new BenchmarkTableTerminalObserver.Marker(chain.terminalLogicalId(), ring,
                        "i", chain.terminalRowId(), workload.id().equals("stateless") ? "qty" : "marker",
                        workload.id().equals("stateless") ? 7L : terminalValue(chain)));
            }
            String database = new ConnectionString(fork.storeUri()).getDatabase();
            if (database == null) { throw new AssertionError("benchmark table observer database is absent"); }
            var observer = BenchmarkTableTerminalObserver.open(fork.storeUri(), database, markers);
            return new BenchmarkTableCaptureSet(fork, documents, observer, Map.copyOf(sources));
        } catch (RuntimeException | Error failure) {
            try { documents.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private BenchmarkTableCaptureSet(BenchmarkForkEnvironment fork, StoreDocuments documents,
            BenchmarkTableTerminalObserver observer, Map<String, Source> sources) {
        this.fork = fork; this.documents = documents; this.observer = observer; this.sources = sources;
    }

    long awaitMeasured(BenchmarkWorkloadDefinitions.Workload workload, String phase) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
        for (var chain : workload.sourceChains()) {
            if (BenchmarkMeasuredEndMarkers.forChain(workload, chain).containsKey(phase)) {
                await(chain, chain.id() + "/" + phase, deadline);
            }
        }
        return System.nanoTime();
    }

    BenchmarkAckOracle.TableConfirmationProof awaitTerminal(BenchmarkWorkloadDefinitions.SourceChain chain,
            String sourceToken) throws InterruptedException {
        Result result = await(chain, chain.terminalLogicalId(), System.nanoTime() + Duration.ofMinutes(5).toNanos());
        return BenchmarkAckOracle.TableConfirmationProof.from(chain.terminalLogicalId(), sourceToken,
                result.point(), result.source().binding(), result.cursor());
    }

    private record Result(Source source, BenchmarkTableTerminalObserver.Point point, Document cursor) { }

    private Result await(BenchmarkWorkloadDefinitions.SourceChain chain, String marker, long deadline)
            throws InterruptedException {
        Source source = sources.get(chain.id());
        if (source == null || !source.logical().equals(chain)) { throw new AssertionError("unregistered source chain"); }
        long left = deadline - System.nanoTime();
        if (left <= 0) { throw new AssertionError("table confirmation deadline expired before its marker"); }
        var point = observer.await(marker, Duration.ofNanos(left));
        if (point.epoch() != source.epoch()) { throw new AssertionError("source capture generation changed within fork"); }
        while (true) {
            observer.check();
            var association = physical(fork.control().positionRead(chain.pipelineId()), chain);
            Document root = documents.chain(source.physical());
            if (!association.chainId().equals(source.physical()) || root == null
                    || !(root.get("epoch") instanceof Long || root.get("epoch") instanceof Integer)
                    || !(root.get("epoch") instanceof Number epoch) || epoch.longValue() != source.epoch()) {
                throw new AssertionError("source association or capture generation changed within fork");
            }
            if (!source.runIdentity().equals(documents.benchmarkRunIdentity(chain.pipelineId()))) {
                throw new AssertionError("benchmark pipeline artifact or execution identity changed within fork");
            }
            Document cursor = documents.consumerOffset(source.physical(), source.binding().consumer());
            if (BenchmarkTableAckGate.covers(source.binding(), point, cursor)) {
                var after = physical(fork.control().positionRead(chain.pipelineId()), chain);
                Document afterRoot = documents.chain(source.physical());
                if (!after.chainId().equals(source.physical()) || afterRoot == null
                        || !(afterRoot.get("epoch") instanceof Integer || afterRoot.get("epoch") instanceof Long)
                        || ((Number) afterRoot.get("epoch")).longValue() != source.epoch()
                        || !source.runIdentity().equals(documents.benchmarkRunIdentity(chain.pipelineId()))) {
                    throw new AssertionError("source or execution identity changed around target confirmation");
                }
                if (!BenchmarkTableAckGate.covers(source.binding(), point,
                        documents.consumerOffset(source.physical(), source.binding().consumer()))) {
                    throw new AssertionError("target confirmation changed during its identity check");
                }
                observer.check();
                receipts.add(Map.of("proofKind", "TABLE_ORDER", "markerId", marker, "ring", point.ring(),
                        "epoch", point.epoch(), "seq", point.seq(), "actualConsumerId", source.binding().consumer(),
                        "expectedWriters", source.binding().writers(), "readConsistency", "SEQUENTIAL_POINT_READS"));
                return new Result(source, point, cursor);
            }
            if (System.nanoTime() >= deadline) { throw new AssertionError("not every target writer confirmed the exact table marker"); }
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }

    List<Map<String, Object>> receipts() {
        List<Map<String, Object>> result = new ArrayList<>(receipts);
        result.add(observer.evidence());
        return List.copyOf(result);
    }

    private static ControlPlane.PositionChain physical(ControlPlane.PositionRead read,
            BenchmarkWorkloadDefinitions.SourceChain chain) {
        List<ControlPlane.PositionChain> matches = read.chains().stream()
                .filter(value -> value.sourceId().equals(chain.sourceId()) && value.tables().equals(List.of(chain.table()))).toList();
        if (!read.pipelineId().equals(chain.pipelineId()) || matches.size() != 1) {
            throw new AssertionError("benchmark source association is absent or ambiguous");
        }
        return matches.getFirst();
    }

    static BenchmarkTableTerminalObserver.Marker measured(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.SourceChain chain, String phase, long row, String ring) {
        String field;
        Object value;
        if (workload.id().equals("copy")) { field = "amount"; value = (row * 17 + workload.seed()) % 1_000 + 1; }
        else if (workload.id().equals("stateless")) { field = "qty"; value = (row * 13 + workload.seed()) % 500 + 1; }
        else if (chain.table().equals("bench_join_orders")) { field = "qty"; value = (row * 7 + workload.seed()) % 1_000 + 1; }
        else if (chain.table().equals("bench_nest_items")) {
            field = "sku"; value = phase.equals("cold-read") ? "cold-" + (row - 300_000) : "sku-" + row + "u";
        } else { throw new AssertionError("measured marker has no frozen value recipe"); }
        return new BenchmarkTableTerminalObserver.Marker(chain.id() + "/" + phase, ring,
                BenchmarkMeasuredEndMarkers.operation(phase).symbol(), row, field, value);
    }

    private static String terminalValue(BenchmarkWorkloadDefinitions.SourceChain chain) {
        return switch (chain.table()) {
            case "bench_copy_orders" -> "copy-orders-terminal";
            case "bench_join_orders" -> "join-orders-terminal";
            case "bench_join_customers" -> "join-customers-terminal";
            case "bench_nest_orders" -> "nest-orders-terminal";
            case "bench_nest_items" -> "nest-items-terminal";
            default -> throw new AssertionError("terminal marker has no frozen value recipe");
        };
    }

    @Override public void close() {
        Throwable primary = null;
        try { observer.close(); } catch (RuntimeException | Error failure) { primary = failure; }
        try { documents.close(); } catch (RuntimeException | Error cleanup) {
            if (primary == null) { primary = cleanup; } else { primary.addSuppressed(cleanup); }
        }
        if (primary instanceof Error failure) { throw failure; }
        if (primary instanceof RuntimeException failure) { throw failure; }
    }
}
