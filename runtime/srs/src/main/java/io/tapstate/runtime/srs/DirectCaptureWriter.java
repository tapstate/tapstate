package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/** One private producer records original batch boundaries before delivery and bounds unconfirmed rows. */
public final class DirectCaptureWriter implements CaptureStartedListener, AutoCloseable {
    private record Pending(String table,long sequence) {}
    private final java.util.Set<Subscription> sourceHandles=java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final DirectChannelProgress opened;
    private final DirectChannelContext context;
    private final DirectChannelStore channels;
    private final WorkloadClaimFence capture,pipeline;
    private final Consumer<Envelope> downstream;
    private final Runnable active;
    private final int capacity;
    private final AtomicBoolean started=new AtomicBoolean(),closed=new AtomicBoolean();
    private final AtomicReference<Throwable> failure=new AtomicReference<>();
    private final Deque<Pending> pending=new ArrayDeque<>();
    private final Map<String,Long> targets=new HashMap<>();
    private volatile String expectedResume;
    // Publishes CaptureHealth before callbacks; that object uses atomic fields and concurrent maps.
    @SuppressWarnings("java:S3077")
    private volatile CaptureHealth health;
    private String anchor;
    private long nextSequence,recordedThrough;

    public DirectCaptureWriter(DirectChannelProgress opened, WorkloadClaimFence originalCapture,
            WorkloadClaimFence originalPipeline, DirectChannelStore channels, Consumer<Envelope> downstream,
            int capacity, Runnable requireActive) {
        this.opened=Objects.requireNonNull(opened,"opened"); this.context=opened.context();
        this.capture=Objects.requireNonNull(originalCapture,"originalCapture"); this.pipeline=Objects.requireNonNull(originalPipeline,"originalPipeline");
        this.active=Objects.requireNonNull(requireActive,"requireActive"); this.channels=Objects.requireNonNull(channels,"channels").guardedBy(this::requireUsable);
        this.downstream=Objects.requireNonNull(downstream,"downstream"); this.capacity=capacity;
        if (capacity<1) throw new IllegalArgumentException("a private producer requires bounded headroom");
        requireCurrent(opened);
        if (opened.current().recordedThrough()!=0) throw unavailable();
        anchor=opened.current().anchor(); active.run();
    }

    public Subscription start(CapturePort port, CaptureConfig config, CaptureStart start, CaptureHealth health) {
        Objects.requireNonNull(port,"port"); Objects.requireNonNull(start,"start"); Objects.requireNonNull(health,"health"); requireUsable();
        if (config.node()==null || !context.scope().pipelineId().equals(config.node().pipelineId())
                || !context.sourceId().equals(config.node().nodeId()) || !opened.tables().equals(new HashSet<>(config.streams()))
                || config.streams().size()!=opened.tables().size() || config.sharedNotes()!=null) throw unavailable();
        String known=opened.resumeToken();
        if (known!=null && (!(start instanceof CaptureStart.Resume resume) || !known.equals(resume.position().token()))
                || known==null && start instanceof CaptureStart.Resume) throw unavailable();
        expectedResume=known;
        if (!started.compareAndSet(false,true)) throw unavailable();
        this.health=health;
        Subscription source=null;
        try {
            source=Objects.requireNonNull(port.cdc(config,start,health.recording(this)),"source subscription"); sourceHandles.add(source); requireUsable();
            var original=source; var ended=new AtomicBoolean();
            return new Subscription() {
                @Override public void acknowledge(SourcePosition position) { requireUsable(); original.acknowledge(position); }
                @Override public synchronized void close() { if(!ended.get()) { DirectCaptureWriter.this.close(); stopSource(original); ended.set(true); } }
            };
        } catch (RuntimeException|Error failed) {
            fail(failed); if (source!=null) try { stopSource(source); } catch(RuntimeException|Error cleanup) { if(cleanup!=failed)failed.addSuppressed(cleanup); }
            throw failed;
        }
    }

    /** The original owner retains incomplete source shutdown independently of business activation. */
    public void stopSources() {
        Throwable first=null;
        for(var source:List.copyOf(sourceHandles))try {stopSource(source);}
        catch(RuntimeException|Error failure){if(first==null)first=failure;else if(first!=failure)first.addSuppressed(failure);}
        if(first instanceof RuntimeException runtime)throw runtime;if(first instanceof Error error)throw error;
    }
    private void stopSource(Subscription source) {
        if(!sourceHandles.contains(source))return;source.close();sourceHandles.remove(source);
    }

    @Override public synchronized void onStart(SourcePosition position) {
        try {
            requireStarted(); Objects.requireNonNull(position,"position");
            if (expectedResume!=null && !expectedResume.equals(position.token()) || anchor!=null && !anchor.equals(position.token())) throw unavailable();
            if (!channels.confirmAnchor(context,opened.epoch(),position.token(),capture,pipeline)) throw unavailable();
            requireUsable(); anchor=position.token();
        } catch(RuntimeException|Error failed) { fail(failed); throw failed; }
    }
    @Override public synchronized void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
        try {
            requireStarted(); var captured=List.copyOf(events); Objects.requireNonNull(position,"position");
            if (anchor==null || nextSequence>=Long.MAX_VALUE-captured.size()-1 || recordedThrough>=Long.MAX_VALUE-1
                    || captured.stream().anyMatch(event -> !opened.tables().contains(event.src()))) throw unavailable();
            var ordered=new ArrayList<Envelope>(captured.size());
            for (int i=0;i<captured.size();i++) {
                var event=captured.get(i); long sequence=nextSequence++;
                targets.put(event.src(),sequence);
                String token=i==captured.size()-1?position.map(SourcePosition::token).orElse(null):null;
                ordered.add(event.withPosition(new ChainPosition(new SourceOrder(opened.epoch(),sequence),token)));
            }
            if (position.isPresent()) {
                awaitCapacity(true);
                var boundary=new ChainPosition(new SourceOrder(opened.epoch(),nextSequence++),position.orElseThrow().token());
                var batch=new DirectBatch(context,opened.epoch(),recordedThrough+1,boundary,Map.copyOf(targets));
                if (!channels.recordBatch(batch,recordedThrough,capture,pipeline)) throw unavailable();
                requireUsable(); recordedThrough++; targets.clear();
            }
            for (var event:ordered) {
                awaitCapacity(false); requireUsable();
                pending.addLast(new Pending(event.src(),event.position().order().seq()));
                downstream.accept(event); requireUsable();
            }
        } catch(RuntimeException|Error failed) { fail(failed); throw failed; }
    }
    private void awaitCapacity(boolean batches) {
        while (true) {
            requireUsable();
            if (!batches && pending.size()<capacity) return;
            var progress=channels.load(context); requireUsable(); requireCurrent(progress);
            while (!pending.isEmpty()) {
                var first=pending.getFirst(); var acknowledged=progress.cursor().sinkAckedByTable().get(first.table());
                if (acknowledged==null || acknowledged.order().epoch()!=opened.epoch() || acknowledged.order().seq()<first.sequence()) break;
                pending.removeFirst();
            }
            if (batches?progress.current().pendingCount()<capacity:pending.size()<capacity) return;
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("the private source was interrupted while waiting for its original writers");
            LockSupport.parkNanos(10_000_000L);
        }
    }
    private void requireCurrent(DirectChannelProgress progress) {
        var window=progress.current();
        if (!context.equals(progress.context()) || window==null || window.epoch()!=opened.epoch()
                || !window.captureFence().equals(capture) || !window.pipelineFence().equals(pipeline)
                || !opened.tables().equals(progress.tables())) throw unavailable();
    }
    private void requireStarted() { requireUsable(); if (!started.get()) throw unavailable(); }
    private void requireUsable() {
        active.run();
        var observed=health; if (observed!=null) observed.failure().ifPresent(this::fail);
        var failed=failure.get(); if (failed instanceof RuntimeException runtime) throw runtime; if (failed instanceof Error error) throw error;
        if (closed.get()) throw unavailable();
    }
    @Override public void onError(Throwable error) { fail(Objects.requireNonNull(error,"error")); }
    private void fail(Throwable error) { failure.compareAndSet(null,error); closed.set(true); }
    @Override public void close() { closed.set(true); }
    private TapstateException unavailable() { return new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE,Map.of("channel",context.channelId()),null); }
}
