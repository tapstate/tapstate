package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Native physical tails and private snapshot attachments have independent lifetimes and authority. */
public final class NativeSrsCaptureRunUnit implements AutoCloseable {
    private final SrsStoreViews views;
    private final HazelcastInstance member;
    private final SourceAcknowledgements acknowledgements;
    private final Set<Tail> tails = ConcurrentHashMap.newKeySet();
    private final Set<CaptureRun> snapshots = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** The same canonical log instance must also back the member's durable ring configuration. */
    public NativeSrsCaptureRunUnit(CacheBusinessScope scope,SrsStores stores,HazelcastInstance member) {
        Objects.requireNonNull(scope,"scope"); Objects.requireNonNull(stores,"stores");
        this.member = Objects.requireNonNull(member,"member");
        this.views = (expected,guard) -> {
            guard.run();
            if (!scope.equals(expected)) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null);
            return new SrsStores(stores.chains().guardedBy(guard),stores.models().guardedBy(guard),
                    stores.log(),stores.consumers().guardedBy(guard));
        };
        this.acknowledgements = new SourceAcknowledgements();
    }

    /** Only a physical CAPTURE owner opens this handle; no Pipeline consumer owns or closes it. */
    public synchronized Tail startTail(CacheBusinessScope scope,SrsChainState opened,WorkloadClaimFence originalCapture,
            CaptureConfig config,CaptureStart start,Function<SrsCaptureWriter,CapturePort> ports,Runnable requireActive) {
        Runnable fixed = guard(requireActive); fixed.run();
        var stores = views.srs(scope,fixed);
        var writer = new SrsCaptureWriter(scope,opened,originalCapture,stores,member,fixed);
        var health = new CaptureHealth(); Subscription source = null; Tail owned = null;
        try {
            source = writer.start(Objects.requireNonNull(ports.apply(writer),"prepared capture port"),config,start,health);
            fixed.run();
            source = acknowledgements.follow(stores.chains().guardedBy(fixed),opened.miningChainId(),opened.epoch(),source,health,fixed);
            owned = new Tail(writer,source,health); tails.add(owned); fixed.run(); return owned;
        } catch (RuntimeException | Error failure) {
            writer.close(); if (owned != null) closeAfter(failure,owned::close);
            else if (source != null) closeAfter(failure,source::close); throw failure;
        }
    }

    /** A tail proposes only its subscribers' confirmed prefix; CACHE rechecks all references atomically. */
    public void retain(CacheBusinessScope scope,String chainId,WorkloadClaimFence originalCapture,Runnable requireActive) {
        Runnable fixed=guard(requireActive);fixed.run();var stores=views.srs(scope,fixed);
        var chain=stores.chains().load(chainId).orElseThrow(() -> new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN,Map.of("chain",chainId),null));
        var members=stores.consumers().attached(chainId);
        for(String table:chain.servingTables()) {
            long floor=Long.MAX_VALUE;boolean subscribed=false;
            for(var member:members.consumers())if(member.tables().contains(table)) {
                subscribed=true;var held=stores.consumers().progress(member.context());
                // A coherent detach may remove this cursor after the topology snapshot. Abandon this cut.
                // The adapter still rejects missing progress for an attached or corrupt relation.
                if(held.isEmpty())return;
                var progress=held.orElseThrow();if(progress.membershipRevision()!=member.revision())return;
                floor=Math.min(floor,progress.ringDoneThrough().getOrDefault(table,-1L));
            }
            String ring=SrsRingbuffer.ringName(chainId,table);
            if(subscribed && floor>=0 && floor!=Long.MAX_VALUE && floor>stores.log().bounds(ring).trimmedThrough())stores.log().trim(ring,floor,originalCapture);
        }
        stores.models().reclaim(chainId,originalCapture);fixed.run();
    }

    /** Resolves the existing durable read rule without publishing a cursor or treating read-ahead as confirmation. */
    public long initialReadAfter(CacheBusinessScope scope,String chain,String table,StartFrom start,String retention,Runnable requireActive) {
        Runnable fixed=guard(requireActive);fixed.run();var stores=views.srs(scope,fixed);String ring=SrsRingbuffer.ringName(chain,table);
        long next=SrsRingReader.from(new SrsRingbuffer(member.getRingbuffer(ring)),start,ignored -> {},retention,ring,stores.log()).nextReadSequence();
        fixed.run();return next-1;
    }

    /** Opens the first prepared read before returning, then drains only this consumer on a bounded worker. */
    public synchronized CaptureRun beginSnapshot(SrsConsumerRun run,CaptureConfig config,long firstSnapshotEpoch,
            Function<SrsSnapshotPreparation,CapturePort> ports,CaptureHandoff handoff,Runnable requireActive) {
        Runnable fixed = guard(requireActive); fixed.run();
        var rings = new TreeMap<String,String>();
        for (String table : config.streams()) rings.put(table,SrsRingbuffer.ringName(run.miningChainId(),table));
        var preparation = new SrsSnapshotPreparation(views,run,rings,firstSnapshotEpoch,fixed);
        SnapshotPhase.Load load = null; CaptureRun provisional = null;
        try {
            load = SnapshotPhase.openPrepared(Objects.requireNonNull(ports.apply(preparation),"prepared snapshot port"),config,preparation);
            fixed.run(); var health = new CaptureHealth();
            CaptureHandoff guarded = new CaptureHandoff() {
                @Override public void accept(io.tapstate.core.event.Envelope row) { fixed.run(); handoff.accept(row); }
                @Override public void loaded(String table) { fixed.run(); handoff.loaded(table); }
            };
            var background = new BackgroundLoad(load,guarded,Optional::empty,health,"tapstate-native-load-"+run.sourceId());
            var result = new CaptureRun(Optional.of(new MiningChainId(run.miningChainId())),false,Optional.empty(),health,background);
            provisional = result; snapshots.add(result); fixed.run(); background.start(); return result;
        } catch (RuntimeException | Error failure) {
            preparation.close();
            if (provisional != null) { var abandoned = provisional; snapshots.remove(abandoned); closeAfter(failure,abandoned::close); }
            else if (load != null) closeAfter(failure,load::close);
            throw failure;
        }
    }

    /** A private change stream records batches before handoff and acknowledges only its original settled prefix. */
    public synchronized CaptureRun startDirect(CaptureConfig config,DirectChannelProgress opened,DirectChannelStore channels,
            CaptureStart start,CapturePort port,CaptureHandoff handoff,int capacity,Runnable requireActive) {
        Objects.requireNonNull(opened,"opened");Objects.requireNonNull(channels,"channels");Objects.requireNonNull(handoff,"handoff");
        Runnable fixed=guard(requireActive);fixed.run();var window=Objects.requireNonNull(opened.current(),"opened capture");
        var writer=new DirectCaptureWriter(opened,window.captureFence(),window.pipelineFence(),channels,row -> {
            fixed.run();handoff.accept(row);fixed.run();
        },capacity,fixed);
        var health=new CaptureHealth();Subscription source=null;CaptureRun provisional=null;
        try {
            source=writer.start(port,config,start,health);fixed.run();
            source=acknowledgements.follow(channels,opened,source,health,fixed);
            provisional=new CaptureRun(Optional.empty(),false,0L,Optional.empty(),Optional.of(source),health);
            snapshots.add(provisional);fixed.run();return provisional;
        } catch(RuntimeException|Error failure) {
            writer.close();
            if(provisional!=null) { var abandoned=provisional;snapshots.remove(abandoned);closeAfter(failure,abandoned::close); }
            else if(source!=null)closeAfter(failure,source::close);
            throw failure;
        }
    }

    /** Opens the original private seam before returning; its CDC tail follows the bounded load on the same worker. */
    public synchronized CaptureRun beginDirectSnapshot(CaptureConfig config,DirectChannelProgress opened,DirectChannelStore channels,
            Function<DirectSnapshotPreparation,CapturePort> ports,CaptureHandoff handoff,int capacity,Runnable requireActive) {
        Runnable fixed=guard(requireActive);fixed.run();var preparation=new DirectSnapshotPreparation(opened,channels,fixed);
        SnapshotPhase.Load load=null;CaptureRun provisional=null;
        try {
            var port=Objects.requireNonNull(ports.apply(preparation),"prepared direct snapshot port");
            load=SnapshotPhase.openDirectPrepared(port,config,preparation);fixed.run();var health=new CaptureHealth();
            CaptureHandoff guarded=new CaptureHandoff() {
                @Override public void accept(io.tapstate.core.event.Envelope row){fixed.run();handoff.accept(row);}
                @Override public void loaded(String table){fixed.run();handoff.loaded(table);}
            };
            var background=new BackgroundLoad(load,guarded,() -> {
                fixed.run();var current=channels.load(opened.context());fixed.run();
                var window=current.current();var original=opened.current();
                if(window==null || window.epoch()!=opened.epoch() || !window.captureFence().equals(original.captureFence())
                        || !window.pipelineFence().equals(original.pipelineFence()))throw new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE,Map.of("channel",opened.context().channelId()),null);
                var saved=current.cursor().sinkAcked();if(saved==null)throw new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE,Map.of("channel",opened.context().channelId()),null);
                var writer=new DirectCaptureWriter(current,window.captureFence(),window.pipelineFence(),channels,row -> {
                    fixed.run();handoff.accept(row);fixed.run();
                },capacity,fixed);
                Subscription source=null;
                try {
                    source=writer.start(port,config,CaptureStart.resume(new io.tapstate.spi.capture.SourcePosition(saved.token())),health);fixed.run();
                    return Optional.of(acknowledgements.follow(channels,current,source,health,fixed));
                } catch(RuntimeException|Error failure) {
                    writer.close();if(source!=null)closeAfter(failure,source::close);throw failure;
                }
            },health,"tapstate-native-direct-load-"+opened.context().sourceId());
            var result=new CaptureRun(Optional.empty(),false,Optional.empty(),health,background);
            provisional=result;snapshots.add(result);fixed.run();background.start();return result;
        } catch(RuntimeException|Error failure) {
            preparation.close();
            if(provisional!=null){var abandoned=provisional;snapshots.remove(abandoned);closeAfter(failure,abandoned::close);}
            else if(load!=null)closeAfter(failure,load::close);
            throw failure;
        }
    }

    /** A bounded-only source opens no change chain or CDC seam and uses the already allocated row order. */
    public synchronized CaptureRun beginSnapshotOnly(CaptureConfig config,long snapshotEpoch,CapturePort port,
            CaptureHandoff handoff,Runnable requireActive) {
        Objects.requireNonNull(config,"config"); Objects.requireNonNull(port,"port"); Objects.requireNonNull(handoff,"handoff");
        Runnable fixed=guard(requireActive); fixed.run(); SnapshotPhase.Load load=null; CaptureRun provisional=null;
        try {
            load=SnapshotPhase.openChainless(port,config,snapshotEpoch); fixed.run(); var health=new CaptureHealth();
            CaptureHandoff guarded=new CaptureHandoff() {
                @Override public void accept(io.tapstate.core.event.Envelope row) { fixed.run(); handoff.accept(row); }
                @Override public void loaded(String table) { fixed.run(); handoff.loaded(table); }
            };
            var background=new BackgroundLoad(load,guarded,Optional::empty,health,"tapstate-native-bounded-"+config.connectorId());
            var result=new CaptureRun(Optional.empty(),false,Optional.empty(),health,background);
            provisional=result; snapshots.add(result); fixed.run(); background.start(); return result;
        } catch (RuntimeException|Error failure) {
            if (provisional!=null) { var abandoned=provisional; snapshots.remove(abandoned); closeAfter(failure,abandoned::close); }
            else if (load!=null) closeAfter(failure,load::close);
            throw failure;
        }
    }

    public final class Tail implements AutoCloseable {
        private final SrsCaptureWriter writer;
        private final Subscription subscription;
        private final CaptureHealth health;
        private final AtomicBoolean ended = new AtomicBoolean();
        private Tail(SrsCaptureWriter writer,Subscription subscription,CaptureHealth health) {
            this.writer = writer; this.subscription = subscription; this.health = health;
        }
        public CaptureHealth health() { return health; }
        @Override public void close() {
            if (!ended.compareAndSet(false,true)) return;
            writer.close(); tails.remove(this); subscription.close();
        }
    }
    public synchronized void stopSnapshot(CaptureRun original) {
        if (snapshots.remove(Objects.requireNonNull(original,"original"))) original.close();
    }
    private Runnable guard(Runnable original) {
        Objects.requireNonNull(original,"requireActive");
        return () -> {
            if (closed.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null);
            original.run();
            if (closed.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null);
        };
    }
    private static Throwable closeAfter(Throwable primary,Runnable close) {
        try { close.run(); }
        catch (RuntimeException | Error failure) {
            if (primary == null) return failure;
            if (failure != primary) primary.addSuppressed(failure);
        }
        return primary;
    }
    @Override public synchronized void close() {
        if (!closed.compareAndSet(false,true)) return;
        Throwable first = null;
        for (var run : List.copyOf(snapshots)) first = closeAfter(first,() -> stopSnapshot(run));
        for (var tail : List.copyOf(tails)) first = closeAfter(first,tail::close);
        first = closeAfter(first,acknowledgements::close);
        if (first instanceof RuntimeException runtime) throw runtime;
        if (first instanceof Error error) throw error;
    }
}
