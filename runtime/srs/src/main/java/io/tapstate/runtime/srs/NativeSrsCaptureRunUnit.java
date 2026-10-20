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
    private final Map<Runnable,Runnable> pendingCloses = new ConcurrentHashMap<>();
    private final Map<CaptureRun,Runnable> runOwners = new ConcurrentHashMap<>();
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
        var health = new CaptureHealth(); var owned = new Tail(writer,null,health,requireActive); tails.add(owned);
        try {
            var source = writer.start(Objects.requireNonNull(ports.apply(writer),"prepared capture port"),config,start,health);
            owned.subscription = source; fixed.run();
            owned.subscription = acknowledgements.follow(stores.chains().guardedBy(fixed),opened.miningChainId(),opened.epoch(),source,health,fixed);
            fixed.run(); return owned;
        } catch (RuntimeException | Error failure) {
            writer.close(); closeAfter(failure,owned::close); throw failure;
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
            load = SnapshotPhase.openPrepared(Objects.requireNonNull(ports.apply(preparation),"prepared snapshot port"),config,preparation,session -> retainSession(session,requireActive));
            fixed.run(); var health = new CaptureHealth();
            CaptureHandoff guarded = new CaptureHandoff() {
                @Override public void accept(io.tapstate.core.event.Envelope row) { fixed.run(); handoff.accept(row); }
                @Override public void loaded(String table) { fixed.run(); handoff.loaded(table); }
            };
            var background = new BackgroundLoad(load,guarded,Optional::empty,health,"tapstate-native-load-"+run.sourceId());
            var result = new CaptureRun(Optional.of(new MiningChainId(run.miningChainId())),false,Optional.empty(),health,background);
            provisional = result; register(result,requireActive); fixed.run(); background.start(); return result;
        } catch (RuntimeException | Error failure) {
            preparation.close();
            if (provisional != null) { var abandoned = provisional; closeAfter(failure,() -> stopSnapshot(abandoned)); }
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
        Runnable ending=() -> {writer.close();writer.stopSources();};pendingCloses.put(ending,requireActive);
        var health=new CaptureHealth();Subscription source=null;CaptureRun provisional=null;
        try {
            source=writer.start(port,config,start,health);fixed.run();
            source=acknowledgements.follow(channels,opened,source,health,fixed);
            provisional=new CaptureRun(Optional.empty(),false,0L,Optional.empty(),Optional.of(source),health);
            register(provisional,requireActive);fixed.run();pendingCloses.remove(ending);return provisional;
        } catch(RuntimeException|Error failure) {
            writer.close();
            if(provisional!=null) { var abandoned=provisional;closeAfter(failure,() -> stopSnapshot(abandoned)); }
            else if(source!=null)closeAfter(failure,source::close);
            closeAfter(failure,() -> closePending(ending));throw failure;
        }
    }

    /** Opens the original private seam before returning; its CDC tail follows the bounded load on the same worker. */
    public synchronized CaptureRun beginDirectSnapshot(CaptureConfig config,DirectChannelProgress opened,DirectChannelStore channels,
            Function<DirectSnapshotPreparation,CapturePort> ports,CaptureHandoff handoff,int capacity,Runnable requireActive) {
        Runnable fixed=guard(requireActive);fixed.run();var preparation=new DirectSnapshotPreparation(opened,channels,fixed);
        SnapshotPhase.Load load=null;CaptureRun provisional=null;
        try {
            var port=Objects.requireNonNull(ports.apply(preparation),"prepared direct snapshot port");
            load=SnapshotPhase.openDirectPrepared(port,config,preparation,session -> retainSession(session,requireActive));fixed.run();var health=new CaptureHealth();
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
                Runnable ending=() -> {writer.close();writer.stopSources();};pendingCloses.put(ending,requireActive);
                Subscription source=null;
                try {
                    source=writer.start(port,config,CaptureStart.resume(new io.tapstate.spi.capture.SourcePosition(saved.token())),health);fixed.run();
                    var followed=acknowledgements.follow(channels,current,source,health,fixed);pendingCloses.remove(ending);return Optional.of(followed);
                } catch(RuntimeException|Error failure) {
                    writer.close();if(source!=null)closeAfter(failure,source::close);closeAfter(failure,() -> closePending(ending));throw failure;
                }
            },health,"tapstate-native-direct-load-"+opened.context().sourceId());
            var result=new CaptureRun(Optional.empty(),false,Optional.empty(),health,background);
            provisional=result;register(result,requireActive);fixed.run();background.start();return result;
        } catch(RuntimeException|Error failure) {
            preparation.close();
            if(provisional!=null){var abandoned=provisional;closeAfter(failure,() -> stopSnapshot(abandoned));}
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
            load=SnapshotPhase.openChainless(port,config,snapshotEpoch,batch -> retainBatch(batch,requireActive)); fixed.run(); var health=new CaptureHealth();
            CaptureHandoff guarded=new CaptureHandoff() {
                @Override public void accept(io.tapstate.core.event.Envelope row) { fixed.run(); handoff.accept(row); }
                @Override public void loaded(String table) { fixed.run(); handoff.loaded(table); }
            };
            var background=new BackgroundLoad(load,guarded,Optional::empty,health,"tapstate-native-bounded-"+config.connectorId());
            var result=new CaptureRun(Optional.empty(),false,Optional.empty(),health,background);
            provisional=result; register(result,requireActive); fixed.run(); background.start(); return result;
        } catch (RuntimeException|Error failure) {
            if (provisional!=null) { var abandoned=provisional; closeAfter(failure,() -> stopSnapshot(abandoned)); }
            else if (load!=null) closeAfter(failure,load::close);
            throw failure;
        }
    }

    public final class Tail implements AutoCloseable {
        private final SrsCaptureWriter writer;
        private volatile Subscription subscription;
        private final CaptureHealth health;
        private final Runnable owner;
        private final AtomicBoolean ended = new AtomicBoolean();
        private Tail(SrsCaptureWriter writer,Subscription subscription,CaptureHealth health,Runnable owner) {
            this.writer = writer; this.subscription = subscription; this.health = health; this.owner = owner;
        }
        public CaptureHealth health() { return health; }
        @Override public synchronized void close() {
            if (ended.get()) return;
            writer.close(); if(subscription!=null)subscription.close(); writer.stopSources(); ended.set(true); tails.remove(this);
        }
    }
    public synchronized void stopSnapshot(CaptureRun original) {
        if (snapshots.contains(Objects.requireNonNull(original,"original"))) { original.close(); snapshots.remove(original); runOwners.remove(original); }
    }
    private void register(CaptureRun original,Runnable owner) {runOwners.put(original,owner);snapshots.add(original);}
    /** Retries only this opening's retained handles, even when business authorization has ended. */
    public synchronized void stopOpenings(Runnable originalOwner) {
        Objects.requireNonNull(originalOwner);Throwable first=null;
        for(var run:List.copyOf(snapshots))if(runOwners.get(run)==originalOwner)first=closeAfter(first,() -> stopSnapshot(run));
        for(var tail:List.copyOf(tails))if(tail.owner==originalOwner)first=closeAfter(first,tail::close);
        for(var pending:List.copyOf(pendingCloses.keySet()))if(pendingCloses.get(pending)==originalOwner)first=closeAfter(first,() -> closePending(pending));
        if(first instanceof RuntimeException runtime)throw runtime;if(first instanceof Error error)throw error;
    }
    private SnapshotSession retainSession(SnapshotSession original,Runnable owner) {
        Runnable close=original::close;pendingCloses.put(close,owner);
        return new SnapshotSession() {
            @Override public CaptureBatch read(String table){return original.read(table);}
            @Override public void close(){closePending(close);}
        };
    }
    private CaptureBatch retainBatch(CaptureBatch original,Runnable owner) {
        Runnable close=original::close;pendingCloses.put(close,owner);
        return new CaptureBatch() {
            @Override public boolean hasNext(){return original.hasNext();}
            @Override public io.tapstate.core.event.Envelope next(){return original.next();}
            @Override public Optional<SourcePosition> seam(){return original.seam();}
            @Override public void close(){closePending(close);}
        };
    }
    private void closePending(Runnable original){original.run();pendingCloses.remove(original);}
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
        closed.set(true);
        Throwable first = null;
        for (var run : List.copyOf(snapshots)) first = closeAfter(first,() -> stopSnapshot(run));
        for (var tail : List.copyOf(tails)) first = closeAfter(first,tail::close);
        for(var pending:List.copyOf(pendingCloses.keySet()))first=closeAfter(first,() -> closePending(pending));
        first = closeAfter(first,acknowledgements::close);
        if (first instanceof RuntimeException runtime) throw runtime;
        if (first instanceof Error error) throw error;
    }
}
