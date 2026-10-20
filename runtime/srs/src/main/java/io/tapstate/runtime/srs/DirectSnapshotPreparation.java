package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.SnapshotPreparation;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/** One original private load preserves its saved seam and orders before the following change stream. */
public final class DirectSnapshotPreparation implements AutoCloseable {
    private enum Phase { CREATED, OPEN, CLOSED }
    private final AtomicReference<Phase> phase=new AtomicReference<>(Phase.CREATED);
    private final DirectChannelProgress original;
    private final DirectChannelStore channels;
    private final Runnable active;
    private final List<String> tables,owed;
    private final SnapshotPreparation request;
    private final AtomicReference<SourcePosition> accepted=new AtomicReference<>();

    public DirectSnapshotPreparation(DirectChannelProgress original,DirectChannelStore channels,Runnable requireActive) {
        this.original=Objects.requireNonNull(original,"original");this.active=Objects.requireNonNull(requireActive,"requireActive");
        if(original.current()==null)throw new IllegalArgumentException("a private snapshot requires its original opened capture");
        this.channels=Objects.requireNonNull(channels,"channels").guardedBy(this::requireUsable);
        tables=original.tables().stream().sorted().toList();owed=tables.stream().filter(table -> !original.cursor().snapshotCompletedTables().contains(table)).toList();
        long epoch=original.cursor().snapshotEpoch();String anchor=original.cursor().cdcStartPosition();
        request=new SnapshotPreparation(epoch==0?original.epoch():epoch,anchor==null?null:new SourcePosition(anchor));requireUsable();
    }
    public SnapshotPreparation request(){return request;}
    public List<String> tables(){return tables;}
    public List<String> stillOwed(){return owed;}
    public String channel(){return original.context().channelId();}

    /** The host completes these original CACHE writes before the connector may read its first row. */
    public void prepare(SnapshotPreparation actual,SourcePosition sampledSeam,Map<String,SourceTable> actualTables) {
        requireReading();var captured=Map.copyOf(actualTables);
        if(!request.equals(actual) || sampledSeam==null || !new HashSet<>(owed).equals(captured.keySet())
                || captured.entrySet().stream().anyMatch(entry -> !entry.getKey().equals(entry.getValue().name())))throw unavailable();
        var pin=request.replayAnchor()==null?sampledSeam:request.replayAnchor();
        if(!channels.pinSnapshot(original.context(),original.revision(),request.snapshotEpoch(),pin.token(),original.current().pipelineFence()))throw unavailable();
        confirmBaseline(pin);requireReading();
        var before=accepted.compareAndExchange(null,pin);if(before!=null && !before.equals(pin))throw unavailable();
    }
    void prepareRetained() {
        requireReading();if(!owed.isEmpty() || request.replayAnchor()==null)throw unavailable();
        confirmBaseline(request.replayAnchor());requireReading();accepted.set(request.replayAnchor());
    }
    private void confirmBaseline(SourcePosition first) {
        var saved=original.cursor().sinkAcked();String token=saved==null?first.token():saved.token();
        if(!channels.confirmAnchor(original.context(),original.epoch(),token,original.current().captureFence(),original.current().pipelineFence()))throw unavailable();
    }
    public SourcePosition accepted(){requireReading();var pin=accepted.get();if(pin==null)throw unavailable();return pin;}
    void beginRead(){requireUsable();if(!phase.compareAndSet(Phase.CREATED,Phase.OPEN))throw unavailable();}
    void requireReading(){requireUsable();if(phase.get()!=Phase.OPEN)throw unavailable();}
    private void requireUsable(){active.run();if(phase.get()==Phase.CLOSED)throw unavailable();}
    @Override public void close(){phase.set(Phase.CLOSED);}
    private TapstateException unavailable(){return new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE,Map.of("channel",channel()),null);}
}
