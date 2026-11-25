package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.IoError;
import java.util.*;

/** Volatile evidence for one exact bounded run; only sink bounds can close the complete writer roster. */
final class SnapshotRecoveryFrontier {
    private final Map<String,Long> epochs;
    private Map<String,Set<String>> writers;
    private final Map<String,Set<String>> landed=new HashMap<>();

    SnapshotRecoveryFrontier(Map<String,Long> epochs) {
        this.epochs=Map.copyOf(epochs);
        if(epochs.isEmpty() || epochs.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue()<1 || entry.getValue()==Long.MAX_VALUE))throw refused();
    }
    synchronized void begin(Map<String,List<String>> roster) {
        if(!roster.keySet().equals(epochs.keySet()))throw refused();
        var copy=new TreeMap<String,Set<String>>();
        for(var entry:roster.entrySet()) {
            var ids=Set.copyOf(entry.getValue());
            if(ids.isEmpty() || ids.size()!=entry.getValue().size() || ids.stream().anyMatch(String::isBlank))throw refused();
            copy.put(entry.getKey(),ids);
        }
        if(writers!=null && !writers.equals(copy))throw refused();
        writers=Map.copyOf(copy);
    }
    synchronized void bounded(String stream,String writer,SourceOrder through) {
        if(writers==null || !writers.containsKey(stream) || !writers.get(stream).contains(writer)
                || through==null || !SourceOrder.snapshotRow(epochs.get(stream)).equals(through))throw refused();
        landed.computeIfAbsent(stream,ignored -> new HashSet<>()).add(writer);
    }
    synchronized boolean complete() {
        return writers!=null && writers.entrySet().stream().allMatch(entry -> landed.getOrDefault(entry.getKey(),Set.of()).equals(entry.getValue()));
    }
    private static TapstateException refused(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
