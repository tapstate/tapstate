package io.tapstate.spi.store;

import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PipelineSourceConfig;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import java.util.*;

/** One immutable private source/read contract captured before first-cache preparation. */
public record DirectChannelPreparation(DirectChannelContext context,Set<String> tables,ReadMode readMode,String sourceIdentity) {
    public DirectChannelPreparation {
        Objects.requireNonNull(context,"context"); tables = Set.copyOf(tables); Objects.requireNonNull(readMode,"readMode");
        if (tables.isEmpty() || tables.stream().anyMatch(String::isBlank) || readMode == ReadMode.SNAPSHOT_ONLY
                || sourceIdentity == null || !sourceIdentity.matches("mc-[0-9a-f]{64}")) throw new IllegalArgumentException("invalid captured private source contract");
    }
    public static DirectChannelPreparation of(PipelineIntentScope scope,SourceResource source,ReadMode mode,String start,Collection<String> selected) {
        return of(scope, source, SourceRef.bare(source.id()), mode, start, selected);
    }
    public static DirectChannelPreparation of(PipelineIntentScope scope,SourceResource source,PipelineResource pipeline,ReadMode mode,String start,Collection<String> selected) {
        return of(scope, source, pipeline == null ? SourceRef.bare(source.id()) : pipeline.sourceReference(source.id()),
                mode, start, selected);
    }

    public static DirectChannelPreparation of(PipelineIntentScope scope,SourceResource source,SourceRef reference,
            ReadMode mode,String start,Collection<String> selected) {
        String checked = checkedStart(scope,start);
        var tables = Set.copyOf(selected);
        if (tables.size() != selected.size()) throw new IllegalArgumentException("a private preparation cannot repeat a selected table");
        String key = source.srs() == null ? null : source.srs().key();
        var settings = PipelineSourceConfig.settings(reference, source);
        var overrides = PipelineSourceConfig.overrides(reference, source);
        var context = new DirectChannelContext(scope,SourceCaptureIdentity.directChannel(source.connector(),settings,key,scope.pipelineId(),reference.binding(),overrides),reference.binding(),
                SourceCaptureIdentity.directCapture(source.connector(),settings,key,scope.pipelineId(),reference.binding(),checked,tables,overrides));
        return new DirectChannelPreparation(context,tables,mode,SourceCaptureIdentity.physical(source.connector(),settings));
    }
    static String checkedStart(PipelineIntentScope scope,String start) {
        try { SourceCaptureIdentity.startAxis(start); return start == null ? "latest" : start; }
        catch (java.time.DateTimeException | ArithmeticException invalid) {
            throw new io.tapstate.core.common.TapstateException(IoError.DOCUMENT_INVALID,Map.of("id",scope.pipelineId(),"field","settings.start_from"),null);
        }
    }

}
