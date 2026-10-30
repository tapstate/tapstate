package io.tapstate.spi.store;

import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.SourceResource;
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
        String checked = checkedStart(scope,start);
        var tables = Set.copyOf(selected);
        if (tables.size() != selected.size()) throw new IllegalArgumentException("a private preparation cannot repeat a selected table");
        String key = source.srs() == null ? null : source.srs().key();
        var context = new DirectChannelContext(scope,SourceCaptureIdentity.directChannel(source.connector(),source.config(),key,scope.pipelineId(),source.id()),source.id(),
                SourceCaptureIdentity.directCapture(source.connector(),source.config(),key,scope.pipelineId(),source.id(),checked,tables));
        return new DirectChannelPreparation(context,tables,mode,SourceCaptureIdentity.physical(source.connector(),source.config()));
    }
    static String checkedStart(PipelineIntentScope scope,String start) {
        try { SourceCaptureIdentity.startAxis(start); return start == null ? "latest" : start; }
        catch (java.time.DateTimeException | ArithmeticException invalid) {
            throw new io.tapstate.core.common.TapstateException(IoError.DOCUMENT_INVALID,Map.of("id",scope.pipelineId(),"field","settings.start_from"),null);
        }
    }

}
