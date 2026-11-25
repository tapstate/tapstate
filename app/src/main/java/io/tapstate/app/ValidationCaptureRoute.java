package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.runtime.srs.CaptureObservation;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Objects;

/** One logical alias/table and its separately proven physical capture. Metadata is never a grant. */
record ValidationCaptureRoute(ExecutionFence execution,String alias,String table,CapturePreparation preparation,
        WorkloadClaimFence capture,String channel,long epoch,long membershipRevision) {
    ValidationCaptureRoute {
        Objects.requireNonNull(execution);Objects.requireNonNull(preparation);Objects.requireNonNull(capture);
        alias=identifier(alias,"capture.alias");table=identifier(table,"capture.table");
        channel=identifier(channel,"capture.channel");
        var scope=execution.scope();
        if(scope==null||!preparation.scope().equals(new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()))
                ||capture.key().type()!=WorkloadClaimType.CAPTURE||!capture.key().resourceId().equals(preparation.captureId())
                ||!capture.key().clusterId().equals(scope.clusterId())||epoch<1||epoch==Long.MAX_VALUE
                ||membershipRevision<0||membershipRevision==Long.MAX_VALUE
                ||preparation.mode()==CapturePreparation.Mode.SNAPSHOT_ONLY
                ||(preparation.mode()==CapturePreparation.Mode.SHARED_SRS)!=(membershipRevision>0)
                ||membershipRevision>0&&!channel.equals(preparation.chainId()))throw invalid();
    }
    static String identifier(String value,String field){if(value==null||value.isBlank())throw invalid();return ValidationError.fieldName(value,field);}
    /** A shared physical opening may have been opened by another alias or Pipeline. */
    void requireOpening(CaptureObservation.Context opening) {
        if(!preparation.captureId().equals(opening.captureId())||!channel.equals(opening.chainId())
                ||!capture.equals(opening.fence())||epoch!=opening.epoch()||!opening.tables().contains(table))throw invalid();
    }
    private static TapstateException invalid(){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","capture.originalRoute"),null);}
}
