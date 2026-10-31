package io.tapstate.app;

import io.tapstate.core.alerting.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class AlertRulePreviewTest {
    @ParameterizedTest @ValueSource(ints={0,3600})
    void zeroAndMaximumWindowsKeepTheirFirstConclusionAndExactVirtualTime(int seconds) {
        var form=Map.<String,Object>of("typeId","pipeline.failed","typeVersion",1,"resourceKinds",List.of("pipeline"),
                "allResources",true,"parameters",Map.of("trigger-for",seconds,"resolve-for",seconds));
        var scope=new AlarmKey.Scope("installation","cluster");
        var result=AlertRulePreview.evaluate(Map.of("form",form,"sample","GOOD"),scope,BuiltinAlarmTypes.catalog(),
                AlertChannelPolicy.Deployment.ON_PREM,"operator",Instant.parse("2026-10-27T00:00:00Z"));
        assertThat(result).containsEntry("initialStatus",seconds==0?"FIRING":"PENDING")
                .containsEntry("status","RESOLVED").containsEntry("quality","VALID")
                .containsEntry("virtualElapsedSeconds",2L*seconds+1)
                .containsEntry("synthetic",true).containsEntry("persisted",false).containsEntry("sent",false);
    }
}
