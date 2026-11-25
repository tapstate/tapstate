package io.tapstate.core.model.canonical;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SourceCaptureIdentityTest {
    private static final Map<String,Object> SETTINGS = Map.of("database","x","tags",List.of("a|b",7,true));
    @Test void retainedPhysicalAndPrivateNamespacesHaveFixedGoldenValues() {
        assertThat(SourceCaptureIdentity.physical("connector",SETTINGS)).isEqualTo("mc-df17d04af39bc891f6c45a6f67cea63ed8610bf81ebe4face688720968493416");
        assertThat(SourceCaptureIdentity.directChannel("connector",SETTINGS,null,"pipeline","source")).isEqualTo("mcd-c1b434b4b17268f8d31fe701ff252a2f587b6e5ed559f36f5270dcc671b7af89");
        assertThat(SourceCaptureIdentity.directChannel("connector",SETTINGS,"forced","pipeline","source")).isEqualTo("mcd-b1f2429426cde071899da5465330ae7538408705348b1ace07c5f1122cbbd736");
    }
    @Test void privateCaptureGoldensKeepStartModeAndSortedStreamBoundaries() {
        var starts = List.of("latest","earliest","2026-10-19T08:00:00.123+08:00");
        var derived = List.of("3394acca4a26ba7cdf8d11fe1c0ea051be2f1529299bc45251a610467ee8acf1","a8a2e5a012c6e0bf9768ad2c8a17211c9088fb853d1b83f27f2b998263b93563","e758f84ecb95813c3685d29f4aa5f9778cfbf9ccd9ee19c1bae5f7306dd612bb");
        var keyed = List.of("49414f2f8c39d78326a5cec240322d07879d7383f7f531df424ef87161d01a62","1a30f25c71c0e4ae0c50a2020bab2a4eb3fd971dc7a37d27c3328fb068ed400a","cf0d837e2ec2d06c3b94e826cb6fc54033a189351315f9257737a430acc09675");
        for (int i=0;i<starts.size();i++) {
            assertThat(SourceCaptureIdentity.directCapture("connector",SETTINGS,null,"pipeline","source",starts.get(i),List.of("quiet","orders"))).isEqualTo("capture-"+derived.get(i));
            assertThat(SourceCaptureIdentity.directCapture("connector",SETTINGS,"forced","pipeline","source",starts.get(i),List.of("orders","quiet"))).isEqualTo("capture-"+keyed.get(i));
        }
    }
    @Test void reorderedSettingsAndNormalizedEquivalentStartsDoNotChangeIdentity() {
        var reordered = new LinkedHashMap<String,Object>(); reordered.put("tags",List.of("a|b",7L,true)); reordered.put("database","x");
        assertThat(SourceCaptureIdentity.physical("connector",reordered)).isEqualTo(SourceCaptureIdentity.physical("connector",SETTINGS));
        assertThat(SourceCaptureIdentity.directCapture("connector",SETTINGS,null,"pipeline","source","2026-10-19T00:00:00.123Z",List.of("quiet","orders")))
                .isEqualTo(SourceCaptureIdentity.directCapture("connector",SETTINGS,null,"pipeline","source","2026-10-19T08:00:00.123+08:00",List.of("orders","quiet")));
    }
}
