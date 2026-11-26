package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineListTest {

    @Test
    void stopResultIsVisibleInBothReadResponses() {
        var catalog = MessageCatalog.bundled();
        for (var result : PipelineCatalogItem.StopState.values()) {
            var status = new PipelineCatalogItem.Status(PipelineCatalogItem.DisplayState.STOPPED,
                    PipelineCatalogItem.UiState.STOPPED, PipelineState.STOPPED, PipelineState.STOPPED,
                    null, null, null, result);
            var item = new PipelineCatalogItem("orders", "Orders", null, null, null, null, "hash", true, status);
            assertThat(PipelineList.of(List.of(item), catalog, Locale.ENGLISH).items().getFirst().status().stopState())
                    .isEqualTo(result);
            assertThat(PipelineStatusResponse.of(item, catalog).stopState()).isEqualTo(result);
        }
    }

    @Test
    void projectsExistingListFieldsAndLocalizesRuntimeAndIntentFailures() {
        var failure = new ObservationFailure("TS0060027",
                Map.of("connector", "mysql", "detail", "write refused"),
                Map.of("message", "captured runtime detail"));
        var status = new PipelineCatalogItem.Status(PipelineCatalogItem.DisplayState.FAILED,
                PipelineCatalogItem.UiState.FAILED, PipelineState.RUNNING, PipelineState.FAILED, failure,
                Instant.parse("2026-07-01T12:34:56Z"), new PipelineCatalogItem.IntentReadFailure(
                        IoError.DOCUMENT_UNREADABLE.code(), Map.of("id", "orders", "field", "intent")));
        var source = new PipelineCatalogItem("orders", "Orders", "description", "cdc", 7L,
                Instant.parse("2026-07-01T12:00:00Z"), "hash", true, status);

        PipelineList response = PipelineList.of(List.of(source), MessageCatalog.bundled(),
                Locale.forLanguageTag("zh-CN"));

        assertThat(response.items()).hasSize(1);
        PipelineList.Item item = response.items().getFirst();
        assertThat(item).extracting(PipelineList.Item::id, PipelineList.Item::name, PipelineList.Item::description,
                PipelineList.Item::mode, PipelineList.Item::revision, PipelineList.Item::contentHash,
                PipelineList.Item::hasArtifact)
                .containsExactly("orders", "Orders", "description", "cdc", 7L, "hash", true);
        assertThat(item.status().failure().code()).isEqualTo("TS0060027");
        assertThat(item.status().failure().params()).containsEntry("detail", "write refused");
        assertThat(item.status().failure().message()).contains("\u8fde\u63a5\u5668", "mysql", "\u5411\u76ee\u6807\u7aef\u5199\u5165\u6570\u636e")
                .doesNotContain("write refused");
        assertThat(item.status().failure().solution()).contains("\u76ee\u6807\u7aef\u8fde\u63a5", "\u5199\u5165\u6743\u9650", "\u6280\u672f\u8be6\u60c5");
        assertThat(item.status().failure().diagnostic()).containsEntry("message", "captured runtime detail");
        assertThat(item.status().intentReadFailure().code()).isEqualTo(IoError.DOCUMENT_UNREADABLE.code());
        assertThat(item.status().intentReadFailure().message())
                .contains("\u65e0\u6cd5\u8bfb\u53d6\u6807\u8bc6\u4e3a 'orders'", "'intent'");
        assertThat(item.status().intentReadFailure().solution())
                .contains("\u6587\u6863\u53ef\u80fd\u5df2\u635f\u574f", "\u68c0\u67e5\u6216\u6062\u590d");
    }
}
