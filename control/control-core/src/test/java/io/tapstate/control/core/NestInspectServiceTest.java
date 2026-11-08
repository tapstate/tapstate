package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.NestInspection;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The nest inspect read hands back what the step holds, and every way it can find nothing is a refusal that
 * says what to do instead - never an empty view, which would read as a document with nothing in it.
 */
class NestInspectServiceTest {

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: order_doc
            source: [ shop_core ]
            transforms:
              - id: tidy
                type: filter
                from: [ orders ]
                expr: "true"
              - id: doc
                type: nest
                from: { o: orders, i: order_items }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - from: i
                      on: { order_id: id }
                      as: array
                      path: items
                      arrayKey: [ id ]
            serve:
              from: doc
              sync:
                - source: shop_target
            """;

    private static final NestInspection HELD = new NestInspection(NestInspection.VERSION, "order_doc", "doc",
            List.of("id"), List.of(1L), true, "1:3", List.of(), List.of(), List.of(), 0L, false,
            Map.of("id", 1L));

    @Test
    void whatTheStepHoldsIsHandedBackAsItWasRead() {
        assertThat(service((p, s, k) -> new NestStateInspector.Result.Held(HELD))
                .inspect("order_doc", "doc", List.of("1"))).isSameAs(HELD);
    }

    @Test
    void aStoppedPipelineIsRefusedAsNotRunningRatherThanEmpty() {
        TapstateException refused = refusal(NestStateInspector.NONE, "doc", List.of("1"));

        assertThat(refused.code()).isEqualTo(MonitorError.NEST_NOT_RUNNING);
        assertThat(refused.args()).containsEntry("step", "doc");
    }

    @Test
    void aKeyOfTheWrongWidthNamesTheColumns() {
        TapstateException refused = refusal((p, s, k) -> new NestStateInspector.Result.WrongWidth(List.of("id")),
                "doc", List.of("1", "2"));

        assertThat(refused.code()).isEqualTo(MonitorError.NEST_ROOT_KEY_WIDTH);
        assertThat(refused.args()).containsEntry("columns", "id").containsEntry("given", 2);
    }

    @Test
    void aKeyNothingIsHeldUnderSaysSo() {
        TapstateException refused = refusal((p, s, k) -> new NestStateInspector.Result.NotHeld(List.of("id")),
                "doc", List.of("404"));

        assertThat(refused.code()).isEqualTo(MonitorError.NEST_ROOT_KEY_NOT_HELD);
        assertThat(refused.args()).containsEntry("key", "404");
    }

    @Test
    void aStepThatIsNotANestIsRefusedBeforeAnythingIsRead() {
        TapstateException refused = refusal((p, s, k) -> {
            throw new AssertionError("a step that is not a nest is not read");
        }, "tidy", List.of("1"));

        assertThat(refused.code()).isEqualTo(MonitorError.UNKNOWN_NEST_STEP);
        assertThat(refused.args()).containsEntry("steps", "doc");
    }

    private static TapstateException refusal(NestStateInspector inspector, String step, List<String> key) {
        return catchThrowableOfType(TapstateException.class,
                () -> service(inspector).inspect("order_doc", step, key));
    }

    private static NestInspectService service(NestStateInspector inspector) {
        Resource pipeline = new DslParser().parse(PIPELINE);
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> saved) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.of(pipeline).filter(r -> r.id().equals(id));
            }

            @Override
            public List<Resource> list() {
                return List.of(pipeline);
            }
        });
        return new NestInspectService(artifacts, inspector);
    }
}
