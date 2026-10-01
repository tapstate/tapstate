package io.tapstate.control.restapi;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreviewControllerTest {

    @Test
    void readsARequestWithinTheByteLimit() throws IOException {
        byte[] body = {1, 2, 3, 4};

        assertThat(PipelinePreviewController.readBounded(new ByteArrayInputStream(body), body.length))
                .containsExactly(body);
    }

    @Test
    void refusesAnOversizedRequestWithACodedInputError() {
        byte[] body = {1, 2, 3, 4, 5};

        assertThatThrownBy(() -> PipelinePreviewController.readBounded(new ByteArrayInputStream(body), 4))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }
}
