package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StorageControlCaller;
import io.tapstate.control.core.StorageControlDispatcher;
import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class StorageControlControllerTest {
    @Test void anUnassembledStorageBackendRemainsAnUnavailableDependency() throws Exception {
        var beans = new DefaultListableBeanFactory();
        var controller = new StorageControlController(beans.getBeanProvider(StorageControlDispatcher.class),beans.getBeanProvider(StorageControlCaller.class));
        var mvc = MockMvcBuilders.standaloneSetup(controller).setControllerAdvice(new ApiExceptionHandler(MessageCatalog.bundled())).build();
        var response = mvc.perform(post("/storage/status").contentType("application/json").content("{}"))
                .andExpect(status().isServiceUnavailable())
                .andReturn().getResponse();
        assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
        assertThat(response.getContentAsString()).contains(ControlError.STORAGE_UNAVAILABLE.code()).doesNotContain("401","mongodb://");
    }
    @Test void malformedJsonDoesNotExposeTheInputOrAParserCause() {
        for (String body : new String[]{"null","[]","{\"uri\":\"sentinel-password\"", ""}) {
            assertThatThrownBy(() -> StorageControlController.decode(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                    .isInstanceOfSatisfying(TapstateException.class,error -> {
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST); assertThat(error.getCause()).isNull();
                        assertThat(error.args().toString()).doesNotContain("sentinel-password");
                    });
        }
    }
    @Test void storageConflictsInputFailuresAndUnavailableDependenciesHaveDifferentHttpClasses() {
        assertThat(ApiExceptionHandler.statusFor(ControlError.STORAGE_TEST_REQUIRED).value()).isEqualTo(409);
        assertThat(ApiExceptionHandler.statusFor(ControlError.STORAGE_SCOPE_CONFLICT).value()).isEqualTo(409);
        assertThat(ApiExceptionHandler.statusFor(ControlError.STORAGE_TEST_FAILED).value()).isEqualTo(400);
        assertThat(ApiExceptionHandler.statusFor(ControlError.STORAGE_UNAVAILABLE).value()).isEqualTo(503);
        assertThat(ApiExceptionHandler.statusFor(ControlError.UNAUTHENTICATED).value()).isEqualTo(401);
    }
}
