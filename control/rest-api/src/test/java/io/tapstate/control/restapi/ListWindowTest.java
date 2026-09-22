package io.tapstate.control.restapi;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListWindowTest {

    @Test
    void slicesAnExplicitWindowAndReturnsTheDefaultWindowWhenOnlyOffsetIsSet() {
        List<String> items = List.of("alpha", "bravo", "charlie", "delta");

        assertThat(ListWindow.page(items, 2, 1)).containsExactly("bravo", "charlie");
        assertThat(ListWindow.page(items, null, 2)).containsExactly("charlie", "delta");
        assertThat(ListWindow.page(items, 2, 9)).isEmpty();
    }

    @Test
    void rejectsInvalidListBoundsAtTheHttpBoundary() {
        assertThatThrownBy(() -> ListWindow.page(List.of("alpha"), 0, 0))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("control.malformed-request");
        assertThatThrownBy(() -> ListWindow.page(List.of("alpha"), 201, 0))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("control.malformed-request");
        assertThatThrownBy(() -> ListWindow.page(List.of("alpha"), 1, -1))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("control.malformed-request");
    }
}
