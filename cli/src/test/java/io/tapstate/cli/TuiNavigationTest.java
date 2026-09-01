package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiNavigationTest {

    @Test
    void movesWithinItemsAndReturnsFromDetailsWithoutLosingSelection() {
        TuiNavigation navigation = TuiNavigation.initial(List.of("orders", "audit"));

        TuiNavigation selected = navigation.move(1);
        TuiNavigation detail = selected.open();
        TuiNavigation returned = detail.back();

        assertThat(selected.selected()).isEqualTo("audit");
        assertThat(detail.detailOpen()).isTrue();
        assertThat(returned.detailOpen()).isFalse();
        assertThat(returned.selected()).isEqualTo("audit");
    }

    @Test
    void clampsSelectionAndKeepsAnEmptyCollectionInListMode() {
        TuiNavigation navigation = TuiNavigation.initial(List.of("orders")).move(99);

        assertThat(navigation.selected()).isEqualTo("orders");
        assertThat(TuiNavigation.initial(List.of()).open().detailOpen()).isFalse();
    }
}
