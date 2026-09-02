package io.tapstate.cli;

import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Text;
import dev.tamboui.widgets.paragraph.Paragraph;

import java.util.stream.Collectors;

/**
 * TamboUI rendering adapter for the workbench state.
 *
 * <p>The reducer continues to own all screen state. This adapter deliberately has no terminal or
 * command concerns: TamboUI owns buffering and frame diffing while the existing dashboard model
 * remains the single presentation projection during the migration.
 */
final class TamboDashboard {

    private final TuiDashboard projection = new TuiDashboard();

    void render(Frame frame, TuiDashboard.State state) {
        String text = projection.render(state, frame.width(), frame.height()).stream()
                .map(Object::toString)
                .collect(Collectors.joining("\n"));
        frame.renderWidget(Paragraph.builder().text(Text.from(text)).build(), frame.area());
    }
}
