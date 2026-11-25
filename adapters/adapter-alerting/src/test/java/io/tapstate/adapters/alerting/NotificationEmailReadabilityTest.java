package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.MessageSettings;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;
import static io.tapstate.adapters.alerting.NotificationRenderer.*;

class NotificationEmailReadabilityTest {
    @ParameterizedTest
    @EnumSource(Kind.class)
    void emailTextRemainsReadableWhenTheClientReplacesBodyStylesAndResetsHeadings(Kind kind) throws Exception {
        var original = kind == Kind.CHANNEL_TEST
                ? new Facts(kind, "test-delivery", 0, NotificationTemplateRenderTest.T, List.of(), 0,
                        null, List.of(), "test-1", "Email")
                : NotificationTemplateRenderTest.facts(kind);
        var facts = new Facts(original.kind(), original.deliveryId(), original.logicalSequence(), original.createdAt(),
                original.items(), original.totalItems(), URI.create("https://example.test/alerts"),
                original.previous(), original.testId(), original.channelName());
        var html = new NotificationRenderer().render(facts, new MessageSettings("", "", "Team guidance", "Team contact"))
                .snapshot().html();
        var failures = new ArrayList<String>();
        var stack = new ArrayDeque<Colors>();
        stack.push(new Colors("#13181d", "#ffffff"));
        new ParserDelegator().parse(new StringReader(html), new HTMLEditorKit.ParserCallback() {
            @Override public void handleStartTag(HTML.Tag tag, MutableAttributeSet attributes, int position) {
                var parent = stack.peek();
                String foreground = List.of(HTML.Tag.H1, HTML.Tag.H2, HTML.Tag.STRONG, HTML.Tag.TH).contains(tag)
                        ? "#13181d" : parent.foreground();
                String background = parent.background();
                // Webmail can replace the message body and impose its own semantic-element defaults.
                if (tag != HTML.Tag.BODY) {
                    Object style = attributes.getAttribute(HTML.Attribute.STYLE);
                    if (style != null) for (String declaration : style.toString().split(";")) {
                        String[] pair = declaration.split(":", 2);
                        if (pair.length != 2) continue;
                        if (pair[0].trim().equals("color")) foreground = pair[1].trim();
                        if (pair[0].trim().equals("background")) background = pair[1].trim();
                    }
                }
                stack.push(new Colors(foreground, background));
            }
            @Override public void handleEndTag(HTML.Tag tag, int position) {
                if (stack.size() > 1) stack.pop();
            }
            @Override public void handleText(char[] text, int position) {
                String content = new String(text);
                if (content.isBlank()) return;
                var colors = stack.peek();
                double contrast = contrast(colors.foreground(), colors.background());
                if (contrast < 4.5) failures.add(content + " (" + colors + ", contrast=" + contrast + ")");
            }
        }, true);
        assertThat(failures).as("readable text after a webmail client's body and heading reset").isEmpty();
    }

    private record Colors(String foreground, String background) {}

    private static double contrast(String foreground, String background) {
        double a = luminance(foreground), b = luminance(background);
        return (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
    }
    private static double luminance(String hex) {
        assertThat(hex).matches("#[0-9a-fA-F]{6}");
        return 0.2126 * linear(Integer.parseInt(hex.substring(1, 3), 16))
                + 0.7152 * linear(Integer.parseInt(hex.substring(3, 5), 16))
                + 0.0722 * linear(Integer.parseInt(hex.substring(5, 7), 16));
    }
    private static double linear(int channel) {
        double value = channel / 255.0;
        return value <= 0.04045 ? value / 12.92 : Math.pow((value + 0.055) / 1.055, 2.4);
    }
}
