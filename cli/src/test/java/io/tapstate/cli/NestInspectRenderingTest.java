package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@code nest inspect} prints: the key by column, whether the root row is in and at what order, a row
 * per part with its own order, what waits for a parent, and the document - labelled as last sent or as about
 * to be sent, since the two differ exactly when changes are unsent.
 */
class NestInspectRenderingTest {

    @Test
    void aWholeDocumentListsItsPartsAndTheDocumentAsLastSent() {
        Map<String, Object> view = view(true, "1:3", List.of(
                part("items", List.of("L1"), "1:4"), part("items", List.of("L2"), "1:5"),
                part("customer", List.of("C1"), "1:2")), List.of(), 0, Map.of("id", 1, "items", List.of()));

        String text = render(view);

        assertThat(text).contains("order_doc  step doc  root key id=1", "root row: present, order 1:3",
                "items     [\"L1\"]  1:4    in document", "customer  [\"C1\"]  1:2    in document",
                "document, as last sent:");
    }

    @Test
    void anOrphansParentKeyShowsWhatIsHeldForIt() {
        Map<String, Object> view = view(false, null, List.of(part("items", List.of("L9"), "1:6")),
                List.of(Map.of("path", "items", "parentKey", "X", "held", 2)), 3, null);

        String text = render(view);

        assertThat(text).contains("root row: not arrived; what is below waits for it",
                "items  [\"L9\"]  1:6    held", "2 change(s) under items X, which has not arrived",
                "document: none while the root row is absent; 3 change(s) held");
    }

    private static String render(Map<String, Object> view) {
        StringWriter text = new StringWriter();
        Repl.renderNestInspect(new PrintWriter(text, true), view);
        return text.toString();
    }

    private static Map<String, Object> view(boolean present, String order, List<Map<String, Object>> parts,
            List<Map<String, Object>> waiting, long unsent, Map<String, Object> document) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("pipelineId", "order_doc");
        view.put("step", "doc");
        view.put("rootKeyColumns", List.of("id"));
        view.put("rootKey", List.of(1));
        view.put("rootPresent", present);
        view.put("rootOrder", order);
        view.put("parts", parts);
        view.put("waiting", waiting);
        view.put("missing", List.of());
        view.put("unsent", unsent);
        view.put("inTransit", false);
        view.put("document", document);
        return view;
    }

    private static Map<String, Object> part(String path, List<Object> key, String order) {
        return Map.of("path", path, "key", key, "order", order, "deleted", false);
    }
}
