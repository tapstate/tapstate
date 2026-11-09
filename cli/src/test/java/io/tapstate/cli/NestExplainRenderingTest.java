package io.tapstate.cli;

import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@code explain <pipeline> <step>} prints for a nest step: the tree with each embed's ownership and
 * join, the build line, one sample line per embed, and each warning with its rendered message and what to
 * change - or that the sample raised none.
 */
class NestExplainRenderingTest {

    private static final String ANSWER = """
            {"tree":{"pipelineId":"order_doc","step":"doc","rootAlias":"o","rootKey":["id"],"elementLimit":20000,
              "embeds":[
                {"path":"items","alias":"i","as":"array","owned":true,"on":{"order_id":"id"},"key":["id"],
                 "referrerLimit":0,"children":[
                   {"path":"items.notes","alias":"n","as":"array","owned":true,"on":{"item_id":"id"},"key":["id"],
                    "referrerLimit":0,"children":[]}]},
                {"path":"customer","alias":"c","as":"object","owned":false,"on":{"id":"customer_id"},"key":["id"],
                 "referrerLimit":100000,"children":[]}]},
             "build":"not available: this version has no build phase",
             "samples":[
               {"path":"items","alias":"i","source":"shop","table":"order_items","tableRows":100,"sampledRows":9,
                "largestInSample":9,"averagePerParent":10.0,"notSampled":null},
               {"path":"items.notes","alias":"n","source":null,"table":null,"tableRows":null,"sampledRows":0,
                "largestInSample":null,"averagePerParent":null,"notSampled":"source.connector-not-browsable"},
               {"path":"customer","alias":"c","source":"shop","table":"customers","tableRows":1,"sampledRows":10,
                "largestInSample":10,"averagePerParent":10.0,"notSampled":null}],
             "warnings":[
               {"code":"monitor.nest-reference-widely-shared","embed":"customer","args":{},
                "message":"One row of customers behind customer is pointed at by about 10 documents.",
                "solution":"Embed it from the other side."}]}
            """;

    @Test
    void eachEmbedShowsItsOwnershipJoinAndSampleAndEachWarningWhatToChange() {
        String text = render(ANSWER);

        assertThat(text).contains(
                "order_doc  step doc  root o  key id",
                "at most 20000 elements per document",
                "build: not available: this version has no build phase",
                "    items  from i  as array  owned, gathered: each row names its one parent  on order_id = parent id",
                "      sample: shop.order_items, 100 rows; at most 9 under one parent in 9 rows read; about 10.0 per document",
                "      items.notes  from n",
                "        sample: not sampled: source.connector-not-browsable",
                "    customer  from c  as object  shared, referenced: the parent names the row, at most 100000 "
                        + "documents per row  on id = parent customer_id",
                "about 10.0 documents per row",
                "    monitor.nest-reference-widely-shared  customer",
                "      Embed it from the other side.");
    }

    @Test
    void aSampleThatRaisedNothingSaysSo() {
        String text = render(ANSWER.replaceAll("(?s)\"warnings\":\\[.*\\]\\}\\s*$", "\"warnings\":[]}"));

        assertThat(text).contains("warnings: none from the sample").doesNotContain("monitor.");
    }

    private static String render(String json) {
        StringWriter text = new StringWriter();
        Repl.renderNestExplain(new PrintWriter(text, true), (Map<?, ?>) JsonReader.parse(json));
        return text.toString();
    }
}
