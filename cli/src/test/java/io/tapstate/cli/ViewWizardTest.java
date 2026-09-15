package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The interactive standalone {@code view} wizard, driven by a scripted prompter. A {@code kind: view}
 * definition is a reusable MDM sink body (id + primary key, no {@code from:} wiring — X19); the wizard
 * asks just those Tier-1 fields and builds the canonical artifact. Richer fields (storage, schema) are
 * authored by hand.
 */
class ViewWizardTest {

    private static String yaml(ViewResource v) {
        return new CanonicalWriter().write(v);
    }

    @Test
    void buildsAViewWithAPrimaryKey() {
        ScriptedPrompter p = new ScriptedPrompter("v_cust", "customer_id");
        ViewResource view = new ViewWizard(p).run();
        assertThat(yaml(view)).isEqualTo(
                """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: customer_id
                """);
    }

    @Test
    void repromptsABlankPrimaryKey() {
        // A view is materialized into a sink keyed on this field, so there is no legal view without
        // one and nothing sensible to default it to: a blank answer is asked again rather than kept.
        ScriptedPrompter p = new ScriptedPrompter("v_cust", "", "cust_id");
        ViewResource view = new ViewWizard(p).run();
        assertThat(yaml(view)).isEqualTo(
                """
                version: tapstate/v1
                kind: view
                id: v_cust
                primary_key: cust_id
                """);
    }

    @Test
    void defaultsTheViewIdWhenLeftBlank() {
        ScriptedPrompter p = new ScriptedPrompter("", "cust_id");
        ViewResource view = new ViewWizard(p).run();
        assertThat(view.id()).isEqualTo("view");
    }

    @Test
    void repromptsAnIdContainingADot() {
        // ids may not contain a dot (it would crash the parser); the wizard re-prompts on one
        ScriptedPrompter p = new ScriptedPrompter("a.b", "v_ok", "cust_id");
        ViewResource view = new ViewWizard(p).run();
        assertThat(view.id()).isEqualTo("v_ok");
    }

    @Test
    void wizardOutputIsACanonicalFixedPoint() {
        // a view the wizard writes must re-parse to an equal model: write(parse(write(m))) == write(m)
        ScriptedPrompter p = new ScriptedPrompter("v_cust", "customer_id");
        String once = yaml(new ViewWizard(p).run());
        String twice = new CanonicalWriter().write(new DslParser().parse(once));
        assertThat(twice).isEqualTo(once);
    }
}
