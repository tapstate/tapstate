package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The interactive standalone {@code transform} wizard's question flow, driven by a scripted prompter.
 * A standalone transform is a reusable definition body (X19): pure logic, no {@code from:} wiring.
 * Asserts on the canonical artifact the answers produce.
 */
class TransformWizardTest {

    private static String yaml(TransformResource t) {
        return new CanonicalWriter().write(t);
    }

    @Test
    void buildsAFilterTransform() {
        ScriptedPrompter p = new ScriptedPrompter("live_only", "filter", "op != 'd'");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: live_only
                type: filter
                expr: "op != 'd'"
                """);
    }

    @Test
    void buildsAMapTransformWithRenameAndDropRules() {
        // each field is name + rule: $src rename / false drop; blank name ends the field list
        ScriptedPrompter p = new ScriptedPrompter(
                "mask_pii", "map", "ssn", "false", "phone", "false", "");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: mask_pii
                type: map
                fields:
                  ssn: false
                  phone: false
                """);
    }

    @Test
    void buildsAJsTransformAsAMultilineLiteralBlock() {
        // the whole script is captured as one multi-line block via the lines() primitive
        ScriptedPrompter p = new ScriptedPrompter("parse", "js", "emit(after)\nemit(before)");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: parse
                type: js
                script: |
                  emit(after)
                  emit(before)
                """);
    }

    @Test
    void buildsAUnionTransform() {
        ScriptedPrompter p = new ScriptedPrompter("merged", "union");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: merged
                type: union
                """);
    }

    @Test
    void buildsAJoinTransformWithTheOnlyEngineAndMultilineSql() {
        // the wizard asks no engine question any more, so the replies are id, type and the SQL
        ScriptedPrompter p = new ScriptedPrompter(
                "cust_wide", "join",
                "SELECT c.id AS customer_id, o.amount AS amount\nFROM c JOIN o ON o.customer_id = c.id");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: cust_wide
                type: join
                engine: builtin
                sql: |
                  SELECT c.id AS customer_id, o.amount AS amount
                  FROM c JOIN o ON o.customer_id = c.id
                """);
    }

    @Test
    void buildsANestTreeWithRecursiveEmbeds() {
        // root.from / embed.from are abstract aliases bound at the use site (X19); the tree nests one
        // child under another (policy -> claim). Tier-2 fields (mode / order / ignoreUpdates) are hand-edited.
        ScriptedPrompter p = new ScriptedPrompter(
                "c360_shape", "nest",
                "customer", "customer_id",            // root: from, key
                "embed",                              // add a child embed
                "policy", "CUST_ID", "customer_id", "", // policy: from, on(child,parent), end on
                "array", "policies", "POLICY_ID",     // as, path, arrayKey
                "embed",                              // nested embed under policy
                "claim", "POLICY_ID", "POLICY_ID", "", // claim: from, on, end on
                "array", "claims", "CLAIM_ID",        // as, path, arrayKey
                "(done)",                             // claim has no children
                "(done)");                            // root has no more children
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: c360_shape
                type: nest
                root:
                  from: customer
                  key: [customer_id]
                  embed:
                    - from: policy
                      on:
                        CUST_ID: customer_id
                      as: array
                      path: policies
                      arrayKey: [POLICY_ID]
                      embed:
                        - from: claim
                          on:
                            POLICY_ID: POLICY_ID
                          as: array
                          path: claims
                          arrayKey: [CLAIM_ID]
                """);
    }

    @Test
    void buildsANestWithAnObjectEmbedAndNoArrayKey() {
        // an object (1:1) embed has no arrayKey question; a blank root key is omitted
        ScriptedPrompter p = new ScriptedPrompter(
                "addr_shape", "nest",
                "customer", "",                       // root: from, no key
                "embed",
                "address", "CUST_ID", "id", "", // address: from, on, end on
                "object", "address",                  // as object, path (no arrayKey asked)
                "(done)",                             // address has no children
                "(done)");                            // root has no more children
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: addr_shape
                type: nest
                root:
                  from: customer
                  embed:
                    - from: address
                      on:
                        CUST_ID: id
                      as: object
                      path: address
                """);
    }

    @Test
    void buildsAMapTransformWithAllFourRuleForms() {
        // $src rename / =expr computed / false drop / plain literal — every rule form in one map
        ScriptedPrompter p = new ScriptedPrompter(
                "shape", "map",
                "customer_id", "$cust_no",
                "joined_at", "=timestamp(after.join_date)",
                "ssn", "false",
                "region", "EU",
                "");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: shape
                type: map
                fields:
                  customer_id: $cust_no
                  joined_at: "=timestamp(after.join_date)"
                  ssn: false
                  region: EU
                """);
    }

    @Test
    void buildsANestEmbedWithACompositeJoinKey() {
        // an embed's on: maps multiple child fields to parent fields (a composite join key); the on map
        // is emitted in canonical (dictionary) order regardless of the order the pairs were entered
        ScriptedPrompter p = new ScriptedPrompter(
                "comp", "nest",
                "customer", "customer_id",
                "embed",
                "policy", "POLICY_ID", "p_id", "BRANCH_ID", "b_id", "",
                "array", "policies", "POLICY_ID",
                "(done)", "(done)");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: comp
                type: nest
                root:
                  from: customer
                  key: [customer_id]
                  embed:
                    - from: policy
                      on:
                        BRANCH_ID: b_id
                        POLICY_ID: p_id
                      as: array
                      path: policies
                      arrayKey: [POLICY_ID]
                """);
    }

    @Test
    void buildsANestWithMultipleSiblingEmbeds() {
        // a root with two sibling children at the same level (array policy + object address)
        ScriptedPrompter p = new ScriptedPrompter(
                "multi", "nest",
                "customer", "customer_id",
                "embed", "policy", "CUST_ID", "customer_id", "", "array", "policies", "POLICY_ID", "(done)",
                "embed", "address", "CUST_ID", "customer_id", "", "object", "address", "(done)",
                "(done)");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: multi
                type: nest
                root:
                  from: customer
                  key: [customer_id]
                  embed:
                    - from: policy
                      on:
                        CUST_ID: customer_id
                      as: array
                      path: policies
                      arrayKey: [POLICY_ID]
                    - from: address
                      on:
                        CUST_ID: customer_id
                      as: object
                      path: address
                """);
    }

    @Test
    void buildsANestWithNoEmbeds() {
        // declining the first embed yields a root-only nest; the embed block is omitted entirely
        ScriptedPrompter p = new ScriptedPrompter("root_only", "nest", "customer", "customer_id", "(done)");
        assertThat(yaml(new TransformWizard(p).run())).isEqualTo(
                """
                version: tapstate/v1
                kind: transform
                id: root_only
                type: nest
                root:
                  from: customer
                  key: [customer_id]
                """);
    }

    @Test
    void reAsksTheTypeWhenAMapHasNoFields() {
        // a fields-less map cannot round-trip; picking map then entering no fields re-asks the type
        ScriptedPrompter p = new ScriptedPrompter("t1", "map", "", "union");
        TransformResource t = new TransformWizard(p).run();
        assertThat(t.body()).isInstanceOf(TransformBody.Union.class);
    }

    @Test
    void defaultsTheTransformIdWhenLeftBlank() {
        ScriptedPrompter p = new ScriptedPrompter("", "union");
        assertThat(new TransformWizard(p).run().id()).isEqualTo("transform");
    }

    @Test
    void repromptsAnIdContainingADot() {
        // ids may not contain a dot (it would crash the parser); the wizard re-prompts on one
        ScriptedPrompter p = new ScriptedPrompter("a.b", "u_ok", "union");
        assertThat(new TransformWizard(p).run().id()).isEqualTo("u_ok");
    }

    @Test
    void everyTransformShapeIsACanonicalFixedPoint() {
        // each shape the wizard can produce must re-parse to an equal model: write(parse(write(m))) == write(m)
        assertFixedPoint(new ScriptedPrompter("live_only", "filter", "op != 'd'"));
        assertFixedPoint(new ScriptedPrompter("mask_pii", "map", "ssn", "false", ""));
        assertFixedPoint(new ScriptedPrompter("parse", "js", "emit(after)"));
        assertFixedPoint(new ScriptedPrompter("merged", "union"));
        assertFixedPoint(new ScriptedPrompter("wide", "join", "SELECT 1 FROM c"));
        assertFixedPoint(new ScriptedPrompter("root_only", "nest", "customer", "customer_id", "(done)"));
        // a deep tree (root -> policy -> claim) round-trips, exercising recursive embed serialization
        assertFixedPoint(new ScriptedPrompter(
                "c360_shape", "nest", "customer", "customer_id",
                "embed", "policy", "CUST_ID", "customer_id", "", "array", "policies", "POLICY_ID",
                "embed", "claim", "POLICY_ID", "POLICY_ID", "", "array", "claims", "CLAIM_ID",
                "(done)", "(done)"));
        // a wide tree (two sibling embeds) round-trips, exercising the sibling loop + object embed
        assertFixedPoint(new ScriptedPrompter(
                "multi", "nest", "customer", "customer_id",
                "embed", "policy", "CUST_ID", "customer_id", "", "array", "policies", "POLICY_ID", "(done)",
                "embed", "address", "CUST_ID", "customer_id", "", "object", "address", "(done)",
                "(done)"));
    }

    private static void assertFixedPoint(ScriptedPrompter p) {
        String once = yaml(new TransformWizard(p).run());
        String twice = new CanonicalWriter().write(new DslParser().parse(once));
        assertThat(twice).isEqualTo(once);
    }
}
