package io.tapstate.core.lifecycle;

import java.util.Map;

/**
 * How one metric that the flat {@code name -> value} view cannot hold whole is squeezed onto it: the flat
 * key a point with these attributes belongs under. Several points may answer with the same key, and what
 * happens to them then follows the metric's type rather than anything decided here — work adds up, a
 * reading does not.
 *
 * <p>The rule lives with the caller and not with the metric. What a metric is stays the same wherever it
 * is read, while how much of it a particular face can show is that face's own limitation; a fact that
 * carried its own flat spelling would be a fact shaped by the oldest thing reading it.
 *
 * <p>A rule answers for every point it is handed, or answers {@code null} to say it cannot. There is no
 * key meaning "leave this one out": a total short by the points nobody had a key for is indistinguishable
 * from a correct one, and {@link FlatMetricProjection} refuses a point it has no key for rather than
 * letting that happen quietly.
 *
 * <p>Which makes one shape worth naming, because it is the shape a rule written the obvious way has: a
 * rule that returns {@code prefix + attributes.get(dimension)} does not answer {@code null} when the
 * point lacks that dimension — it answers {@code prefix + "null"}, a key indistinguishable from a real
 * one, beside totals now short by exactly the points that landed on it. The guard cannot catch that,
 * because it is not a missing key; the rule has to ask whether the attribute is there.
 */
@FunctionalInterface
public interface FlatReduction {

    /**
     * The flat key a point carrying {@code attributes} belongs under, or {@code null} when this rule has
     * none for it — which the projection refuses, loudly, as a fact whose points it cannot account for.
     *
     * @param attributes the point's attributes, as measured — possibly empty, never {@code null}
     */
    String keyFor(Map<String, String> attributes);
}
