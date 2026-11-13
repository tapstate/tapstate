package io.tapstate.control.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Checking and repairing the groups of a running aggregate step.
 *
 * <p>Both compare a group's totals with the rows the step itself holds for that group. Neither reads the
 * source or the target, so a clean check says "the totals match the rows the step holds" and nothing
 * more: it does not say the step holds every source row, nor that the target holds the latest values.
 * No face may present it as agreement with the source.
 *
 * <p>They are answered by the running step, between two of its own updates, so a group is never read
 * while it is being written. A step that is not running cannot answer, and the request is refused by
 * name rather than answered from outside it.
 */
public interface AggregateStepChecks {

    /**
     * What is asked.
     *
     * @param group the one group to look at, as the text of each of its key values by name, with a null
     *              value for the null of that column; null to walk the step's groups a page at a time
     * @param after where the page before ended, as an earlier {@link Report#next()} gave it; null to
     *              start from the first group. Ignored when {@code group} is given
     */
    record Request(Map<String, String> group, String after) {

        public Request {
            group = group == null ? null : new LinkedHashMap<>(group);
        }
    }

    /**
     * One group that was not simply clean.
     *
     * @param group   the group, as the text of each of its key values by name
     * @param verdict {@code mismatch} when something derived differed from the group's rows,
     *                {@code inconclusive} when the group holds more than one request can read or rewrite
     *                and nothing is claimed about it, {@code absent} when the step holds nothing under
     *                the group that was named and nothing was changed or sent, {@code clean} for a group
     *                listed only because it cannot be given exactly now
     * @param fields  the output fields whose stored state differed; {@code (rows)} is the count of rows
     * @param code    the error code under which the group cannot be given exactly now, or null
     * @param field   the field that code is about, or null
     */
    record Finding(Map<String, String> group, String verdict, List<String> fields, String code, String field) {

        public Finding {
            group = new LinkedHashMap<>(group);
            fields = List.copyOf(fields);
        }
    }

    /**
     * @param pipeline  the pipeline asked about
     * @param step      the aggregate step asked about
     * @param repaired  whether the groups looked at were also written again from their rows and sent on
     * @param groups    how many groups this answer covers
     * @param findings  the groups that differed, could not be decided, or cannot be given
     * @param next      where the next page starts, to hand back as {@link Request#after()}; null when
     *                  the last group has been covered
     * @param conflicts input events the step has not applied since it started, because they carried an
     *                  order already stored with other content, or came for a row through another input
     * @param withheld  times since it started that the step sent a group at its last good values
     */
    record Report(String pipeline, String step, boolean repaired, int groups, List<Finding> findings, String next,
            long conflicts, long withheld) {

        public Report {
            findings = List.copyOf(findings);
        }
    }

    /** Compares the groups {@code request} names with their rows and changes nothing. */
    Report check(String pipelineId, String step, Request request);

    /**
     * Writes the totals of the groups {@code request} names again from their rows and sends each group
     * on, as {@code principal}. A row that is wrong or missing is not mended by this.
     */
    Report repair(String principal, String pipelineId, String step, Request request);
}
