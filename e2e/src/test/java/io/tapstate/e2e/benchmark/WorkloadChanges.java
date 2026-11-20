package io.tapstate.e2e.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

final class WorkloadChanges {
    enum Kind { INSERT, UPDATE, DELETE }

    record Event(Kind kind, long key, long ordinal) {
    }

    record Counts(int inserts, int updates, int deletes) {
        int total() {
            return Math.addExact(Math.addExact(inserts, updates), deletes);
        }

        Counts plus(Counts other) {
            return new Counts(Math.addExact(inserts, other.inserts), Math.addExact(updates, other.updates),
                    Math.addExact(deletes, other.deletes));
        }
    }

    private final List<WorkloadRecipe.Input> inputs;
    private final List<Keys> keys;
    private final WorkloadRecipe recipe;

    WorkloadChanges(WorkloadRecipe recipe, MongoWorkloadGenerator.HotKeys hotKeys) {
        this.recipe = recipe;
        inputs = recipe.inputs.stream().filter(WorkloadRecipe.Input::receivesChanges).toList();
        keys = inputs.stream().map(input -> new Keys(input, recipe.identity.seed(), hotKeys)).toList();
    }

    List<Counts> allocate(int events, WorkloadRecipe.Mix mix) {
        if (events < 0 || inputs.isEmpty()) {
            throw new IllegalArgumentException("Changes require a nonnegative event count and at least one mutable input");
        }
        int[] operation = allocate(events, new int[] {mix.insert(), mix.update(), mix.delete()});
        int[] weights = inputs.stream().mapToInt(WorkloadRecipe.Input::share).toArray();
        int[][] counts = allocateByInput(operation, weights);
        var result = new ArrayList<Counts>();
        for (int index = 0; index < inputs.size(); index++) {
            result.add(new Counts(counts[0][index], counts[1][index], counts[2][index]));
        }
        return result;
    }

    private static int[][] allocateByInput(int[] operations, int[] weights) {
        int[] capacity = allocate(Arrays.stream(operations).sum(), weights);
        int weightSum = Arrays.stream(weights).sum();
        int[][] counts = new int[operations.length][weights.length];
        int[] remaining = operations.clone();
        // Reserve the floor of each cell before any operation may claim a shared input remainder.
        for (int operation = 0; operation < operations.length; operation++) {
            for (int input = 0; input < weights.length; input++) {
                int floor = (int) ((long) operations[operation] * weights[input] / weightSum);
                counts[operation][input] = floor;
                remaining[operation] -= floor;
                capacity[input] -= floor;
            }
        }
        for (int operation = 0; operation < operations.length; operation++) {
            for (int extra = 0; extra < remaining[operation]; extra++) {
                int best = -1;
                long bestDeficit = Long.MIN_VALUE;
                for (int input = 0; input < weights.length; input++) {
                    long deficit = (long) operations[operation] * weights[input]
                            - (long) counts[operation][input] * weightSum;
                    if (capacity[input] > 0 && deficit > bestDeficit) {
                        best = input;
                        bestDeficit = deficit;
                    }
                }
                counts[operation][best]++;
                capacity[best]--;
            }
        }
        return counts;
    }
    private static int[] allocate(int total, int[] weights) {
        int sum = Arrays.stream(weights).sum();
        if (sum <= 0) {
            throw new IllegalArgumentException("Change stream weights must have a positive sum");
        }
        int[] result = new int[weights.length];
        List<Integer> sorted = new ArrayList<>(weights.length);
        int used = 0;
        for (int index = 0; index < weights.length; index++) {
            result[index] = (int) ((long) total * weights[index] / sum);
            used += result[index];
            sorted.add(index);
        }
        sorted.sort(Comparator.<Integer>comparingLong(index -> (long) total * weights[index] % sum).reversed());
        for (int index = 0; index < total - used; index++) {
            result[sorted.get(index)]++;
        }
        return result;
    }

    void validate(List<Counts> warmup, List<Counts> measured) {
        for (int index = 0; index < inputs.size(); index++) {
            var input = inputs.get(index);
            var first = warmup.get(index);
            var second = measured.get(index);
            long initialNeeded = (long) initialMutations(first) + initialMutations(second);
            if (recipe.changeMode != WorkloadRecipe.Changes.STEADY && initialNeeded > input.rows()) {
                throw new IllegalArgumentException("Insufficient pre-existing keys for " + input.name()
                        + ": " + initialNeeded + " mutations need distinct keys");
            }
            if ((long) input.rows() + first.inserts() + second.inserts() >= WorkloadRecipe.KEY_LANE) {
                throw new IllegalArgumentException("Event keys exceed the reserved range for " + input.name());
            }
        }
    }

    private static int initialMutations(Counts counts) {
        return Math.max(0, counts.updates() - counts.inserts()) + counts.deletes();
    }

    List<Stream> phase(List<Counts> counts) {
        var streams = new ArrayList<Stream>();
        for (int index = 0; index < inputs.size(); index++) {
            streams.add(new Stream(inputs.get(index), keys.get(index), counts.get(index),
                    recipe.changeMode == WorkloadRecipe.Changes.STEADY));
        }
        return streams;
    }

    static final class Stream {
        final WorkloadRecipe.Input input;
        final Counts requested;
        final Keys keys;
        private final boolean steady;
        private int insertsOnly;
        private int pairs;
        private int updatesOnly;
        private int deletes;
        private long pairedKey;

        Stream(WorkloadRecipe.Input input, Keys keys, Counts requested, boolean steady) {
            this.input = input;
            this.keys = keys;
            this.requested = requested;
            this.steady = steady;
            pairs = Math.min(requested.inserts(), requested.updates());
            insertsOnly = requested.inserts() - pairs;
            updatesOnly = requested.updates() - pairs;
            deletes = requested.deletes();
        }

        Event next() {
            if (pairedKey != 0) {
                long key = pairedKey;
                pairedKey = 0;
                return new Event(Kind.UPDATE, key, keys.ordinal++);
            }
            int remaining = insertsOnly + pairs + updatesOnly + deletes;
            if (remaining == 0) {
                return null;
            }
            int selected = keys.order.nextInt(remaining);
            if (selected < insertsOnly) {
                insertsOnly--;
                return new Event(Kind.INSERT, keys.nextInsert++, keys.ordinal++);
            }
            selected -= insertsOnly;
            if (selected < pairs) {
                pairs--;
                pairedKey = keys.nextInsert++;
                return new Event(Kind.INSERT, pairedKey, keys.ordinal++);
            }
            selected -= pairs;
            if (selected < updatesOnly) {
                updatesOnly--;
                return new Event(Kind.UPDATE, keys.initial(steady), keys.ordinal++);
            }
            deletes--;
            return new Event(Kind.DELETE, keys.initial(false), keys.ordinal++);
        }
    }

    static final class Keys {
        final SplittableRandom order;
        private final SplittableRandom selection;
        private final long first;
        private final int rows;
        private final int eventHotPercent;
        private final Sample cold;
        private final Sample hot;
        long nextInsert;
        long ordinal;
        long hotSelections;

        Keys(WorkloadRecipe.Input input, long seed, MongoWorkloadGenerator.HotKeys skew) {
            first = input.firstKey();
            rows = input.rows();
            nextInsert = first + rows;
            order = new SplittableRandom(seed ^ first);
            selection = order.split();
            int hotRows = skew == null ? 0 : Math.max(1, (int) ((long) rows * skew.keyPercent() / 100));
            eventHotPercent = skew == null ? 0 : skew.eventPercent();
            hot = new Sample(0, hotRows);
            cold = new Sample(hotRows, rows - hotRows);
        }

        long initial(boolean repeat) {
            if (repeat) {
                return first + selection.nextInt(rows);
            }
            if (hot.left > 0 && (cold.left == 0 || selection.nextInt(100) < eventHotPercent)) {
                hotSelections++;
                return first + hot.take(selection);
            }
            return first + cold.take(selection);
        }
    }

    /** Sparse partial shuffle. It retains selected-key state and no generated documents. */
    private static final class Sample {
        private final int offset;
        private final Map<Integer, Integer> moved = new HashMap<>();
        private int left;

        Sample(int offset, int size) {
            this.offset = offset;
            left = size;
        }

        int take(SplittableRandom random) {
            int at = random.nextInt(left);
            int result = moved.getOrDefault(at, at);
            int last = --left;
            if (at != last) {
                moved.put(at, moved.getOrDefault(last, last));
            }
            moved.remove(last);
            return offset + result;
        }
    }
}
