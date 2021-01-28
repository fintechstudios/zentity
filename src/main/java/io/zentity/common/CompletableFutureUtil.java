package io.zentity.common;

import io.zentity.common.FunctionalUtil.Recursable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CompletableFutureUtil {
    /**
     * Returns a new {@link CompletionStage} that, when the passed stage completes exceptionally,
     * is composed using the results of the supplied function applied to this stage's exception.
     *
     * @param stage The current future stage.
     * @param fn    The handler function.
     * @param <T>   The future result type.
     * @return The wrapped future stage.
     * @see <a href="https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/concurrent/CompletionStage.html#exceptionallyCompose(java.util.function.Function)"></a>
     * @see <a href="https://github.com/spotify/completable-futures/blob/25bbd6e0c1c6cef974112aeb859938a6e927f4c5/src/main/java/com/spotify/futures/CompletableFutures.java"></a>
     */
    public static <T> CompletionStage<T> composeExceptionally(CompletionStage<T> stage, Function<Throwable, ? extends CompletableFuture<T>> fn) {
        return stage
            .thenApply(CompletableFuture::completedFuture)
            .exceptionally(fn)
            .thenCompose(Function.identity());
    }

    /**
     * Returns a new {@link CompletableFuture} that, when the passed future completes exceptionally, is
     * composed using the results of the supplied function applied to this stage's exception.
     *
     * @param fut The current future.
     * @param fn  The handler function.
     * @param <T> The future result type.
     * @return The wrapped future.
     * @see <a href="https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/util/concurrent/CompletionStage.html#exceptionallyCompose(java.util.function.Function)"></a>
     * @see <a href="https://github.com/spotify/completable-futures/blob/25bbd6e0c1c6cef974112aeb859938a6e927f4c5/src/main/java/com/spotify/futures/CompletableFutures.java"></a>
     */
    public static <T> CompletableFuture<T> composeExceptionally(CompletableFuture<T> fut, Function<Throwable, ? extends CompletableFuture<T>> fn) {
        return composeExceptionally((CompletionStage<T>) fut, fn).toCompletableFuture();
    }

    /**
     * Unwrap a {@link CompletionException} to get the first cause that is not a {@link CompletionException}, or the
     * last-most {@link CompletionException} if there is no more specific cause.
     *
     * @param ex The exception.
     * @return The most specific {@link Throwable} that was found.
     */
    public static Throwable getCause(Throwable ex) {
        Objects.requireNonNull(ex, "exception cannot be null");
        if ((ex instanceof CompletionException || ex instanceof ExecutionException) && ex.getCause() != null) {
            return getCause(ex.getCause());
        }

        return ex;
    }

    /**
     * Like {@link CompletableFuture#completedFuture} but completed exceptionally.
     *
     * @param throwable the cause.
     * @param <T>       the future type.
     * @return the future, completed exceptionally.
     */
    public static <T> CompletableFuture<T> exceptionallyCompletedFuture(Throwable throwable) {
        CompletableFuture<T> fut = new CompletableFuture<>();
        fut.completeExceptionally(throwable);
        return fut;
    }

    /**
     * Create a looping recursive function.
     *
     * @param exitCondition  The exit condition predicate.
     * @param futureSupplier A function to get a new {@link CompletableFuture} for recursive chaining, called once each loop.
     * @param <T>            The type of the result of each loop.
     * @return A recursive function that will chain {@link CompletableFuture CompletableFutures} until the exit condition is met.
     */
    public static <T> Recursable<T, CompletableFuture<T>> recursiveLoopFunction(final Predicate<T> exitCondition, final Supplier<CompletableFuture<T>> futureSupplier) {
        return (val, f) -> {
            if (exitCondition.test(val)) {
                return CompletableFuture.completedFuture(val);
            }
            return futureSupplier.get().thenCompose(f);
        };
    }

    /**
     * Join all the results of a stream of futures into a list.
     *
     * @param futures The stream of {@link CompletableFuture CompletableFutures}.
     * @param <T> The item result type.
     * @return The list of results.
     */
    public static <T> List<T> joinAllOf(Stream<CompletableFuture<T>> futures) {
        return futures.map(CompletableFuture::join).collect(Collectors.toList());
    }

    /**
     * Join all the results of a collection of futures into a list.
     *
     * @param futures The collection of {@link CompletableFuture CompletableFutures}.
     * @param <T> The item result type.
     * @return The list of results.
     */
    public static <T> List<T> joinAllOf(Collection<CompletableFuture<T>> futures) {
        return joinAllOf(futures.stream());
    }

    /**
     * Like {@link CompletableFuture#allOf} but returns all the resulting values in a {@link List}.
     *
     * @param futures Collection of futures.
     * @param <T>     The type of result item.
     * @return A future that completes with the combined result list.
     */
    public static <T> CompletableFuture<List<T>> allOf(Collection<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allFut = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return allFut.thenApply(nil -> joinAllOf(futures));
    }

    /**
     * Like {@link CompletableFuture#allOf} but accepts a {@link Collection}.
     *
     * @param futures Collection of futures.
     * @return A future that completes when all futures in the collection are finished.
     */
    public static CompletableFuture<Void> allOfIgnored(Collection<CompletableFuture<?>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Run a list of async operations one after the other.
     *
     * @param suppliers A list of suppliers that kick off async work.
     * @param <T> The result type of a single async task.
     * @return A future with the results of all of them.
     */
    public static <T> CompletableFuture<List<T>> runSeries(Collection<Supplier<CompletableFuture<T>>> suppliers) {
        return suppliers
            .stream()
            .reduce(
                CompletableFuture.completedFuture(new ArrayList<>()),
                // accumulator
                (allFut, nextSupplier) -> allFut
                    .thenCompose(
                        (allItems) -> nextSupplier.get().thenApply((item) -> {
                            allItems.add(item);
                            return allItems;
                        })
                    ),
                // combiner
                (fut1, fut2) -> fut1.thenCombine(fut2, (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                })
            );
    }

    @SuppressWarnings("unchecked")
    static <T> CompletableFuture<List<T>> runParallelInChains(List<Supplier<CompletableFuture<T>>> suppliers, int parallelism) {
        final int size = suppliers.size();
        // Hold on to all supplied futures so that results can be ordered at the end
        CompletableFuture<T>[] futures = new CompletableFuture[size];

        final AtomicInteger currentIdx = new AtomicInteger(0);

        final CompletableFuture<Void> FINISHED = CompletableFuture.completedFuture(null);

        final Supplier<CompletableFuture<?>> nextFutureSupplier = () -> {
            int nextIdx = currentIdx.getAndIncrement();

            if (nextIdx >= size) {
                // signal that there are no more futures to supply
                return FINISHED;
            }

            CompletableFuture<T> nextFuture = suppliers.get(nextIdx).get();
            futures[nextIdx] = nextFuture;
            return nextFuture;
        };

        // recursively get the next future to run immediately after the last future finishes
        final Recursable<CompletableFuture<?>, CompletableFuture<?>> futureRunner = (fut, f) -> {
            if (fut == FINISHED) {
                return FINISHED;
            }
            return fut.thenCompose((ignored) -> f.apply(nextFutureSupplier.get()));
        };

        // "parallelism" number of "channels" for running as many at the same time as possible
        List<CompletableFuture<?>> channels = IntStream.range(0, parallelism)
            .mapToObj(i -> futureRunner.apply(CompletableFuture.completedFuture(null)))
            .collect(Collectors.toList());

        return allOfIgnored(channels).thenApply((ignored) -> joinAllOf(Arrays.stream(futures)));
    }

    static <T> CompletableFuture<List<T>> runParallelInPartitions(List<Supplier<CompletableFuture<T>>> suppliers, int parallelism) {
        Collection<List<Supplier<CompletableFuture<T>>>> partitions = CollectionUtil.partition(suppliers, parallelism);

        return partitions
            .stream()
            .reduce(
                CompletableFuture.completedFuture(new ArrayList<>()),
                // accumulator
                (allFut, nextBatch) -> allFut
                    .thenCompose(
                        (allItems) -> runSeries(nextBatch).thenApply((item) -> {
                            allItems.addAll(item);
                            return allItems;
                        })
                    ),
                // combiner
                (fut1, fut2) -> fut1.thenCombine(fut2, (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                })
            );
    }

    /**
     * Run a list of async operations one after the other.
     *
     * <p>
     *     NOTE: The current implementation recursively chains futures using {@link CompletableFuture#thenCompose}.
     *     Running long lists of futures will cause a StackOverflow exception, as the future chains
     *     will grow too long. To mitigate this for now, lists longer than 1,000 will be partitioned evenly
     *     and each partition will be run in parallel which is less efficient,
     *     especially when tasks widely vary in execution time. A nice optimization would be
     *     a non-recursive implementation, likely involving another class to manage execution and "chaining".
     *
     * @param suppliers A list of suppliers that kick off async work.
     * @param parallelism The max async tasks to run at one time.
     * @param <T> The result type of a single async task.
     * @return A future with the results of all of them.
     */
    public static <T> CompletableFuture<List<T>> runParallel(List<Supplier<CompletableFuture<T>>> suppliers, int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("Cannot have parallelism less than 1");
        }

        if (parallelism == 1) {
            return runSeries(suppliers);
        }

        if (suppliers.size() < 1000) {
            return runParallelInChains(suppliers, parallelism);
        }

        // otherwise, just partition and run each partition in series
        return runParallelInPartitions(suppliers, parallelism);
    }
}
