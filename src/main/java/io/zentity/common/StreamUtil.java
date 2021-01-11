package io.zentity.common;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtil {
    /**
     * @see <a href="https://stackoverflow.com/a/24511534/4705719"></a>
     * @param iterable The iterable.
     * @param <T> The type of element.
     * @return A stream of all the elements.
     */
    public static <T> Stream<T> fromIterable(final Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * @see <a href="https://stackoverflow.com/a/24511534/4705719"></a>
     * @param iterator The iterator.
     * @param <T> The type of element.
     * @return A stream of all the elements.
     */
    public static <T> Stream<T> fromIterator(final Iterator<T> iterator) {
        return fromIterable(() -> iterator);
    }

    /**
     * Constructs a stateful function to be used in {@link Stream#flatMap} that buffers items into "tuple" arrays
     * of a single type. This is not re-usable between streams, as it is stateful. Parallel streams also might be
     * prone to issues with ordering.
     *
     * @param tupleHolder An array to hold elements as they are buffered. The size must be constant between tuples,
     *                    and items will only be emitted once the tuple is "full".
     * @param <T> The type of each item.
     * @return A function for flat mapping a single stream.
     */
    public static <T> Function<T, Stream<T[]>> tupleFlatmapper(T[] tupleHolder) {
        final int size = tupleHolder.length;
        final AtomicLong idxCounter = new AtomicLong(0);

        if (size == 0) {
            throw new IllegalArgumentException("Cannot have a zero-length tuple.");
        }

        return (T item) -> {
            int index = (int) (idxCounter.getAndIncrement() % size);

            tupleHolder[index] = item;

            if (index != size - 1) {
                return Stream.empty();
            }

            T[] fullTuple = tupleHolder.clone();

            return Stream.<T[]>builder()
                .add(fullTuple)
                .build();
        };
    }
}
