package io.zentity.common;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {
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
}
