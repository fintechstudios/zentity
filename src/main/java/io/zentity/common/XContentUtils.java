package io.zentity.common;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

public class XContentUtils {
    public static String serialize(XContentBuilder builder) {
        return Strings.toString(builder);
    }

    public static String serialize(XContentBuilder builder, ToXContent content, ToXContent.Params params) throws IOException {
        return serialize(content.toXContent(builder, params));
    }

    /**
     * A simple helper for serializing JSON.
     *
     * @param content The query to serialize.
     * @param params The xcontent params.
     * @return The JSON string.
     * @throws IOException When there is an issue with serialization.
     */
    public static String serializeAsJSON(ToXContent content, ToXContent.Params params) throws IOException {
        return serialize(XContentFactory.jsonBuilder(), content, params);
    }

    /**
     * A simple helper for serializing JSON.
     *
     * @param content The query to serialize.
     * @return The JSON string.
     * @throws IOException When there is an issue with serialization.
     */
    public static String serializeAsJSON(ToXContent content) throws IOException {
        return serializeAsJSON(content, ToXContent.EMPTY_PARAMS);
    }

    public static UnaryOperator<XContentBuilder> composeModifiers(List<UnaryOperator<XContentBuilder>> builderModifiers) {
        return builderModifiers
            .stream()
            .reduce(
                UnaryOperator.identity(),
                (mod1, mod2) -> (builder) -> mod1.andThen(mod2).apply(builder),
                (mod1, mod2) -> (builder) -> mod1.andThen(mod2).apply(builder)
            );
    }

    public static XContentBuilder jsonBuilder() throws IOException {
        return XContentFactory.jsonBuilder();
    }

    public static XContentBuilder jsonBuilder(UnaryOperator<XContentBuilder> modifier) throws IOException {
        return modifier.apply(jsonBuilder());
    }

    /**
     * Wrap a {@link CheckedFunction} into something that can be used as a builder modifier.
     *
     * @param checkedFunction The function that throws a checked exception.
     * @return A wrapped function that re-throws any caught checked exceptions as {@link RuntimeException RuntimeExceptions}.
     */
    public static UnaryOperator<XContentBuilder> uncheckedModifier(CheckedFunction<XContentBuilder, XContentBuilder, ? extends Exception> checkedFunction) {
        return (builder) -> {
            try {
                return checkedFunction.apply(builder);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
