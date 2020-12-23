package io.zentity.resolution;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.function.UnaryOperator;

public class XContentUtils {
    public static String serialize(XContentBuilder builder, ToXContent content, ToXContent.Params params) throws IOException {
        return Strings.toString(content.toXContent(builder, params));
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

    /**
     * A helper that allows modifying the JSON {@link XContentBuilder} before it is used.
     *
     * @param builderModifier The function to modify the
     * @param content The content to serialize.
     * @param params The params for the builder.
     * @return The JSON string.
     * @throws IOException When there is an issue with serialization.
     */
    public static String serializeAsJson(UnaryOperator<XContentBuilder> builderModifier, ToXContent content, ToXContent.Params params) throws IOException {
        XContentBuilder builder = builderModifier.apply(XContentFactory.jsonBuilder());
        return serialize(builder, content, params);
    }

    /**
     * A helper that allows modifying the JSON {@link XContentBuilder} before it is used.
     *
     * @param builderModifier The function to modify the
     * @param content The content to serialize.
     * @return The JSON string.
     * @throws IOException When there is an issue with serialization.
     */
    public static String serializeAsJson(UnaryOperator<XContentBuilder> builderModifier, ToXContent content) throws IOException {
        return serializeAsJson(builderModifier, content, ToXContent.EMPTY_PARAMS);
    }
}
