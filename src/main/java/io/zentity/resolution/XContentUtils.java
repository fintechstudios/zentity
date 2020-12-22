package io.zentity.resolution;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class XContentUtils {
    /**
     * A simple helper for serializing JSON.
     *
     * @param content The query to serialize.
     * @param params The xcontent params.
     * @return The JSON string.
     * @throws IOException When there is an issue with serialization.
     */
    public static String serializeAsJSON(ToXContent content, ToXContent.Params params) throws IOException {
        return Strings.toString(content.toXContent(XContentFactory.jsonBuilder(), params));
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
}
