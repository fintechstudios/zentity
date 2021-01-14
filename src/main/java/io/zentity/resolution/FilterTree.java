package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A recursive {@link TreeMap} for filtering resolvers.
 */
@JsonSerialize(using = FilterTree.Serializer.class)
public class FilterTree extends TreeMap<String, FilterTree> {
    // Needed to get around reflection access for recursive types
    public static class Serializer extends StdSerializer<FilterTree> {
        public Serializer() {
            this(null);
        }

        protected Serializer(Class<FilterTree> t) {
            super(t);
        }

        @Override
        public void serialize(FilterTree value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            if (value.isEmpty()) {
                gen.writeRawValue("{}");
            } else {
                gen.writeStartObject();
                for (Map.Entry<String, FilterTree> entry : value.entrySet()) {
                    gen.writeObjectField(entry.getKey(), entry.getValue());
                }
                gen.writeEndObject();
            }
        }
    }
}
