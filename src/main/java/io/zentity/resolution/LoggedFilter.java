package io.zentity.resolution;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class LoggedFilter {
    // resolvers
    // resolver_name => { attributes: string[] }
    @JsonProperty("resolvers")
    @JsonSerialize(using = ResolverAttributesSerializer.class)
    public Map<String, Collection<String>> resolverAttributes;
    // tree
    @JsonProperty("tree")
    public Map<Integer, FilterTree> groupedTree;

    public static class ResolverAttributesSerializer extends StdSerializer<Map<String, Collection<String>>> {
        public ResolverAttributesSerializer() {
            this(null);
        }

        public ResolverAttributesSerializer(Class<Map<String, Collection<String>>> typeClass) {
            super(typeClass);
        }

        @Override
        public void serialize(Map<String, Collection<String>> value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            // nest the collection under each resolver into an "attributes" key
            final Map<String, Map<String, Collection<String>>> finalMap = new TreeMap<>();
            value.forEach((key, attributes) -> {
                Map<String, Collection<String>> nested = new TreeMap<>();
                nested.put("attributes", attributes);
                finalMap.put(key, nested);
            });
            gen.writeObject(finalMap);
        }
    }
}
