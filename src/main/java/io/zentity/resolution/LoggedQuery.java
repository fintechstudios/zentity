package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Map;

@JsonSerialize(using = LoggedQuery.Serializer.class)
public class LoggedQuery {
    // _index
    public String index;
    // _hop
    public int hop;
    // _query
    public int queryNumber;
    // search
    public LoggedSearch search;
    // filters
    public Map<String, LoggedFilter> filters;

    public static class Serializer extends StdSerializer<LoggedQuery> {
        private static final LoggedSearch.Serializer SEARCH_SERIALIZER = new LoggedSearch.Serializer();

        public Serializer() {
            this(null);
        }

        public Serializer(Class<LoggedQuery> t) {
            super(t);
        }

        @Override
        public void serialize(LoggedQuery value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();

            gen.writeStringField("_index", value.index);
            gen.writeNumberField("_hop", value.hop);
            gen.writeNumberField("_query", value.queryNumber);
            gen.writeObjectField("filters", value.filters);

            gen.writeFieldName("search");
            // access directly to avoid reflection
            SEARCH_SERIALIZER.serialize(value.search, gen, provider);

            gen.writeEndObject();
        }
    }
}
