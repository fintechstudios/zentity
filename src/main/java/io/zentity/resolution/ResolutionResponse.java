package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentParseException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@JsonSerialize(using = ResolutionResponse.Serializer.class)
public class ResolutionResponse {
    // took, in ms
    public Duration took;
    // into { total: number, hits }
    public List<JsonNode> hits = new ArrayList<>();
    // TODO: move to where the response is needed
    public boolean includeHits = true;
    // query
    // TODO: move to where the response is needed
    public boolean includeQueries = true;
    public List<LoggedQuery> queries = new ArrayList<>();
    // error, perhaps w/ stack trace
    public Throwable error;
    // TODO: move to where the response is needed
    public boolean includeStackTrace = true;

    public boolean isFailure() {
        return this.error != null;
    }

    public static class Serializer extends StdSerializer<ResolutionResponse> {
        private static final LoggedQuery.Serializer QUERY_SERIALIZER = new LoggedQuery.Serializer();

        public Serializer() {
            this(null);
        }

        public Serializer(Class<ResolutionResponse> typeClass) {
            super(typeClass);
        }

        static boolean isEsException(Throwable ex) {
            return ex instanceof ElasticsearchException || ex instanceof XContentParseException;
        }

        static void serializeException(Throwable ex, boolean includeStackTrace, JsonGenerator gen) throws IOException {
            gen.writeStartObject();

            if (isEsException(ex)) {
                gen.writeStringField("by", "elasticsearch");
            } else {
                gen.writeStringField("by", "zentity");
            }

            gen.writeStringField("type", ex.getClass().getCanonicalName());
            gen.writeStringField("reason", ex.getMessage());

            if (includeStackTrace) {
                StringWriter traceWriter = new StringWriter();
                ex.printStackTrace(new PrintWriter(traceWriter));
                gen.writeStringField("stack_trace", traceWriter.toString());
            }

            gen.writeEndObject();
        }

        @Override
        public void serialize(ResolutionResponse value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            // encode 'took' in ms
            gen.writeFieldName("took");
            gen.writeNumber(value.took.toMillis());
            if (value.includeHits) {
                // encode hits with the total
                gen.writeObjectFieldStart("hits");

                gen.writeFieldName("total");
                gen.writeNumber(value.hits.size());

                gen.writeFieldName("hits");
                gen.writeObject(value.hits);

                gen.writeEndObject();
            }
            if (value.includeQueries && !value.queries.isEmpty()) {
                gen.writeArrayFieldStart("queries");
                // hack to get around reflection issues
                for (LoggedQuery query : value.queries) {
                    QUERY_SERIALIZER.serialize(query, gen, provider);
                }
                gen.writeEndArray();
            }
            if (value.error != null) {
                gen.writeFieldName("error");
                serializeException(value.error, value.includeStackTrace, gen);
            }

            gen.writeEndObject();
        }
    }
}
