package io.zentity.resolution;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.elasticsearch.ElasticsearchException;

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
    public Exception error;
    // TODO: move to where the response is needed
    public boolean includeStackTrace = true;

    public static class SerializedException {
        public String by;
        public String type;
        public String reason;

        @JsonInclude(Include.NON_NULL)
        public String stackTrace = null;

        public SerializedException(Exception ex, boolean includeStackTrace) {
            by = (ex instanceof ElasticsearchException) ? "elasticsearch": "zentity";
            type = ex.getClass().getCanonicalName();
            reason = ex.getMessage();
            if (includeStackTrace) {
                StringWriter traceWriter = new StringWriter();
                ex.printStackTrace(new PrintWriter(traceWriter));
                stackTrace = traceWriter.toString();
            }
        }
    }

    public static class Serializer extends StdSerializer<ResolutionResponse> {
        public Serializer() {
            this(null);
        }

        public Serializer(Class<ResolutionResponse> typeClass) {
            super(typeClass);
        }

        @Override
        public void serialize(ResolutionResponse value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            // encode 'took' in ms
            gen.writeFieldName("took");
            gen.writeObject(value.took.toMillis());
            if (value.includeHits) {
                // encode hits with the total
                gen.writeObjectFieldStart("hits");

                gen.writeFieldName("total");
                gen.writeObject(value.hits.size());

                gen.writeFieldName("hits");
                gen.writeObject(value.hits);

                gen.writeEndObject();
            }
            if (value.includeQueries && !value.queries.isEmpty()) {
                gen.writeArrayFieldStart("queries");
                gen.writeObject(value.queries);
                gen.writeEndArray();
            }
            if (value.error != null) {
                gen.writeFieldName("error");
                gen.writeObject(new SerializedException(value.error, value.includeStackTrace));
            }

            gen.writeEndObject();
        }
    }
}
