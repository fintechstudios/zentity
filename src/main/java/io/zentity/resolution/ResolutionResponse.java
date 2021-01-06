package io.zentity.resolution;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
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

    @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
    public static class SerializedException {
        public String by;
        public String type;
        public String reason;

        static boolean isEsException(Throwable ex) {
            return ex instanceof ElasticsearchException || ex instanceof XContentParseException;
        }

        @JsonInclude(Include.NON_NULL)
        public String stackTrace = null;

        public SerializedException(Throwable ex, boolean includeStackTrace) {
            by = isEsException(ex) ? "elasticsearch": "zentity";
            type = ex.getClass().getCanonicalName();
            // TODO: support more info from parse exceptions
            //       see: https://github.com/zentity-io/zentity/commit/44b908c89ea32386eb086b3cf86aaa8b05aa0b07#diff-f0157aff5b741656786bef437bb41ce170eaba293f8ba212ced5e1f4315ae6a8R1239
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
                gen.writeNumber(value.hits.size());

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
