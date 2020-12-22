package io.zentity.resolution;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@JsonSerialize(using = LoggedSearch.Serializer.class)
public class LoggedSearch {
    // request
    QueryBuilder searchRequest;
    // response, if non-null
    SearchResponse response;
    // response
    ElasticsearchException responseError;

    // perhaps this belongs better as a global custom StdSerializer<ElasticsearchException> than a class
    @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
    public static class SerializedElasticsearchException {
        @JsonRawValue
        public String rootCause;

        public String type;

        public String reason;

        public int status;

        public SerializedElasticsearchException(ElasticsearchException ex) throws IOException {
            rootCause = "[" + Strings.toString(ex.toXContent(jsonBuilder().startObject(), ToXContent.EMPTY_PARAMS).endObject()) + "]";
            type = ElasticsearchException.getExceptionName(ex);
            reason = ex.getMessage();
            status = ex.status().getStatus();
        }
    }

    // need to output { "request": {}, "response": {}}
    // where response is either the response or an error
    // where error is { "error": { root_cause: esRawJson, type:ElasticsearchException.getExceptionName(e), reason: e.message, status: e.status().getStatus()
    public static class Serializer extends StdSerializer<LoggedSearch> {
        public Serializer() {
            this(null);
        }

        public Serializer(Class<LoggedSearch> typeClass) {
            super(typeClass);
        }

        @Override
        public void serialize(LoggedSearch value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("request");
            gen.writeRawValue(value.searchRequest.toString());
            // write the response, either the error or the real response
            gen.writeFieldName("response");
            if (value.response == null) {
                // serialize the error
                SerializedElasticsearchException serializedEx = new SerializedElasticsearchException(value.responseError);
                gen.writeStartObject();
                gen.writeFieldName("error");
                gen.writeObject(serializedEx);
                gen.writeEndObject();
            } else {
                gen.writeRawValue(value.response.toString());
            }
            gen.writeEndObject();
        }
    }
}
