package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.zentity.common.FunctionalUtil.UnCheckedUnaryOperator;
import io.zentity.common.XContentUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

@JsonSerialize(using = LoggedSearch.Serializer.class)
public class LoggedSearch {
    // request
    public SearchRequestBuilder searchRequest;
    // response, if non-null
    public SearchResponse response;
    // response error, if no response
    public Throwable responseError;

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

        static void serializeException(Throwable ex, JsonGenerator gen) throws IOException {
            // see: https://github.com/zentity-io/zentity/commit/44b908c89ea32386eb086b3cf86aaa8b05aa0b07#diff-f0157aff5b741656786bef437bb41ce170eaba293f8ba212ced5e1f4315ae6a8R1239
            gen.writeStartObject();

            gen.writeStringField("reason", ex.getMessage());

            if (ex instanceof XContentParseException) {
                XContentParseException pEx = (XContentParseException) ex;
                gen.writeStringField("type", "parsing_exception");
                gen.writeNumberField("line", pEx.getLineNumber());
                gen.writeNumberField("col", pEx.getColumnNumber());
                gen.writeNumberField("status", RestStatus.BAD_REQUEST.getStatus());

                gen.writeArrayFieldStart("root_cause");
                gen.writeStartObject();
                gen.writeStringField("type", "parsing_exception");
                gen.writeNumberField("line", pEx.getLineNumber());
                gen.writeNumberField("col", pEx.getColumnNumber());
                gen.writeNumberField("status", RestStatus.BAD_REQUEST.getStatus());
                gen.writeStringField("reason", pEx.getMessage());
                gen.writeEndObject();
                gen.writeEndArray();
            } else if (ex instanceof ElasticsearchException) {
                final ElasticsearchException esEx = (ElasticsearchException) ex;
                String type = ElasticsearchException.getExceptionName(esEx);
                gen.writeStringField("type", type);
                gen.writeNumberField("status", esEx.status().getStatus());

                gen.writeArrayFieldStart("root_cause");
                String rawCause = XContentUtil.serialize(
                    XContentUtil.jsonBuilder(
                        UnCheckedUnaryOperator.from((builder) -> {
                            builder.startObject();
                            esEx.toXContent(builder, ToXContent.EMPTY_PARAMS);
                            builder.endObject();
                            return builder;
                        })
                    )
                );
                gen.writeRawValue(rawCause);
                gen.writeEndArray();
            } else {
                String type = ex.getClass().getCanonicalName();
                gen.writeStringField("type", type);
                gen.writeNumberField("status", RestStatus.INTERNAL_SERVER_ERROR.getStatus());

                gen.writeArrayFieldStart("root_cause");
                gen.writeStartObject();
                gen.writeStringField("type", type);
                gen.writeNumberField("status", RestStatus.INTERNAL_SERVER_ERROR.getStatus());
                gen.writeStringField("reason", ex.getMessage());
                gen.writeEndObject();
                gen.writeEndArray();
            }

            gen.writeEndObject();
        }

        @Override
        public void serialize(LoggedSearch value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();

            gen.writeFieldName("request");
            gen.writeRawValue(XContentUtil.serializeAsJSON(value.searchRequest.request().source()));

            // write the response, either the error or the real response
            gen.writeFieldName("response");
            if (value.response == null) {
                // serialize the error
                gen.writeStartObject();

                gen.writeFieldName("error");
                serializeException(value.responseError, gen);

                gen.writeEndObject();
            } else {
                gen.writeRawValue(XContentUtil.serializeAsJSON(value.response));
            }

            gen.writeEndObject();
        }
    }
}
