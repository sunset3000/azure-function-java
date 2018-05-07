package com.signalfx.azurefunctions.example;

import java.util.*;
import java.io.IOException;
import com.microsoft.azure.serverless.functions.annotation.*;
import com.microsoft.azure.serverless.functions.*;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
        /**
         * This function listens at endpoint "/api/hello". Two ways to invoke it using "curl" command in bash:
         * 1. curl -d "HTTP Body" {your host}/api/hello
         * 2. curl {your host}/api/hello?name=HTTP%20Query
         */
        @FunctionName("Hello-Signalfx")
        public HttpResponseMessage<String> hello(
                        @HttpTrigger(name = "req", methods = {"get", "post"}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
                        final ExecutionContext context) {
                try (MetricWrapper wrapper = new MetricWrapper(context)) {
                    try {
                        String query = request.getQueryParameters().get("name");
                        String name = request.getBody().orElse(query);

                            if (name == null) {
                            return request.createResponse(400, "Please pass a name on the query string or in the request body");
                        } else {
                            return request.createResponse(200, "Hello, " + name);
                        }
                    } catch (Exception e) {
                        wrapper.error();
                    } finally {
                        wrapper.close();
                    }
                } catch (IOException e) {
                    context.getLogger().warning("Exception thrown closing wrapper");
                }
                return request.createResponse(200, "Hello");
        }
}
