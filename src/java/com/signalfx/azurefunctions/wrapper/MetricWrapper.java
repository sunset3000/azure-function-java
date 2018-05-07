/*
 * Copyright (C) 2018 SignalFx, Inc.
 */
package com.signalfx.azurefunctions.wrapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.microsoft.azure.serverless.functions.ExecutionContext;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.signalfx.metrics.auth.StaticAuthToken;
import com.signalfx.metrics.connection.HttpDataPointProtobufReceiverFactory;
import com.signalfx.metrics.connection.HttpEventProtobufReceiverFactory;
import com.signalfx.metrics.errorhandler.OnSendErrorHandler;
import com.signalfx.metrics.flush.AggregateMetricSender;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers;

/**
 * @author park
 * @author mstumpfx
 */
public class MetricWrapper implements Closeable {

    protected static final String AUTH_TOKEN = "SIGNALFX_AUTH_TOKEN";
    private static final String TIMEOUT_MS = "SIGNALFX_SEND_TIMEOUT";

    // metric names
    protected static final String METRIC_NAME_PREFIX = "azure.function.";
    protected static final String METRIC_NAME_INVOCATIONS = METRIC_NAME_PREFIX + "invocations";
    protected static final String METRIC_NAME_ERRORS = METRIC_NAME_PREFIX + "errors";
    protected static final String METRIC_NAME_DURATION = METRIC_NAME_PREFIX + "duration";
    public static final Joiner JOINER = Joiner.on(":");

    private final AggregateMetricSender.Session session;

    private final List<SignalFxProtocolBuffers.Dimension> defaultDimensions;

    private final long startTime;

    public MetricWrapper(ExecutionContext context) {
        this(context, null);
    }

    public MetricWrapper(ExecutionContext context,
                         List<SignalFxProtocolBuffers.Dimension> dimensions) {
        this(context, dimensions, System.getenv(AUTH_TOKEN));
    }

    public MetricWrapper(ExecutionContext context,
                         List<SignalFxProtocolBuffers.Dimension> dimensions,
                         String authToken
                         ) {
        int timeoutMs = 300; // default timeout to 300ms
        try {
            timeoutMs = Integer.valueOf(System.getenv(TIMEOUT_MS));
        } catch (NumberFormatException e) {
            // use default
        }

        // Create endpoint for ingest URL
        SignalFxAzureFuncEndpoint signalFxEndpoint = new SignalFxAzureFuncEndpoint();

        // Create datapoint dataPointReceiverFactory for endpoint
        HttpDataPointProtobufReceiverFactory dataPointReceiverFactory = new HttpDataPointProtobufReceiverFactory(signalFxEndpoint)
                .setVersion(2);

        HttpEventProtobufReceiverFactory eventReceiverFactory = new HttpEventProtobufReceiverFactory(
                signalFxEndpoint);

        if (timeoutMs > -1) {
            dataPointReceiverFactory.setTimeoutMs(timeoutMs);
            eventReceiverFactory.setTimeoutMs(timeoutMs);
        }

        AggregateMetricSender metricSender = new AggregateMetricSender("",
                dataPointReceiverFactory,
                eventReceiverFactory,
                new StaticAuthToken(authToken),
                Collections.<OnSendErrorHandler> singleton(metricError -> {
                    context.getLogger().info("Metric sending error");
                }));
        session = metricSender.createSession();
        this.defaultDimensions = getDefaultDimensions(context).entrySet().stream().map(
                e -> getDimensionAsProtoBuf(e.getKey(), e.getValue())
        ).collect(Collectors.toList());

        if (dimensions != null) {
            this.defaultDimensions.addAll(dimensions);
        }

        MetricSender.setWrapper(this);

        startTime = System.nanoTime();
        sendMetricCounter(METRIC_NAME_INVOCATIONS, SignalFxProtocolBuffers.MetricType.COUNTER);
    }

    private static String getWrapperVersionString() {
        try {
            Properties properties = new Properties();
            InputStream resourceAsStream = MetricWrapper.class
                    .getResourceAsStream("/signalfx_wrapper.properties");
            if (resourceAsStream == null) {
                // should not happen, resource could not be found
                return null;
            }
            properties.load(resourceAsStream);
            return properties.getProperty("artifactId") + "-" + properties.getProperty("version");
        } catch (IOException e) {
            return null;
        }
    }

    private static Map<String, String> getDefaultDimensions(ExecutionContext context) {
        Map<String, String> defaultDimensions = new HashMap<>();

        String function_name = context.getFunctionName();
        String resource_name = System.getenv("WEBSITE_SITE_NAME");
        String resource_name_secondary = System.getenv("APP_POOL_ID");
        String region = getRegionName(System.getenv("REGION_NAME"));

        if (!Strings.isNullOrEmpty(region)) {
            defaultDimensions.put("azure_region", region);
        } else {
            context.getLogger().warning("region undefined");
            defaultDimensions.put("azure_region", "undefined");
        }

        if (!Strings.isNullOrEmpty(function_name)) {
            defaultDimensions.put("azure_function_name", function_name);
        } else {
            context.getLogger().warning("function name undefined");
            defaultDimensions.put("azure_function_name", "undefined");
        }


        if (!Strings.isNullOrEmpty(resource_name)) {
            defaultDimensions.put("azure_resource_name", resource_name);
        } else if (!Strings.isNullOrEmpty(resource_name_secondary)) {
            defaultDimensions.put("azure_resource_name", resource_name_secondary);
        } else{
            context.getLogger().warning("azure resource name undefined");
            defaultDimensions.put("azure_resource_name", "undefined");
        }

        String wrapperVersion = getWrapperVersionString();
        if (!Strings.isNullOrEmpty(wrapperVersion)) {
            defaultDimensions.put("function_wrapper_version", wrapperVersion);
        } else {
            defaultDimensions.put("function_wrapper_version", "undefined");
        }

        defaultDimensions.put("is_Azure_Function", "true");
        defaultDimensions.put("metric_source", "azure_function_wrapper");
        return defaultDimensions;
    }

    private static String getRegionName(String region) {
        if (region != null) {
            switch (region) {
                case "East US 2": return "eastus2";
                case "West US 2": return "westus2";
                case "South Central US": return "southcentralus";
                case "West Central US": return "westcentralus";
                case "East US": return "eastus";
                case "North Central US": return "northcentralus";
                case "North Europe": return "northeurope";
                case "Canada East": return "canadaeast";
                case "Central US": return "centralus";
                case "West US": return "westus";
                case "West Europe": return "westeurope";
                case "Central India": return "centralindia";
                case "Southeast Asia": return "southeastasia";
                case "Canada Central": return "canadacentral";
                case "Korea Central": return "koreacentral";
                case "France Central": return "francecentral";
                case "South India": return "southindia";
                case "Australia East": return "australiaeast";
                case "Australia Southeast": return "australiasoutheast";
                case "Japan West": return "japanwest";
                case "UK West": return "ukwest";
                case "UK South": return "uksouth";
                case "Japan East": return "japaneast";
                case "East Asia": return "eastasia";
                case "Brazil South": return "brazilsouth";
                default: return null;
            }
        }
        return null;
    }


    private static SignalFxProtocolBuffers.Dimension getDimensionAsProtoBuf(String key, String value){
        return SignalFxProtocolBuffers.Dimension.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();
    }

    private void sendMetric(String metricName, SignalFxProtocolBuffers.MetricType metricType,
                            SignalFxProtocolBuffers.Datum datum) {
        SignalFxProtocolBuffers.DataPoint.Builder builder =
                SignalFxProtocolBuffers.DataPoint.newBuilder()
                        .setMetric(metricName)
                        .setMetricType(metricType)
                        .setValue(datum);
        MetricSender.sendMetric(builder);
    }

    private void sendMetricCounter(String metricName,
                                   SignalFxProtocolBuffers.MetricType metricType) {
        sendMetric(metricName, metricType,
                SignalFxProtocolBuffers.Datum.newBuilder().setIntValue(1).build());
    }

    protected void sendMetric(SignalFxProtocolBuffers.DataPoint.Builder builder) {
        builder.addAllDimensions(defaultDimensions);
        session.setDatapoint(builder.build());
    }

    public void error() {
        sendMetricCounter(METRIC_NAME_ERRORS, SignalFxProtocolBuffers.MetricType.COUNTER);
    }

    @Override
    public void close() throws IOException {
        sendMetric(METRIC_NAME_DURATION, SignalFxProtocolBuffers.MetricType.GAUGE,
                SignalFxProtocolBuffers.Datum.newBuilder()
                    .setDoubleValue((System.nanoTime() - startTime) / 1000000f)
                    .build()
        );
        session.close();
    }
}
