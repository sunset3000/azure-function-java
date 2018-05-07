# SignalFx Java Azure Function Wrapper

SignalFx Java Azure Function Wrapper.

## Supported Languages

* Java 7+

## Usage

The SignalFx Java Azure Function Wrapper is a wrapper around an Azure Function, used to instrument execution of the function and send metrics to SignalFx.

### Install via maven dependency
```xml
<dependency>
  <groupId>com.signalfx.public</groupId>
  <artifactId>signalfx-azure-functions</artifactId>
  <version>0.0.1</version>
</dependency>
```


### Using the Metric Wrapper

Wrap the code inside the handler as followed:
```java
@FunctionName("Hello-SignalFx")
public HttpResponseMessage<String> hello(HttpRequestMessage<Optional<String>> request,
                final ExecutionContext context) {
        MetricWrapper wrapper = new MetricWrapper(context);
            // your code
        } catch (Exception e) {
          wrapper.error();
        } finally {
          wrapper.close();
        }
}
```

### Environment Variable
Set the Azure Function environment variables as follows:

1) Set authentication token:
```
 SIGNALFX_AUTH_TOKEN=signalfx token
```
2) Optional parameters available:
```
 SIGNALFX_API_HOSTNAME=[pops.signalfx.com]
 SIGNALFX_API_PORT=[443]
 SIGNALFX_API_SCHEME=[https]
 SIGNALFX_SEND_TIMEOUT=milliseconds for signalfx client timeout [2000]
```

### Metrics and dimensions sent by the wrapper

The Azure Function wrapper sends the following metrics to SignalFx:

| Metric Name  | Type | Description |
| ------------- | ------------- | ---|
| azure.function.invocations  | Counter  | Count number of function invocations|
| azure.function.errors  | Counter  | Count number of errors from underlying function|
| azure.function.duration  | Gauge  | Milliseconds in execution time of underlying function|

The function wrapper adds the following dimensions to all data points sent to SignalFx:

| Dimension | Description |
| ------------- | ---|
| azure_region  | Azure region where the function is executed  |
| azure_function_name  | Name of the function |
| azure_resource_name  | Name of the function app where the function is running |
| function_wrapper_version  | SignalFx function wrapper qualifier (e.g. signalfx-azurefunction-0.0.11) |
| is_Azure_Function  | Used to differentiate between Azure App Service and Azure Function metrics |
| metric_source | The literal value of 'azure_function_wrapper' |

### Sending a custom metric from the Azure function
```java
// construct data point builder
SignalFxProtocolBuffers.DataPoint.Builder builder =
        SignalFxProtocolBuffers.DataPoint.newBuilder()
                .setMetric("my.custom.metric")
                .setMetricType(SignalFxProtocolBuffers.MetricType.GAUGE)
                .setValue(
                        SignalFxProtocolBuffers.Datum.newBuilder()
                                .setDoubleValue(100));

// add custom dimension
builder.addDimensionsBuilder().setKey("applicationName").setValue("CoolApp").build();

// send the metric
MetricSender.sendMetric(builder);
```

### Testing locally.
1) Follow the Azure instructions to run functions locally https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-java-maven

2) Install as shown above by adding the dependency to pom.xml


## License

Apache Software License v2. Copyright © 2014-2017 SignalFx
