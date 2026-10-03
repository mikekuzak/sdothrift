# SDO Thrift Data Handler

An IBM DataHandler implementation for transforming Apache Thrift objects (libthrift 0.21.0) and SDO DataObjects.

## IBM reference

This project implements a custom data handler for IBM Business Automation Workflow (BAW) and IBM Integration Designer, following the contract in [IBM's custom data handler documentation](https://www.ibm.com/docs/en/baw/26.0.x?topic=registries-creating-custom-data-handler). The handler implements `commonj.connector.runtime.DataHandler` (`transform`, `transformInto`, and `setBindingContext`) and is registered as `com.sdothrift.ThriftSDODataHandler`.

Apache Thrift reference: [github.com/apache/thrift](https://github.com/apache/thrift).

## Scope

The implementation supports bidirectional transformation for the primitive Thrift types, enums, binary strings, UUIDs, lists, sets, maps, and structs listed below. It does not support every Thrift type or every Thrift/SDO deployment scenario; see [Known limitations](#known-limitations).

The handler's textual JSON boundary uses ordinary JSON field names. Separately, the serializer byte API (`serializeToBytes` / `deserializeFromBytes`) uses the configured Thrift wire protocol: BINARY, COMPACT, or JSON. SIMPLE_JSON is write-only; attempting to read it fails explicitly.

For handler JSON-to-SDO conversion, a Thrift schema class must be supplied in `options` or in the binding context under `thrift.target.class`. Without it, the handler throws `CONFIGURATION_ERROR`.

## Requirements and dependencies

- Java source and target level: 1.8.
- Apache Thrift (`libthrift`): 0.21.0.
- EMF: `org.eclipse.emf.ecore` 2.23.0, `commonj-sdo` 2.1.0, `ecore-sdo` 2.1.1, and `ecore-change` 2.1.0.
- Other runtime dependencies: `jackson-databind` 2.15.2, `commons-lang3` 3.12.0, `commons-collections4` 4.4, and `slf4j-api` 2.0.7. `logback-classic` is used for tests.
- `jars/soacore_apis.jar` is configured with Maven `system` scope. It supplies IBM's `commonj.connector.runtime.DataHandler`, `BindingContext`, and `DataHandlerException`, plus `com.ibm.websphere.bo.*`. It is available to compile and test, but Maven does not package it in the artifact. This IBM jar is proprietary and is **not** committed to or redistributed with this repository (the `jars/` directory is git-ignored); supply it locally, or from your internal Maven repository, before building.

## Type mapping

| Thrift type | SDO representation | Thrift reconstruction / notes |
|-------------|--------------------|-------------------------------|
| `bool` | `Boolean` | `Boolean` |
| `byte` | `Byte` | `Byte` |
| `i16` | `Short` | `Short` |
| `i32` | `Integer` | `Integer` |
| `i64` | `Long` | `Long` |
| `double` | `Double` | `Double` |
| `string` | `String` | `String` |
| `binary` / `string` (binary annotation) | Base64 `String` | Base64-decodes to `byte[]` |
| `uuid` | `String` | `UUID.fromString` |
| `list<T>` | Many-valued SDO property (`java.util.List` semantics) | List; duplicates are preserved |
| `set<T>` | Many-valued UNIQUE SDO property, represented as a list | `java.util.Set` |
| `map<K,V>` | Containment list of entry DataObjects, each with `key` and `value` features | `java.util.Map`; non-string keys are rejected |
| `struct` | Nested SDO `DataObject` | Nested struct |
| `union` | Nested SDO `DataObject` | Maps generically as a struct; no one-of validation |
| `enum` | `Integer` (`TEnum.getValue()`) | Generated `findByValue(int)` |

## Configuration

The handler recognizes these binding-context keys:

| Key | Status |
|-----|--------|
| `thrift.protocol` | Consumed by transformation logic |
| `null.handling.strategy` | Consumed by transformation logic |
| `collection.type.preferences` | Read into configuration only; not yet consumed |
| `performance.caching.enabled` | Read into configuration only; not yet consumed |
| `performance.cache.size` | Read into configuration only; not yet consumed |
| `debug.logging.enabled` | Read into configuration only; not yet consumed |
| `buffer.size` | Consumed by transformation logic |
| `character.encoding` | Consumed by transformation logic |
| `strict.validation.enabled` | Consumed by transformation logic |

Example:

```java
Map<String, Object> context = new HashMap<>();
context.put("thrift.protocol", "BINARY");
context.put("null.handling.strategy", "PRESERVE");
context.put("buffer.size", 8192);
context.put("character.encoding", "UTF-8");
context.put("strict.validation.enabled", true);
dataHandler.setBindingContext(context);
```

## Testing

Run the unit tests with:

```bash
mvn clean test
```

The verified offline run, `mvn -o clean test`, completed with **116 tests, 0 failures, 0 errors**, and `BUILD SUCCESS`. The test suites are `ThriftSDODataHandlerTest`, `ThriftToSDOTransformerTest`, `SDOToThriftTransformerTest`, `TypeMapperTest`, and `ThriftSerializerTest`. There are no integration tests or performance tests. Coverage has not been measured; a configured JaCoCo plugin is not evidence of a coverage percentage.

## IBM Integration Designer / BAW deployment

The IBM API jar is a local, system-scoped compile/test dependency and is not bundled in the project artifact. The actual IBM runtime must provide the IBM APIs when deploying the handler.

1. Build the artifact with `mvn clean package`. The recorded successful verification is `mvn -o clean test`; packaging and deployment have not been verified in an IBM runtime.
2. Copy the resulting project artifact to the IBM Integration Designer/BAW runtime's library location.
3. Register `com.sdothrift.ThriftSDODataHandler` as a custom data handler using the product's data-handler configuration.
4. Configure the binding properties required by the application.

The deployment steps describe the intended integration procedure; this project has not been validated inside an IBM BAW or Integration Designer runtime.

## Project structure

```text
src/main/java/com/sdothrift/
├── ThriftSDODataHandler.java
├── config/ThriftSDOConfiguration.java
├── exception/ThriftSDODataHandlerException.java
├── serializer/ThriftSerializer.java
├── transformer/
│   ├── SDOToThriftTransformer.java
│   ├── ThriftToSDOTransformer.java
│   └── TypeMapper.java
└── util/Java8CompatibilityUtils.java

src/test/java/com/sdothrift/
├── ThriftSDODataHandlerTest.java
├── serializer/
│   └── ThriftSerializerTest.java
├── transformer/
│   ├── SDOToThriftTransformerTest.java
│   ├── ThriftToSDOTransformerTest.java
│   └── TypeMapperTest.java
└── util/
    ├── TestDataGenerator.java
    ├── TestFailureAnalyzer.java
    ├── TestSDOFixtures.java
    ├── BasicTestRunner.java
    └── SimpleTestRunner.java
```

`BasicTestRunner`, `SimpleTestRunner`, and `TestFailureAnalyzer` are standalone main-method utilities, not JUnit tests. There is no integration-test package.

## Performance

No performance benchmarks have been measured or published.

## Verification status

Verification is unit-level only. The project has not been validated inside an IBM BAW or Integration Designer runtime.

## Known limitations

- Thrift unions map generically as structs, with no one-of validation.
- `collection.type.preferences`, `performance.caching.enabled`, `performance.cache.size`, and `debug.logging.enabled` are not yet wired into transformation behavior.
- `jars/soacore_apis.jar` is system-scoped and is not packaged into the artifact. Deployment requires the IBM runtime to supply its IBM APIs.
- No performance benchmark or coverage percentage is available.
