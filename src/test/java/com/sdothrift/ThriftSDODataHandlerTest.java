package com.sdothrift;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
import com.sdothrift.util.TestSDOFixtures;
import commonj.connector.runtime.DataHandlerException;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for ThriftSDODataHandler class.
 * Tests the main IBM DataHandler implementation following the delegation pattern.
 */
@ExtendWith(ThriftSDODataHandlerTest.ConfigurationParameterResolver.class)
class ThriftSDODataHandlerTest {
    
    private ThriftSDODataHandler dataHandler;
    private ThriftSDOConfiguration configuration;
    
    /**
     * Custom parameter resolver for test parameters.
     * Resolves ONLY ThriftSDOConfiguration; JUnit owns every other parameter.
     */
    static class ConfigurationParameterResolver implements ParameterResolver, TestInstancePostProcessor {
        
        @Override
        public boolean supportsParameter(ParameterContext parameterContext, 
                                           ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType().equals(ThriftSDOConfiguration.class);
        }
        
        @Override
        public Object resolveParameter(ParameterContext parameterContext, 
                                      ExtensionContext extensionContext) 
                throws ParameterResolutionException {
            
            if (parameterContext.getParameter().getType().equals(ThriftSDOConfiguration.class)) {
                return createTestConfiguration();
            }
            
            return null;
        }
        
        @Override
        public void postProcessTestInstance(Object testInstance, 
                                          ExtensionContext extensionContext) {
            if (testInstance instanceof ThriftSDODataHandler) {
                ThriftSDODataHandler handler = (ThriftSDODataHandler) testInstance;
                handler.validateConfiguration();
            }
        }
        
        private ThriftSDOConfiguration createTestConfiguration() {
            ThriftSDOConfiguration config = new ThriftSDOConfiguration();
            config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.JSON);
            config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.PRESERVE);
            config.setPerformanceCachingEnabled(true);
            config.setStrictValidationEnabled(true);
            config.setBufferSize(4096);
            return config;
        }
    }
    
    @BeforeEach
    @DisplayName("Initialize data handler with test configuration")
    void setUp(ThriftSDOConfiguration config) {
        this.configuration = config != null ? config : createTestConfiguration();
        this.dataHandler = new ThriftSDODataHandler(this.configuration);
        
        // Set binding context
        Map<String, Object> bindingContext = new HashMap<>();
        bindingContext.put("thrift.protocol", this.configuration.getThriftProtocol().getProtocolName());
        bindingContext.put("null.handling.strategy", this.configuration.getNullHandlingStrategy().getStrategyName());
        bindingContext.put("performance.caching.enabled", this.configuration.isPerformanceCachingEnabled());
        bindingContext.put("strict.validation.enabled", this.configuration.isStrictValidationEnabled());
        // Untyped JSON cannot identify a Java Thrift class: positive JSON->SDO tests
        // supply the schema class through the binding context.
        bindingContext.put("thrift.target.class", TestDataGenerator.TestThriftStruct.class);
        
        this.dataHandler.setBindingContext(bindingContext);
    }
    
    private ThriftSDOConfiguration createTestConfiguration() {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.BINARY);
        config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.PRESERVE);
        config.setPerformanceCachingEnabled(true);
        config.setStrictValidationEnabled(false);
        return config;
    }
    
    @Test
    @DisplayName("Should transform Thrift object to SDO")
    void shouldTransformThriftObjectToSDO() throws Exception {
        TestDataGenerator.TestThriftStruct thriftStruct = TestDataGenerator.createTestThriftStruct();
        try {
            Object result = dataHandler.transform(thriftStruct, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
            
            // Forward mapping must carry real values, not just succeed.
            EDataObject sdo = (EDataObject) result;
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("id"))).isEqualTo(123);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("name"))).isEqualTo("Test Structure");
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("active"))).isEqualTo(true);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("score"))).isEqualTo(95.5);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("tags")))
                .asList().containsExactly("tag1", "tag2", "tag3");
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("properties"))).asList().hasSize(2);
            EDataObject nested = (EDataObject) sdo.eGet(sdo.eClass().getEStructuralFeature("nested"));
            assertThat(nested.eGet(nested.eClass().getEStructuralFeature("value"))).isEqualTo("nested_value");
            assertThat(nested.eGet(nested.eClass().getEStructuralFeature("description"))).isEqualTo("nested_description");
        } catch (DataHandlerException e) {
            fail("Transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should transform SDO object to Thrift")
    void shouldTransformSDOObjectToThrift() throws Exception {
        EDataObject sdoObject = TestSDOFixtures.basic();
        try {
            Object result = dataHandler.transform(sdoObject, TestDataGenerator.TestThriftStruct.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(TestDataGenerator.TestThriftStruct.class);
            
            TestDataGenerator.TestThriftStruct thriftResult = (TestDataGenerator.TestThriftStruct) result;
            assertThat(thriftResult.getId()).isEqualTo(123);
            assertThat(thriftResult.getName()).isEqualTo("Test Structure");
            assertThat(thriftResult.isActive()).isTrue();
            assertThat(thriftResult.getScore()).isEqualTo(95.5);
        } catch (DataHandlerException e) {
            fail("Transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should transform JSON string to Thrift")
    void shouldTransformJsonStringToThrift() throws Exception {
        String jsonInput = TestDataGenerator.createTestThriftJson();
        try {
            Object result = dataHandler.transform(jsonInput, TestDataGenerator.TestThriftStruct.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(TestDataGenerator.TestThriftStruct.class);
            
            // Field-name JSON must decode to exact values for all seven fields.
            TestDataGenerator.TestThriftStruct thriftResult = (TestDataGenerator.TestThriftStruct) result;
            assertThat(thriftResult.getId()).isEqualTo(123);
            assertThat(thriftResult.getName()).isEqualTo("Test Structure");
            assertThat(thriftResult.isActive()).isTrue();
            assertThat(thriftResult.getScore()).isEqualTo(95.5);
            assertThat(thriftResult.getTags()).containsExactly("tag1", "tag2", "tag3");
            assertThat(thriftResult.getProperties())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
            assertThat(thriftResult.getNested()).isNotNull();
            assertThat(thriftResult.getNested().getValue()).isEqualTo("nested_value");
            assertThat(thriftResult.getNested().getDescription()).isEqualTo("nested_description");
        } catch (DataHandlerException e) {
            fail("JSON transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should transform Thrift JSON to SDO")
    void shouldTransformThriftJsonToSDO() throws Exception {
        try {
            Object result = dataHandler.transform(TestDataGenerator.createTestThriftJson(), EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
            
            // JSON -> Thrift -> SDO must preserve exact values.
            EDataObject sdo = (EDataObject) result;
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("id"))).isEqualTo(123);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("name"))).isEqualTo("Test Structure");
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("active"))).isEqualTo(true);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("score"))).isEqualTo(95.5);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("tags")))
                .asList().containsExactly("tag1", "tag2", "tag3");
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("properties"))).asList().hasSize(2);
            EDataObject nested = (EDataObject) sdo.eGet(sdo.eClass().getEStructuralFeature("nested"));
            assertThat(nested.eGet(nested.eClass().getEStructuralFeature("value"))).isEqualTo("nested_value");
            assertThat(nested.eGet(nested.eClass().getEStructuralFeature("description"))).isEqualTo("nested_description");
        } catch (DataHandlerException e) {
            fail("Thrift JSON to SDO transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should fail JSON to SDO when class metadata is absent")
    void shouldFailJsonToSDOWithoutClassMetadata() throws Exception {
        // A handler whose binding context carries no thrift.target.class must report
        // a configuration error instead of guessing the Thrift class from field names.
        ThriftSDOConfiguration bareConfig = new ThriftSDOConfiguration();
        bareConfig.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.PRESERVE);
        ThriftSDODataHandler bareHandler = new ThriftSDODataHandler(bareConfig);
        bareHandler.setBindingContext(new HashMap<>());
        
        assertThatThrownBy(() -> bareHandler.transform(TestDataGenerator.createTestThriftJson(), EDataObject.class, null))
            .isInstanceOf(DataHandlerException.class)
            .hasMessageContaining("Cannot determine Thrift class");
    }
    
    @Test
    @DisplayName("Should handle InputStream input correctly")
    void shouldHandleInputStreamInput() throws IOException {
        String testContent = TestDataGenerator.createTestThriftJson();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(testContent.getBytes(StandardCharsets.UTF_8));
        
        try {
            Object result = dataHandler.transform(inputStream, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
            
            EDataObject sdo = (EDataObject) result;
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("id"))).isEqualTo(123);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("name"))).isEqualTo("Test Structure");
        } catch (DataHandlerException e) {
            fail("InputStream transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle byte array input correctly")
    void shouldHandleByteArrayInput() throws Exception {
        String testContent = TestDataGenerator.createTestThriftJson();
        byte[] byteArray = testContent.getBytes(StandardCharsets.UTF_8);
        
        try {
            Object result = dataHandler.transform(byteArray, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
            
            EDataObject sdo = (EDataObject) result;
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("id"))).isEqualTo(123);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("name"))).isEqualTo("Test Structure");
        } catch (DataHandlerException e) {
            fail("Byte array transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle Reader input correctly")
    void shouldHandleReaderInput() throws IOException {
        String testContent = TestDataGenerator.createTestThriftJson();
        StringReader reader = new StringReader(testContent);
        
        try {
            Object result = dataHandler.transform(reader, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
            
            EDataObject sdo = (EDataObject) result;
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("id"))).isEqualTo(123);
            assertThat(sdo.eGet(sdo.eClass().getEStructuralFeature("name"))).isEqualTo("Test Structure");
        } catch (DataHandlerException e) {
            fail("Reader transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should reject invalid numeric JSON values")
    void shouldRejectInvalidNumericJson() throws Exception {
        String invalidNumericJson = "{"
            + "\"id\": \"not-a-number\","
            + "\"name\": \"Bad Numeric\","
            + "\"active\": true,"
            + "\"score\": 1.0"
            + "}";
        
        assertThatThrownBy(() -> dataHandler.transform(invalidNumericJson, TestDataGenerator.TestThriftStruct.class, null))
            .isInstanceOf(DataHandlerException.class);
    }
    
    @Test
    @DisplayName("Should handle null input according to configuration")
    void shouldHandleNullInputAccordingToConfiguration() throws Exception {
        // Test with ERROR strategy
        ThriftSDOConfiguration errorConfig = new ThriftSDOConfiguration();
        errorConfig.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.ERROR);
        
        dataHandler = new ThriftSDODataHandler(errorConfig);
        Map<String, Object> bindingContext = new HashMap<>();
        dataHandler.setBindingContext(bindingContext);
        
        try {
            dataHandler.transform(null, TestDataGenerator.TestThriftStruct.class, null);
            fail("Should have thrown DataHandlerException for null input with ERROR strategy");
        } catch (DataHandlerException e) {
            assertThat(e.getMessage()).contains("Null input encountered");
        }
    }
    
    @Test
    @DisplayName("Should handle null input with PRESERVE strategy")
    void shouldHandleNullInputWithPreserveStrategy() throws Exception {
        // Test with PRESERVE strategy
        ThriftSDOConfiguration preserveConfig = new ThriftSDOConfiguration();
        preserveConfig.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.PRESERVE);
        
        dataHandler = new ThriftSDODataHandler(preserveConfig);
        Map<String, Object> bindingContext = new HashMap<>();
        dataHandler.setBindingContext(bindingContext);
        
        try {
            Object result = dataHandler.transform(null, TestDataGenerator.TestThriftStruct.class, null);
            assertThat(result).isNull();
        } catch (DataHandlerException e) {
            fail("Should not throw DataHandlerException for null input with PRESERVE strategy: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle null input with DEFAULT strategy")
    void shouldHandleNullInputWithDefaultStrategy() throws Exception {
        // Test with DEFAULT strategy
        ThriftSDOConfiguration defaultConfig = new ThriftSDOConfiguration();
        defaultConfig.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.DEFAULT);
        
        dataHandler = new ThriftSDODataHandler(defaultConfig);
        Map<String, Object> bindingContext = new HashMap<>();
        dataHandler.setBindingContext(bindingContext);
        
        try {
            Object result = dataHandler.transform(null, TestDataGenerator.TestThriftStruct.class, null);
            // Should return a struct with default values
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(TestDataGenerator.TestThriftStruct.class);
        } catch (DataHandlerException e) {
            fail("Should not throw DataHandlerException for null input with DEFAULT strategy: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle transformation options")
    void shouldHandleTransformationOptions() throws Exception {
        try {
            Map<String, Object> options = new HashMap<>();
            options.put("test.option", "test.value");
            
            Object result = dataHandler.transform(TestDataGenerator.createTestThriftStruct(), EDataObject.class, options);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
        } catch (DataHandlerException e) {
            fail("Transformation with options should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should update configuration from binding context")
    void shouldUpdateConfigurationFromBindingContext() throws Exception {
        Map<String, Object> newBindingContext = new HashMap<>();
        newBindingContext.put("thrift.protocol", "COMPACT");
        newBindingContext.put("null.handling.strategy", "OMIT");
        newBindingContext.put("performance.caching.enabled", false);
        
        dataHandler.setBindingContext(newBindingContext);
        
        ThriftSDOConfiguration updatedConfig = dataHandler.getConfiguration();
        assertThat(updatedConfig.getThriftProtocol()).isEqualTo(ThriftSDOConfiguration.ThriftProtocol.COMPACT);
        assertThat(updatedConfig.getNullHandlingStrategy()).isEqualTo(ThriftSDOConfiguration.NullHandlingStrategy.OMIT);
        assertThat(updatedConfig.isPerformanceCachingEnabled()).isFalse();
    }
    
    @Test
    @DisplayName("Should handle transformInto correctly")
    void shouldHandleTransformIntoCorrectly() throws Exception {
        EDataObject sourceSDO = TestSDOFixtures.basic();
        TestDataGenerator.TestThriftStruct targetThrift = new TestDataGenerator.TestThriftStruct();
        
        try {
            dataHandler.transformInto(sourceSDO, targetThrift, null);
            
            // transformInto must actually populate the target via Thrift metadata.
            assertThat(targetThrift.getId()).isEqualTo(123);
            assertThat(targetThrift.getName()).isEqualTo("Test Structure");
            assertThat(targetThrift.isActive()).isTrue();
            assertThat(targetThrift.getScore()).isEqualTo(95.5);
            assertThat(targetThrift.getTags()).containsExactly("tag1", "tag2", "tag3");
            assertThat(targetThrift.getProperties())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
            assertThat(targetThrift.getNested()).isNotNull();
            assertThat(targetThrift.getNested().getValue()).isEqualTo("nested_value");
            assertThat(targetThrift.getNested().getDescription()).isEqualTo("nested_description");
        } catch (DataHandlerException e) {
            fail("transformInto should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle different null handling strategies in transformInto")
    void shouldHandleDifferentNullHandlingStrategiesInTransformInto() throws Exception {
        ThriftSDOConfiguration errorConfig = new ThriftSDOConfiguration();
        errorConfig.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.ERROR);
        
        dataHandler = new ThriftSDODataHandler(errorConfig);
        Map<String, Object> bindingContext = new HashMap<>();
        dataHandler.setBindingContext(bindingContext);
        
        // A real schema whose fields are all unset (not a featureless mock).
        EDataObject nullSDO = TestSDOFixtures.allUnset();
        TestDataGenerator.TestThriftStruct targetThrift = new TestDataGenerator.TestThriftStruct();
        
        try {
            dataHandler.transformInto(nullSDO, targetThrift, null);
            fail("Should throw DataHandlerException for null SDO with ERROR strategy in transformInto");
        } catch (DataHandlerException e) {
            // The field-null ERROR domain message must survive the transformer and
            // handler wrapping layers. The specific field depends on metadata
            // iteration order, so only the message prefix is pinned.
            assertThat(e).getRootCause().hasMessageContaining("Null value encountered for field:");
        }
    }
    
    @Test
    @DisplayName("Should handle incompatible transformation scenarios")
    void shouldHandleIncompatibleTransformationScenarios() throws Exception {
        try {
            // Try to transform incompatible types
            dataHandler.transform(new Object(), String.class, null);
            fail("Should throw DataHandlerException for incompatible transformation");
        } catch (DataHandlerException e) {
            assertThat(e.getMessage()).contains("Unsupported transformation");
        }
    }
    
    @Test
    @DisplayName("Should provide configuration access")
    void shouldProvideConfigurationAccess() throws Exception {
        ThriftSDOConfiguration config = dataHandler.getConfiguration();
        assertThat(config).isNotNull();
        assertThat(config.getThriftProtocol()).isEqualTo(configuration.getThriftProtocol());
        assertThat(config.getNullHandlingStrategy()).isEqualTo(configuration.getNullHandlingStrategy());
        assertThat(config.isPerformanceCachingEnabled()).isEqualTo(configuration.isPerformanceCachingEnabled());
    }
    
    @Test
    @DisplayName("Should provide binding context access")
    void shouldProvideBindingContextAccess() throws Exception {
        Map<String, Object> context = dataHandler.getBindingContext();
        assertThat(context).isNotNull();
        assertThat(context).containsKey("thrift.protocol");
        assertThat(context).containsKey("null.handling.strategy");
    }
    
    @Test
    @DisplayName("Should clear caches successfully")
    void shouldClearCachesSuccessfully() throws Exception {
        // Perform some operations to populate caches
        dataHandler.transform(TestDataGenerator.createTestThriftStruct(), EDataObject.class, null);
        
        Map<String, Integer> statsBefore = dataHandler.getCacheStatistics();
        assertThat(statsBefore).isNotEmpty();
        
        // Clear caches
        dataHandler.clearCaches();
        
        Map<String, Integer> statsAfter = dataHandler.getCacheStatistics();
        assertThat(statsAfter.get("constructorCacheSize")).isEqualTo(0);
        assertThat(statsAfter.get("fieldMetaDataCacheSize")).isEqualTo(0);
    }
    
    @Test
    @DisplayName("Should validate configuration")
    void shouldValidateConfiguration() throws Exception {
        // This should not throw any exceptions
        assertThatCode(() -> dataHandler.validateConfiguration()).doesNotThrowAnyException();
        
        // Test with invalid configuration
        ThriftSDOConfiguration invalidConfig = new ThriftSDOConfiguration();
        invalidConfig.setMaxCacheSize(0); // Invalid - below minimum
        
        dataHandler = new ThriftSDODataHandler(invalidConfig);
        
        assertThatThrownBy(() -> dataHandler.validateConfiguration())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxCacheSize must be at least 1");
    }
    
    @Test
    @DisplayName("Should handle performance tests")
    void shouldHandlePerformanceTests() throws Exception {
        TestDataGenerator.TestThriftStruct largeStruct = createLargeTestStruct();
        
        long startTime = System.currentTimeMillis();
        
        try {
            dataHandler.transform(largeStruct, EDataObject.class, null);
            long endTime = System.currentTimeMillis();
            
            assertThat(endTime - startTime).isLessThan(10000); // Should complete within 10 seconds
        } catch (DataHandlerException e) {
            fail("Performance test should not throw exception: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle edge cases")
    void shouldHandleEdgeCases() throws Exception {
        Object[] edgeCases = TestDataGenerator.createEdgeCaseData();
        
        for (Object edgeCase : edgeCases) {
            try {
                dataHandler.transform(edgeCase, String.class, null);
                // Should either succeed or throw a DataHandlerException with meaningful message
            } catch (DataHandlerException e) {
                // Verify the exception is meaningful
                assertThat(e.getMessage()).isNotEmpty();
                assertThat(e.getMessage()).doesNotContain("Unsupported transformation");
            }
        }
    }
    
    /**
     * Creates a large test struct for performance testing.
     */
    private TestDataGenerator.TestThriftStruct createLargeTestStruct() {
        java.util.List<String> largeList = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeList.add("large-item-" + i);
        }
        
        java.util.Map<String, String> largeMap = new java.util.HashMap<>();
        for (int i = 0; i < 500; i++) {
            largeMap.put("large-key-" + i, "large-value-" + i);
        }
        
        return new TestDataGenerator.TestThriftStruct(
            Integer.MAX_VALUE,
            "Large Performance Test",
            true,
            Double.MAX_VALUE,
            largeList,
            largeMap,
            new TestDataGenerator.TestNestedStruct("large-nested", "large nested description")
        );
    }
    
    /**
     * Creates test scenarios with different null handling strategies.
     */
    @ParameterizedTest
    @MethodSource("provideNullHandlingScenarios")
    @DisplayName("Should handle various null handling strategies")
    void shouldHandleVariousNullHandlingStrategies(String strategy, Object input, Class<?> targetClass) throws Exception {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.fromString(strategy));
        
        dataHandler = new ThriftSDODataHandler(config);
        
        try {
            Object result = dataHandler.transform(input, targetClass, null);
            
            if ("ERROR".equals(strategy) && input == null) {
                fail("Should throw DataHandlerException for ERROR strategy with null input");
            } else {
                // Should not throw exception for other strategies
                assertThat(result).isNotNull();
            }
        } catch (DataHandlerException e) {
            if (!("ERROR".equals(strategy))) {
                fail("Should not throw DataHandlerException for " + strategy + " strategy: " + e.getMessage());
            }
        }
    }
    
    private static Stream<Arguments> provideNullHandlingScenarios() {
        return Stream.of(
            Arguments.of("PRESERVE", TestDataGenerator.createTestThriftStruct(), TestDataGenerator.TestThriftStruct.class),
            Arguments.of("DEFAULT", null, TestDataGenerator.TestThriftStruct.class),
            Arguments.of("OMIT", TestDataGenerator.createTestThriftStruct(), TestDataGenerator.TestThriftStruct.class),
            Arguments.of("ERROR", null, TestDataGenerator.TestThriftStruct.class)
        );
    }
}
