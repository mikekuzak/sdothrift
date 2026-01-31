package com.sdothrift;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
import com.sdothrift.util.TestFailureAnalyzer;
import commonj.connector.runtime.DataHandlerException;
import org.apache.thrift.TBase;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
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
            config.setThriftProtocol(ThiftSDOConfiguration.ThriftProtocol.JSON);
            config.setNullHandlingStrategy(ThiftSDOConfiguration.NullHandlingStrategy.PRESERVE);
            config.setPerformanceCachingEnabled(true);
            config.setStrictValidationEnabled(true);
            config.setBufferSize(4096);
            return config;
        }
    }
    
    @BeforeEach
    @DisplayName("Initialize data handler with test configuration")
    void setUp(ThiftSDOConfiguration config) {
        this.configuration = config != null ? createTestConfiguration() : config;
        this.dataHandler = new ThriftSDODataHandler(this.configuration);
        
        // Set binding context
        Map<String, Object> bindingContext = new HashMap<>();
        bindingContext.put("thrift.protocol", this.configuration.getThriftProtocol().getProtocolName());
        bindingContext.put("null.handling.strategy", this.configuration.getNullHandlingStrategy().getStrategyName());
        bindingContext.put("performance.caching.enabled", this.configuration.isPerformanceCachingEnabled());
        bindingContext.put("strict.validation.enabled", this.configuration.isStrictValidationEnabled());
        
        this.dataHandler.setBindingContext(bindingContext);
    }
    
    @Test
    @DisplayName("Should transform Thrift object to SDO")
    void shouldTransformThriftObjectToSDO(TestDataGenerator.TestThriftStruct thriftStruct) {
        try {
            Object result = dataHandler.transform(thriftStruct, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
        } catch (DataHandlerException e) {
            fail("Transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should transform SDO object to Thrift")
    void shouldTransformSDOObjectToThrift(EDataObject sdoObject) {
        try {
            Object result = dataHandler.transform(sdoObject, TestDataGenerator.TestThriftStruct.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(TestDataGenerator.TestThriftStruct.class);
            
            TestDataGenerator.TestThriftStruct thriftResult = (TestDataGenerator.TestThriftStruct) result;
            assertThat(thriftResult.getId()).isEqualTo(123);
            assertThat(thriftResult.getName()).isEqualTo("Test Structure");
        } catch (DataHandlerException e) {
            fail("Transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should transform JSON string to Thrift")
    void shouldTransformJsonStringToThrift(String jsonInput) {
        try {
            Object result = dataHandler.transform(jsonInput, TestDataGenerator.TestThriftStruct.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(TestDataGenerator.TestThriftStruct.class);
        } catch (DataHandlerException e) {
            fail("JSON transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should transform Thrift JSON to SDO")
    void shouldTransformThriftJsonToSDO() {
        try {
            Object result = dataHandler.transform(TestDataGenerator.createTestThriftJson(), EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
        } catch (DataHandlerException e) {
            fail("Thrift JSON to SDO transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle InputStream input correctly")
    void shouldHandleInputStreamInput() throws IOException {
        String testContent = TestDataGenerator.createTestThriftJson();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(testContent.getBytes());
        
        try {
            Object result = dataHandler.transform(inputStream, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
        } catch (DataHandlerException e) {
            fail("InputStream transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle byte array input correctly")
    void shouldHandleByteArrayInput() {
        String testContent = TestDataGenerator.createTestThriftJson();
        byte[] byteArray = testContent.getBytes();
        
        try {
            Object result = dataHandler.transform(byteArray, EDataObject.class, null);
            
            assertThat(result).isNotNull();
            assertThat(result).isInstanceOf(EDataObject.class);
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
        } catch (DataHandlerException e) {
            fail("Reader transformation should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle null input according to configuration")
    void shouldHandleNullInputAccordingToConfiguration() {
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
    void shouldHandleNullInputWithPreserveStrategy() {
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
    void shouldHandleNullInputWithDefaultStrategy() {
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
    void shouldHandleTransformationOptions() {
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
    void shouldUpdateConfigurationFromBindingContext() {
        Map<String, Object> newBindingContext = new HashMap<>();
        newBindingContext.put("thrift.protocol", "COMPACT");
        newBindingContext.put("null.handling.strategy", "OMIT");
        newBindingContext.put("performance.caching.enabled", false);
        
        dataHandler.setBindingContext(newBindingContext);
        
        ThriftSDOConfiguration updatedConfig = dataHandler.getConfiguration();
        assertThat(updatedConfig.getThriftProtocol()).isEqualTo(ThriftSDOConfiguration.ThriftProtocol.COMPACT);
        assertThat(updatedConfig.getNullHandlingStrategy()).isEqualTo(ThiftSDOConfiguration.NullHandlingStrategy.OMIT);
        assertThat(updatedConfig.isPerformanceCachingEnabled()).isFalse();
    }
    
    @Test
    @DisplayName("Should handle transformInto correctly")
    void shouldHandleTransformIntoCorrectly() {
        EDataObject sourceSDO = createTestSDOObject();
        TestDataGenerator.TestThriftStruct targetThrift = new TestDataGenerator.TestThriftStruct();
        
        try {
            dataHandler.transformInto(sourceSDO, targetThrift, null);
            
            // For now, just verify no exception is thrown
            // In a real implementation, targetThrift should be populated
        } catch (DataHandlerException e) {
            fail("transformInto should not throw DataHandlerException: " + e.getMessage(), e);
        }
    }
    
    @Test
    @DisplayName("Should handle different null handling strategies in transformInto")
    void shouldHandleDifferentNullHandlingStrategiesInTransformInto() {
        ThriftSDOConfiguration errorConfig = new ThriftSDOConfiguration();
        errorConfig.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.ERROR);
        
        dataHandler = new ThriftSDODataHandler(errorConfig);
        Map<String, Object> bindingContext = new HashMap<>();
        dataHandler.setBindingContext(bindingContext);
        
        EDataObject nullSDO = createNullSDOObject();
        TestDataGenerator.TestThriftStruct targetThrift = new TestDataGenerator.TestThriftStruct();
        
        try {
            dataHandler.transformInto(nullSDO, targetThrift, null);
            fail("Should throw DataHandlerException for null SDO with ERROR strategy in transformInto");
        } catch (DataHandlerException e) {
            assertThat(e.getMessage()).contains("Null input encountered");
        }
    }
    
    @Test
    @DisplayName("Should handle incompatible transformation scenarios")
    void shouldHandleIncompatibleTransformationScenarios() {
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
    void shouldProvideConfigurationAccess() {
        ThriftSDOConfiguration config = dataHandler.getConfiguration();
        assertThat(config).isNotNull();
        assertThat(config.getThriftProtocol()).isEqualTo(configuration.getThriftProtocol());
        assertThat(config.getNullHandlingStrategy()).isEqualTo(configuration.getNullHandlingStrategy());
        assertThat(config.isPerformanceCachingEnabled()).isEqualTo(configuration.isPerformanceCachingEnabled());
    }
    
    @Test
    @DisplayName("Should provide binding context access")
    void shouldProvideBindingContextAccess() {
        Map<String, Object> context = dataHandler.getBindingContext();
        assertThat(context).isNotNull();
        assertThat(context).containsKey("thrift.protocol");
        assertThat(context).containsKey("null.handling.strategy");
    }
    
    @Test
    @DisplayName("Should clear caches successfully")
    void shouldClearCachesSuccessfully() {
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
    void shouldValidateConfiguration() {
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
    void shouldHandlePerformanceTests() {
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
    void shouldHandleEdgeCases() {
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
     * Creates a test SDO DataObject for testing.
     */
    private EDataObject createTestSDOObject() {
        // This would require creating an actual SDO with proper EClass
        // For now, return a mock SDO object
        return new EDataObject() {
            @Override
            public org.eclipse.emf.ecore.EClass eClass() {
                return new org.eclipse.emf.ecore.EClass() {
                    @Override
                    public String getName() {
                        return "TestStruct";
                    }
                    
                    @Override
                    public org.eclipse.emf.ecore.EPackage getEPackage() {
                        return new org.eclipse.emf.ecore.EPackage() {
                            @Override
                            public String getName() {
                                return "test.package";
                            }
                        };
                    }
                };
            }
            
            @Override
            public Object eGet(org.eclipse.emf.ecore.EStructuralFeature feature) {
                // Return test data based on feature name
                switch (feature.getName()) {
                    case "id":
                        return 123;
                    case "name":
                        return "Test Structure";
                    case "active":
                        return true;
                    case "score":
                        return 95.5;
                    case "tags":
                        return java.util.List.of("tag1", "tag2", "tag3");
                    case "properties":
                        return java.util.Map.of("key1", "value1", "key2", "value2");
                    case "nested":
                        return new TestDataGenerator.TestNestedStruct("nested_value", "nested_description");
                    default:
                        return null;
                }
            }
            
            @Override
            public void eSet(org.eclipse.emf.ecore.EStructuralFeature feature, Object newValue) {
                // Mock implementation - would set internal state
            }
            
            @Override
            public boolean eIsSet(org.eclipse.emf.ecore.EStructuralFeature feature) {
                // Mock implementation - return true for most features
                return !"nested".equals(feature.getName());
            }
        };
    }
    
    /**
     * Creates a null SDO DataObject for testing.
     */
    private EDataObject createNullSDOObject() {
        return new EDataObject() {
            @Override
            public org.eclipse.emf.ecore.EClass eClass() {
                return new org.eclipse.emf.ecore.EClass() {
                    @Override
                    public String getName() {
                        return "NullStruct";
                    }
                    
                    @Override
                    public org.eclipse.emf.ecore.EPackage getEPackage() {
                        return new org.eclipse.emf.ecore.EPackage() {
                            @Override
                            public String getName() {
                                return "test.package";
                            }
                        };
                    }
                };
            }
            
            @Override
            public Object eGet(org.eclipse.emf.ecore.EStructuralFeature feature) {
                return null; // All fields are null
            }
            
            @Override
            public void eSet(org.eclipse.emf.ecore.EStructuralFeature feature, Object newValue) {
                // Mock implementation
            }
            
            @Override
            public boolean eIsSet(org.eclipse.emf.ecore.EStructuralFeature feature) {
                return false; // No fields are set
            }
        };
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
    void shouldHandleVariousNullHandlingStrategies(String strategy, Object input, Class<?> targetClass) {
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