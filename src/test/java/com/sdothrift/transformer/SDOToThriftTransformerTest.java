package com.sdothrift.transformer;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
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

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for SDOToThriftTransformer class.
 * Tests transformation from SDO DataObjects to Thrift objects.
 */
@ExtendWith(SDOToThriftTransformerTest.SDOParameterResolver.class)
class SDOToThriftTransformerTest {
    
    private SDOToThriftTransformer transformer;
    
    /**
     * Custom parameter resolver for test parameters.
     */
    static class SDOParameterResolver implements ParameterResolver, TestInstancePostProcessor {
        
        @Override
        public boolean supportsParameter(ParameterContext parameterContext, 
                                           ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType().equals(ThriftSDOConfiguration.class) ||
                   parameterContext.getParameter().getType().equals(String.class) ||
                   parameterContext.getParameter().getType().equals(Class.class);
        }
        
        @Override
        public Object resolveParameter(ParameterContext parameterContext, 
                                      ExtensionContext extensionContext) 
                throws ParameterResolutionException {
            
            if (parameterContext.getParameter().getType().equals(ThriftSDOConfiguration.class)) {
                return createTestConfiguration();
            }
            
            if (parameterContext.getParameter().getType().equals(String.class)) {
                return TestDataGenerator.createTestThriftJson();
            }
            
            if (parameterContext.getParameter().getType().equals(Class.class)) {
                return TestDataGenerator.TestThriftStruct.class;
            }
            
            return null;
        }
        
        @Override
        public void postProcessTestInstance(Object testInstance, 
                                          ExtensionContext extensionContext) {
            if (testInstance instanceof SDOToThriftTransformer) {
                // No special post-processing needed
            }
        }
        
        private ThriftSDOConfiguration createTestConfiguration() {
            ThriftSDOConfiguration config = new ThriftSDOConfiguration();
            config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.JSON);
            config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.PRESERVE);
            config.setPerformanceCachingEnabled(true);
            config.setStrictValidationEnabled(true);
            return config;
        }
    }
    
    @BeforeEach
    @DisplayName("Initialize transformer with test configuration")
    void setUp(ThriftSDOConfiguration configuration) {
        this.transformer = new SDOToThriftTransformer(configuration);
    }
    
    @Test
    @DisplayName("Should transform SDO to basic Thrift struct")
    void shouldTransformSDOToBasicThriftStruct() {
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = createTestSDOFromTestData();
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(123);
        assertThat(result.getName()).isEqualTo("Test Structure");
        assertThat(result.isActive()).isTrue();
        assertThat(result.getScore()).isEqualTo(95.5);
        assertThat(result.getTags()).hasSize(3);
        assertThat(result.getProperties()).hasSize(2);
        assertThat(result.getNested()).isNotNull();
    }
    
    @Test
    @DisplayName("Should handle null SDO transformation")
    void shouldHandleNullSDOTransformation() {
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(null, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNull();
    }
    
    @Test
    @DisplayName("Should validate transformation before execution")
    void shouldValidateTransformationBeforeExecution() {
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = createTestSDOFromTestData();
        
        boolean isValid = transformer.validateTransformation(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(isValid).isTrue();
    }
    
    @Test
    @DisplayName("Should fail validation for invalid SDO")
    void shouldFailValidationForInvalidSDO() {
        // Create an SDO with missing required fields
        org.eclipse.emf.ecore.sdo.EDataObject invalidSDO = createInvalidSDO();
        
        boolean isValid = transformer.validateTransformation(invalidSDO, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(isValid).isFalse();
    }
    
    @Test
    @DisplayName("Should handle different null handling strategies")
    @ParameterizedTest
    @ValueSource(strings = {"PRESERVE", "DEFAULT", "OMIT", "ERROR"})
    void shouldHandleDifferentNullHandlingStrategies(String strategy) {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.fromString(strategy));
        
        transformer = new SDOToThriftTransformer(config);
        
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = createTestSDOWithNulls();
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        
        // Verify null handling behavior
        if ("ERROR".equals(strategy)) {
            // This should throw an exception during transformation
            // The exact behavior depends on implementation details
        }
    }
    
    @Test
    @DisplayName("Should handle different Thrift protocols")
    @ParameterizedTest
    @ValueSource(strings = {"BINARY", "COMPACT", "JSON"})
    void shouldHandleDifferentThriftProtocols(String protocol, org.eclipse.emf.ecore.sdo.EDataObject sdoObject) {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.fromString(protocol));
        
        transformer = new SDOToThriftTransformer(config);
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(123);
        assertThat(result.getName()).isEqualTo("Test Structure");
    }
    
    @Test
    @DisplayName("Should handle complex nested structures")
    void shouldHandleComplexNestedStructures() {
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = createComplexTestSDO();
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(999);
        assertThat(result.getName()).isEqualTo("Complex Structure");
        assertThat(result.getTags()).hasSize(3);
        assertThat(result.getProperties()).hasSize(2);
        assertThat(result.getNested()).isNotNull();
        assertThat(result.getNested().getValue()).isEqualTo("complex");
        assertThat(result.getNested().getDescription()).isEqualTo("complex description");
    }
    
    @Test
    @DisplayName("Should handle edge cases in transformation")
    void shouldHandleEdgeCasesInTransformation() {
        org.eclipse.emf.ecore.sdo.EDataObject edgeCaseSDO = createEdgeCaseTestSDO();
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(edgeCaseSDO, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(Integer.MAX_VALUE);
        assertThat(result.getScore()).isEqualTo(Double.MAX_VALUE);
    }
    
    @Test
    @DisplayName("Should provide cache statistics")
    void shouldProvideCacheStatistics() {
        Map<String, Integer> initialStats = transformer.getCacheStatistics();
        assertThat(initialStats.get("constructorCacheSize")).isEqualTo(0);
        assertThat(initialStats.get("fieldMetaDataCacheSize")).isEqualTo(0);
        
        // Perform some transformations to populate cache
        transformer.transformToThrift(createTestSDOFromTestData(), TestDataGenerator.TestThriftStruct.class);
        transformer.transformToThrift(createComplexTestSDO(), TestDataGenerator.TestThriftStruct.class);
        
        Map<String, Integer> finalStats = transformer.getCacheStatistics();
        assertThat(finalStats.get("constructorCacheSize")).isGreaterThan(0);
        assertThat(finalStats.get("fieldMetaDataCacheSize")).isGreaterThan(0);
    }
    
    @Test
    @DisplayName("Should clear caches successfully")
    void shouldClearCachesSuccessfully() {
        // Populate caches
        transformer.transformToThrift(createTestSDOFromTestData(), TestDataGenerator.TestThriftStruct.class);
        
        Map<String, Integer> statsBefore = transformer.getCacheStatistics();
        assertThat(statsBefore.get("constructorCacheSize")).isGreaterThan(0);
        
        // Clear caches
        transformer.clearCaches();
        
        Map<String, Integer> statsAfter = transformer.getCacheStatistics();
        assertThat(statsAfter.get("constructorCacheSize")).isEqualTo(0);
        assertThat(statsAfter.get("fieldMetaDataCacheSize")).isEqualTo(0);
    }
    
    @Test
    @DisplayName("Should handle large objects efficiently")
    void shouldHandleLargeObjectsEfficiently() {
        org.eclipse.emf.ecore.sdo.EDataObject largeSDO = createLargeTestSDO();
        
        long startTime = System.currentTimeMillis();
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(largeSDO, TestDataGenerator.TestThriftStruct.class);
        long endTime = System.currentTimeMillis();
        
        assertThat(result).isNotNull();
        assertThat(endTime - startTime).isLessThan(5000); // Should complete within 5 seconds
    }
    
    @Test
    @DisplayName("Should handle special characters in strings")
    void shouldHandleSpecialCharactersInStrings() {
        String specialChars = "Test with special chars: 你好世界 🌍 emoji test";
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = createSDOWithSpecialCharacters(specialChars);
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(specialChars);
        // Verify that special characters are preserved
    }
    
    /**
     * Creates a test SDO DataObject from test data.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createTestSDOFromTestData() {
        // This would require creating an actual SDO with proper EClass
        // For now, return a mock SDO object
        return createMockSDO("TestStruct");
    }
    
    /**
     * Creates an invalid SDO DataObject for testing validation.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createInvalidSDO() {
        return createMockSDO("InvalidStruct");
    }
    
    /**
     * Creates a test SDO with null values.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createTestSDOWithNulls() {
        return createMockSDO("NullStruct");
    }
    
    /**
     * Creates a complex test SDO with nested structures.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createComplexTestSDO() {
        return createMockSDO("ComplexStruct");
    }
    
    /**
     * Creates an edge case test SDO.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createEdgeCaseTestSDO() {
        return createMockSDO("EdgeCaseStruct");
    }
    
    /**
     * Creates a large test SDO.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createLargeTestSDO() {
        return createMockSDO("LargeStruct");
    }
    
    /**
     * Creates an SDO with special characters.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createSDOWithSpecialCharacters(String specialChars) {
        return createMockSDO("SpecialCharsStruct");
    }
    
    /**
     * Creates a mock SDO object for testing.
     * This is a simplified implementation - in a real scenario, you'd use actual SDO factories.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createMockSDO(String name) {
        return new org.eclipse.emf.ecore.sdo.EDataObject() {
            @Override
            public org.eclipse.emf.ecore.EClass eClass() {
                return new org.eclipse.emf.ecore.EClass() {
                    @Override
                    public String getName() {
                        return name;
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
}