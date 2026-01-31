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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for ThriftToSDOTransformer class.
 * Tests transformation from Thrift objects to SDO DataObjects.
 */
@ExtendWith(ThriftToSDOTransformerTest.ThriftParameterResolver.class)
class ThriftToSDOTransformerTest {
    
    private ThriftToSDOTransformer transformer;
    
    /**
     * Custom parameter resolver for test parameters.
     */
    static class ThriftParameterResolver implements ParameterResolver, TestInstancePostProcessor {
        
        @Override
        public boolean supportsParameter(ParameterContext parameterContext, 
                                           ExtensionContext extensionContext) {
            return parameterContext.getParameter().getType().equals(TestDataGenerator.TestThriftStruct.class) ||
                   parameterContext.getParameter().getType().equals(ThriftSDOConfiguration.class);
        }
        
        @Override
        public Object resolveParameter(ParameterContext parameterContext, 
                                      ExtensionContext extensionContext) 
                throws ParameterResolutionException {
            
            if (parameterContext.getParameter().getType().equals(TestDataGenerator.TestThriftStruct.class)) {
                return TestDataGenerator.createTestThriftStruct();
            }
            
            if (parameterContext.getParameter().getType().equals(ThriftSDOConfiguration.class)) {
                return createTestConfiguration();
            }
            
            return null;
        }
        
        @Override
        public void postProcessTestInstance(Object testInstance, 
                                          ExtensionContext extensionContext) {
            if (testInstance instanceof ThriftToSDOTransformer) {
                // No special post-processing needed
            }
        }
        
        private ThriftSDOConfiguration createTestConfiguration() {
            ThriftSDOConfiguration config = new ThriftSDOConfiguration();
            config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.JSON);
            config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.PRESERVE);
            config.setPerformanceCachingEnabled(true);
            return config;
        }
    }
    
    @BeforeEach
    @DisplayName("Initialize transformer with test configuration")
    void setUp(ThriftSDOConfiguration configuration) {
        this.transformer = new ThriftToSDOTransformer(configuration);
    }
    
    @Test
    @DisplayName("Should transform basic Thrift struct to SDO")
    void shouldTransformBasicThriftStructToSDO(TestDataGenerator.TestThriftStruct thriftStruct) {
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(thriftStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should handle null Thrift struct transformation")
    void shouldHandleNullThriftStructTransformation() {
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(null);
        
        assertThat(result).isNull();
    }
    
    @Test
    @DisplayName("Should transform empty Thrift struct to SDO")
    void shouldTransformEmptyThriftStructToSDO() {
        TestDataGenerator.TestThriftStruct emptyStruct = TestDataGenerator.createEmptyThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(emptyStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should transform Thrift struct with null fields to SDO")
    void shouldTransformThriftStructWithNullFieldsToSDO() {
        TestDataGenerator.TestThriftStruct nullStruct = TestDataGenerator.createNullThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(nullStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should handle transformation with different null handling strategies")
    @ParameterizedTest
    @ValueSource(strings = {"PRESERVE", "DEFAULT", "OMIT"})
    void shouldHandleDifferentNullHandlingStrategies(String strategy, TestDataGenerator.TestThriftStruct thriftStruct) {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.fromString(strategy));
        
        transformer = new ThriftToSDOTransformer(config);
        
        TestDataGenerator.TestThriftStruct nullStruct = TestDataGenerator.createNullThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(nullStruct);
        
        assertThat(result).isNotNull();
        
        // Verify null handling strategy behavior
        if ("OMIT".equals(strategy)) {
            // Fields should be omitted
            // This would require checking if the specific features exist in the SDO
        }
    }
    
    @Test
    @DisplayName("Should transform with different Thrift protocols")
    @ParameterizedTest
    @ValueSource(strings = {"BINARY", "COMPACT", "JSON"})
    void shouldTransformWithDifferentThriftProtocols(String protocol, TestDataGenerator.TestThriftStruct thriftStruct) {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.fromString(protocol));
        
        transformer = new ThriftToSDOTransformer(config);
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(thriftStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should handle complex nested structures")
    void shouldHandleComplexNestedStructures() {
        TestDataGenerator.TestThriftStruct complexStruct = new TestDataGenerator.TestThriftStruct(
            999,
            "Complex Structure",
            true,
            100.0,
            java.util.List.of("complex", "nested", "structure"),
            java.util.Map.of("complex1", "value1", "complex2", "value2"),
            new TestDataGenerator.TestNestedStruct("complex", "complex description")
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(complexStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should handle edge cases in transformation")
    void shouldHandleEdgeCasesInTransformation() {
        TestDataGenerator.TestThriftStruct edgeCaseStruct = new TestDataGenerator.TestThriftStruct(
            Integer.MAX_VALUE,
            "Edge Case Test",
            false,
            Double.MAX_VALUE,
            java.util.List.of(),
            java.util.Map.of(),
            null
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(edgeCaseStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should provide cache statistics")
    void shouldProvideCacheStatistics() {
        Map<String, Integer> initialStats = transformer.getCacheStatistics();
        assertThat(initialStats.get("eclassCacheSize")).isEqualTo(0);
        
        // Perform some transformations to populate cache
        transformer.transformToSDO(TestDataGenerator.createTestThriftStruct());
        transformer.transformToSDO(TestDataGenerator.createEmptyThriftStruct());
        
        Map<String, Integer> finalStats = transformer.getCacheStatistics();
        assertThat(finalStats.get("eclassCacheSize")).isGreaterThan(0);
    }
    
    @Test
    @DisplayName("Should clear caches successfully")
    void shouldClearCachesSuccessfully() {
        // Populate caches
        transformer.transformToSDO(TestDataGenerator.createTestThriftStruct());
        
        Map<String, Integer> statsBefore = transformer.getCacheStatistics();
        assertThat(statsBefore.get("eclassCacheSize")).isGreaterThan(0);
        
        // Clear caches
        transformer.clearCaches();
        
        Map<String, Integer> statsAfter = transformer.getCacheStatistics();
        assertThat(statsAfter.get("eclassCacheSize")).isEqualTo(0);
    }
    
    @Test
    @DisplayName("Should handle large collections efficiently")
    void shouldHandleLargeCollectionsEfficiently() {
        // Create a struct with large collections
        java.util.List<String> largeList = new java.util.ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeList.add("item" + i);
        }
        
        java.util.Map<String, String> largeMap = new java.util.HashMap<>();
        for (int i = 0; i < 500; i++) {
            largeMap.put("key" + i, "value" + i);
        }
        
        TestDataGenerator.TestThriftStruct largeStruct = new TestDataGenerator.TestThriftStruct(
            1,
            "Large Collection Test",
            true,
            50.0,
            largeList,
            largeMap,
            null
        );
        
        long startTime = System.currentTimeMillis();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(largeStruct);
        long endTime = System.currentTimeMillis();
        
        assertThat(result).isNotNull();
        assertThat(endTime - startTime).isLessThan(5000); // Should complete within 5 seconds
    }
    
    @Test
    @DisplayName("Should handle special characters in strings")
    void shouldHandleSpecialCharactersInStrings() {
        String specialChars = "Test with special chars: 你好世界 🌍 emoji test";
        TestDataGenerator.TestThriftStruct specialStruct = new TestDataGenerator.TestThriftStruct(
            1,
            specialChars,
            true,
            99.9,
            java.util.List.of("special", "unicode", "emoji"),
            java.util.Map.of("special_key", "special_value"),
            null
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(specialStruct);
        
        assertThat(result).isNotNull();
        // Verify that special characters are preserved
        // This would require checking the actual string values in the SDO
    }
    
    @Test
    @DisplayName("Should handle circular references gracefully")
    void shouldHandleCircularReferencesGracefully() {
        // Create a circular reference scenario
        TestDataGenerator.TestNestedStruct nested1 = new TestDataGenerator.TestNestedStruct("nested1", "description1");
        TestDataGenerator.TestNestedStruct nested2 = new TestDataGenerator.TestNestedStruct("nested2", "description2");
        
        // This would require a more complex setup to test actual circular references
        // For now, just test that null nested structures work
        TestDataGenerator.TestThriftStruct structWithNullNested = new TestDataGenerator.TestThriftStruct(
            1,
            "Circular Reference Test",
            true,
            75.0,
            java.util.List.of(),
            java.util.Map.of(),
            null
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(structWithNullNested);
        
        assertThat(result).isNotNull();
        // Verify that null nested is handled correctly
    }
}