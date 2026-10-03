package com.sdothrift.transformer;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
import com.sdothrift.util.TestSDOFixtures;
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
import java.util.Map;

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
    void shouldTransformSDOToBasicThriftStruct() throws Exception {
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
    @DisplayName("Should map SDO enum integer, binary Base64, and UUID string to Thrift")
    void shouldMapEnumBinaryAndUuidFromSDO() throws Exception {
        org.eclipse.emf.ecore.sdo.EDataObject sdo = new ThriftToSDOTransformer(
            new ThriftSDOConfiguration()).transformToSDO(TestDataGenerator.createTestTypesStruct());
        sdo.eSet(sdo.eClass().getEStructuralFeature("color"), Integer.valueOf(2));
        sdo.eSet(sdo.eClass().getEStructuralFeature("data"), "YmluYXJ5AHBheWxvYWQ=");
        sdo.eSet(sdo.eClass().getEStructuralFeature("uuid"), "123e4567-e89b-12d3-a456-426614174000");

        TestDataGenerator.TestTypesStruct result = transformer.transformToThrift(
            sdo, TestDataGenerator.TestTypesStruct.class);

        assertThat(result.getId()).isEqualTo(7);
        assertThat(result.getColor()).isEqualTo(TestDataGenerator.Color.GREEN);
        assertThat(result.getData()).containsExactly("binary\u0000payload".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(result.getUuid()).isEqualTo(java.util.UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
    }
    
    @Test
    @DisplayName("Should handle null SDO transformation")
    void shouldHandleNullSDOTransformation() throws Exception {
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(null, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNull();
    }
    
    @Test
    @DisplayName("Should validate transformation before execution")
    void shouldValidateTransformationBeforeExecution() throws Exception {
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = createTestSDOFromTestData();
        
        boolean isValid = transformer.validateTransformation(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(isValid).isTrue();
        
        // Required fields are matched BY NAME: a fixture with every required field set
        // but every optional field deliberately absent must still validate.
        assertThat(transformer.validateTransformation(TestSDOFixtures.nullPolicy(), TestDataGenerator.TestThriftStruct.class))
            .as("unset optional fields must not fail validation")
            .isTrue();
    }
    
    @Test
    @DisplayName("Should fail validation for invalid SDO")
    void shouldFailValidationForInvalidSDO() throws Exception {
        // Create an SDO with missing required fields
        org.eclipse.emf.ecore.sdo.EDataObject invalidSDO = createInvalidSDO();
        
        boolean isValid = transformer.validateTransformation(invalidSDO, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(isValid).isFalse();
    }
    
    @DisplayName("Should handle different null handling strategies")
    @ParameterizedTest
    @ValueSource(strings = {"PRESERVE", "DEFAULT", "OMIT", "ERROR"})
    void shouldHandleDifferentNullHandlingStrategies(String strategy) throws Exception {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setNullHandlingStrategy(ThriftSDOConfiguration.NullHandlingStrategy.fromString(strategy));
        
        transformer = new SDOToThriftTransformer(config);
        
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = TestSDOFixtures.nullPolicy();

        if ("ERROR".equals(strategy)) {
            // The unset optional field that trips ERROR depends on metadata iteration
            // order, so assert the field-null domain message without pinning the name.
            assertThatThrownBy(() -> transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class))
                .getRootCause()
                .hasMessageContaining("Null value encountered for field:");
            return;
        }

        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(123);
        assertThat(result.getName()).isEqualTo("Test Structure");
        if ("DEFAULT".equals(strategy)) {
            assertThat(result.isSet(TestDataGenerator.TestThriftStruct._Fields.TAGS)).isTrue();
            assertThat(result.getTags()).isEmpty();
            assertThat(result.isSet(TestDataGenerator.TestThriftStruct._Fields.PROPERTIES)).isTrue();
            assertThat(result.getProperties()).isEmpty();
        } else {
            assertThat(result.isSet(TestDataGenerator.TestThriftStruct._Fields.TAGS)).isFalse();
            assertThat(result.isSet(TestDataGenerator.TestThriftStruct._Fields.PROPERTIES)).isFalse();
        }
    }
    
    @DisplayName("Should handle different Thrift protocols")
    @ParameterizedTest
    @ValueSource(strings = {"BINARY", "COMPACT", "JSON"})
    void shouldHandleDifferentThriftProtocols(String protocol) throws Exception {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.fromString(protocol));
        
        transformer = new SDOToThriftTransformer(config);
        
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = TestSDOFixtures.basic();
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(123);
        assertThat(result.getName()).isEqualTo("Test Structure");
        assertThat(result.isActive()).isTrue();
        assertThat(result.getScore()).isEqualTo(95.5);
        assertThat(result.getTags()).containsExactly("tag1", "tag2", "tag3");
        assertThat(result.getProperties()).containsEntry("key1", "value1").containsEntry("key2", "value2");
        assertThat(result.getNested().getValue()).isEqualTo("nested_value");
    }
    
    @Test
    @DisplayName("Should handle complex nested structures")
    void shouldHandleComplexNestedStructures() throws Exception {
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
    void shouldHandleEdgeCasesInTransformation() throws Exception {
        org.eclipse.emf.ecore.sdo.EDataObject edgeCaseSDO = createEdgeCaseTestSDO();
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(edgeCaseSDO, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(Integer.MAX_VALUE);
        assertThat(result.getScore()).isEqualTo(Double.MAX_VALUE);
    }
    
    @Test
    @DisplayName("Should provide cache statistics")
    void shouldProvideCacheStatistics() throws Exception {
        Map<String, Integer> initialStats = transformer.getCacheStatistics();
        assertThat(initialStats.get("constructorCacheSize")).isEqualTo(0);
        assertThat(initialStats.get("fieldMetaDataCacheSize")).isEqualTo(0);
        
        // Populate the two caches independently: constructor lookup is lazy and metadata lookup
        // occurs only when a transformation starts.
        transformer.transformToThrift(TestSDOFixtures.basic(), TestDataGenerator.TestThriftStruct.class);
        transformer.transformToThrift(TestSDOFixtures.complex(), TestDataGenerator.TestThriftStruct.class);
        
        Map<String, Integer> finalStats = transformer.getCacheStatistics();
        assertThat(finalStats.get("constructorCacheSize")).isGreaterThan(0);
        assertThat(finalStats.get("fieldMetaDataCacheSize")).isGreaterThan(0);
    }
    
    @Test
    @DisplayName("Should clear caches successfully")
    void shouldClearCachesSuccessfully() throws Exception {
        // Populate caches
        transformer.transformToThrift(TestSDOFixtures.basic(), TestDataGenerator.TestThriftStruct.class);
        
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
    void shouldHandleLargeObjectsEfficiently() throws Exception {
        org.eclipse.emf.ecore.sdo.EDataObject largeSDO = TestSDOFixtures.large();
        
        long startTime = System.currentTimeMillis();
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(largeSDO, TestDataGenerator.TestThriftStruct.class);
        long endTime = System.currentTimeMillis();
        
        assertThat(result).isNotNull();
        assertThat(endTime - startTime).isLessThan(5000); // Should complete within 5 seconds
    }
    
    @Test
    @DisplayName("Should handle special characters in strings")
    void shouldHandleSpecialCharactersInStrings() throws Exception {
        String specialChars = "Test with special chars: 你好世界 🌍 emoji test";
        org.eclipse.emf.ecore.sdo.EDataObject sdoObject = TestSDOFixtures.specialCharacters(specialChars);
        
        TestDataGenerator.TestThriftStruct result = transformer.transformToThrift(sdoObject, TestDataGenerator.TestThriftStruct.class);
        
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(specialChars);
        // Verify that special characters are preserved
    }
    
    /**
     * Creates a test SDO DataObject from test data.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createTestSDOFromTestData() {
        return TestSDOFixtures.basic();
    }
    
    /**
     * Creates an invalid SDO DataObject for testing validation.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createInvalidSDO() {
        return TestSDOFixtures.invalidMissingRequired();
    }
    
    /**
     * Creates a test SDO with null values.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createTestSDOWithNulls() {
        return TestSDOFixtures.nullPolicy();
    }
    
    /**
     * Creates a complex test SDO with nested structures.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createComplexTestSDO() {
        return TestSDOFixtures.complex();
    }
    
    /**
     * Creates an edge case test SDO.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createEdgeCaseTestSDO() {
        return TestSDOFixtures.edge();
    }
    
    /**
     * Creates a large test SDO.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createLargeTestSDO() {
        return TestSDOFixtures.large();
    }
    
    /**
     * Creates an SDO with special characters.
     */
    private org.eclipse.emf.ecore.sdo.EDataObject createSDOWithSpecialCharacters(String specialChars) {
        return TestSDOFixtures.specialCharacters(specialChars);
    }
}
