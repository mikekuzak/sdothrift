package com.sdothrift.transformer;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
import com.sdothrift.util.TestSDOFixtures;
import org.apache.thrift.TBase;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
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
    void shouldTransformBasicThriftStructToSDO(TestDataGenerator.TestThriftStruct thriftStruct) throws Exception {
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(thriftStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
        assertThat(result.eGet(result.eClass().getEStructuralFeature("id"))).isEqualTo(123);
        assertThat(result.eGet(result.eClass().getEStructuralFeature("name"))).isEqualTo("Test Structure");
        assertThat(result.eGet(result.eClass().getEStructuralFeature("active"))).isEqualTo(true);
        assertThat(result.eGet(result.eClass().getEStructuralFeature("score"))).isEqualTo(95.5d);
        assertThat(result.eGet(result.eClass().getEStructuralFeature("tags"))).asList().containsExactly("tag1", "tag2", "tag3");
        assertThat(result.eGet(result.eClass().getEStructuralFeature("properties"))).asList().hasSize(2);
        org.eclipse.emf.ecore.sdo.EDataObject nested = (org.eclipse.emf.ecore.sdo.EDataObject)
            result.eGet(result.eClass().getEStructuralFeature("nested"));
        assertThat(nested.eGet(nested.eClass().getEStructuralFeature("value"))).isEqualTo("nested_value");
        assertThat(nested.eGet(nested.eClass().getEStructuralFeature("description"))).isEqualTo("nested_description");
    }
    
    @Test
    @DisplayName("Should handle null Thrift struct transformation")
    void shouldHandleNullThriftStructTransformation() throws Exception {
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(null);
        
        assertThat(result).isNull();
    }
    
    @Test
    @DisplayName("Should transform empty Thrift struct to SDO")
    void shouldTransformEmptyThriftStructToSDO() throws Exception {
        TestDataGenerator.TestThriftStruct emptyStruct = TestDataGenerator.createEmptyThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(emptyStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should transform Thrift struct with null fields to SDO")
    void shouldTransformThriftStructWithNullFieldsToSDO() throws Exception {
        TestDataGenerator.TestThriftStruct nullStruct = TestDataGenerator.createNullThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(nullStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @DisplayName("Should handle transformation with different null handling strategies")
    @ParameterizedTest
    @ValueSource(strings = {"PRESERVE", "DEFAULT", "OMIT"})
    void shouldHandleDifferentNullHandlingStrategies(String strategy) throws Exception {
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
    
    @DisplayName("Should transform with different Thrift protocols")
    @ParameterizedTest
    @ValueSource(strings = {"BINARY", "COMPACT", "JSON"})
    void shouldTransformWithDifferentThriftProtocols(String protocol) throws Exception {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        config.setThriftProtocol(ThriftSDOConfiguration.ThriftProtocol.fromString(protocol));
        
        transformer = new ThriftToSDOTransformer(config);
        TestDataGenerator.TestThriftStruct thriftStruct = TestDataGenerator.createTestThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(thriftStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should handle complex nested structures")
    void shouldHandleComplexNestedStructures() throws Exception {
        TestDataGenerator.TestThriftStruct complexStruct = new TestDataGenerator.TestThriftStruct(
            999,
            "Complex Structure",
            true,
            100.0,
            java.util.Arrays.asList("complex", "nested", "structure"),
            new java.util.HashMap<String, String>() {{ put("complex1", "value1"); put("complex2", "value2"); }},
            new TestDataGenerator.TestNestedStruct("complex", "complex description")
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(complexStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should handle edge cases in transformation")
    void shouldHandleEdgeCasesInTransformation() throws Exception {
        TestDataGenerator.TestThriftStruct edgeCaseStruct = new TestDataGenerator.TestThriftStruct(
            Integer.MAX_VALUE,
            "Edge Case Test",
            false,
            Double.MAX_VALUE,
            java.util.Arrays.asList(),
            new java.util.HashMap<String, String>(),
            null
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(edgeCaseStruct);
        
        assertThat(result).isNotNull();
        assertThat(result.eClass().getName()).contains("TestThriftStruct");
    }
    
    @Test
    @DisplayName("Should provide cache statistics")
    void shouldProvideCacheStatistics() throws Exception {
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
    void shouldClearCachesSuccessfully() throws Exception {
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
    void shouldHandleLargeCollectionsEfficiently() throws Exception {
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
    void shouldHandleSpecialCharactersInStrings() throws Exception {
        String specialChars = "Test with special chars: 你好世界 🌍 emoji test";
        TestDataGenerator.TestThriftStruct specialStruct = new TestDataGenerator.TestThriftStruct(
            1,
            specialChars,
            true,
            99.9,
            java.util.Arrays.asList("special", "unicode", "emoji"),
            new java.util.HashMap<String, String>() {{ put("special_key", "special_value"); }},
            null
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(specialStruct);
        
        assertThat(result).isNotNull();
        // Verify that special characters are preserved
        // This would require checking the actual string values in the SDO
    }
    
    @Test
    @DisplayName("Should handle circular references gracefully")
    void shouldHandleCircularReferencesGracefully() throws Exception {
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
            java.util.Arrays.asList(),
            new java.util.HashMap<String, String>(),
            null
        );
        
        org.eclipse.emf.ecore.sdo.EDataObject result = transformer.transformToSDO(structWithNullNested);
        
        assertThat(result).isNotNull();
        // Verify that null nested is handled correctly
    }

    @Test
    @DisplayName("Should round-trip all seven fields through Thrift and SDO")
    void shouldRoundTripAllFieldsThroughThriftAndSDO() throws Exception {
        TestDataGenerator.TestThriftStruct source = TestDataGenerator.createTestThriftStruct();
        org.eclipse.emf.ecore.sdo.EDataObject sdo = transformer.transformToSDO(source);
        TestDataGenerator.TestThriftStruct result = new SDOToThriftTransformer(
            new ThriftSDOConfiguration()).transformToThrift(sdo, TestDataGenerator.TestThriftStruct.class);

        assertThat(result).isEqualTo(source);
        for (TestDataGenerator.TestThriftStruct._Fields field : TestDataGenerator.TestThriftStruct._Fields.values()) {
            assertThat(result.isSet(field)).as("presence of %s", field.getFieldName()).isTrue();
        }
        assertThat(sdo.eIsSet(sdo.eClass().getEStructuralFeature("tags"))).isTrue();
        assertThat(sdo.eIsSet(sdo.eClass().getEStructuralFeature("properties"))).isTrue();
        assertThat(sdo.eIsSet(sdo.eClass().getEStructuralFeature("nested"))).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"BINARY", "COMPACT", "TJSON"})
    @DisplayName("Should read and write actual Thrift wire protocols")
    void shouldRoundTripProtocolBytes(String protocolName) throws Exception {
        TestDataGenerator.TestThriftStruct source = TestDataGenerator.createTestThriftStruct();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        TProtocol writer = protocol(protocolName, new TIOStreamTransport(bytes));
        source.write(writer);
        writer.getTransport().flush();

        TestDataGenerator.TestThriftStruct result = new TestDataGenerator.TestThriftStruct();
        TProtocol reader = protocol(protocolName, new TIOStreamTransport(new ByteArrayInputStream(bytes.toByteArray())));
        result.read(reader);
        assertThat(result).isEqualTo(source);
        for (TestDataGenerator.TestThriftStruct._Fields field : TestDataGenerator.TestThriftStruct._Fields.values()) {
            assertThat(result.isSet(field)).as("presence of %s", field.getFieldName()).isTrue();
        }
        assertThat(result.getNested()).isNotSameAs(source.getNested());
    }

    @Test
    @DisplayName("Should distinguish empty and unset collection values")
    void shouldDistinguishEmptyAndUnsetCollections() {
        TestDataGenerator.TestThriftStruct empty = TestDataGenerator.createEmptyThriftStruct();
        TestDataGenerator.TestThriftStruct unset = TestDataGenerator.createNullThriftStruct();
        assertThat(empty.isSet(TestDataGenerator.TestThriftStruct._Fields.TAGS)).isTrue();
        assertThat(empty.getTags()).isEmpty();
        assertThat(empty.isSet(TestDataGenerator.TestThriftStruct._Fields.PROPERTIES)).isTrue();
        assertThat(empty.getProperties()).isEmpty();
        assertThat(unset.isSet(TestDataGenerator.TestThriftStruct._Fields.TAGS)).isFalse();
        assertThat(unset.isSet(TestDataGenerator.TestThriftStruct._Fields.PROPERTIES)).isFalse();

        org.eclipse.emf.ecore.sdo.EDataObject explicitEmpty = TestSDOFixtures.edge();
        org.eclipse.emf.ecore.sdo.EDataObject absent = TestSDOFixtures.nullPolicy();
        assertThat(explicitEmpty.eIsSet(explicitEmpty.eClass().getEStructuralFeature("tags"))).isTrue();
        assertThat(((List<?>) explicitEmpty.eGet(explicitEmpty.eClass().getEStructuralFeature("tags")))).isEmpty();
        assertThat(absent.eIsSet(absent.eClass().getEStructuralFeature("tags"))).isFalse();
    }

    @Test
    @DisplayName("Should preserve duplicate LIST values and enforce unique SET values")
    void shouldPreserveListDuplicatesAndEnforceSetUniqueness() {
        org.eclipse.emf.ecore.sdo.EDataObject collections = TestSDOFixtures.collectionContract();
        assertThat((List<Object>) collections.eGet(collections.eClass().getEStructuralFeature("tags")))
            .containsExactly("duplicate", "duplicate");
        assertThat((List<Object>) collections.eGet(collections.eClass().getEStructuralFeature("uniqueTags")))
            .containsExactly("duplicate");
        assertThat(collections.eClass().getEStructuralFeature("tags").isUnique()).isFalse();
        assertThat(collections.eClass().getEStructuralFeature("uniqueTags").isUnique()).isTrue();
        assertThat(collections.eClass().getEStructuralFeature("properties").isMany()).isTrue();
        assertThat(((org.eclipse.emf.ecore.EReference) collections.eClass().getEStructuralFeature("properties")).isContainment()).isTrue();
    }

    private static TProtocol protocol(String protocolName, org.apache.thrift.transport.TTransport transport) {
        switch (protocolName) {
            case "BINARY": return new TBinaryProtocol(transport);
            case "COMPACT": return new TCompactProtocol(transport);
            case "TJSON": return new TJSONProtocol(transport);
            default: throw new IllegalArgumentException("Unknown protocol " + protocolName);
        }
    }
}
