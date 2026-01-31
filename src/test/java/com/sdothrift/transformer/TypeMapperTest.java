package com.sdothrift.transformer;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
import org.apache.thrift.protocol.TType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for TypeMapper class.
 * Tests type mapping, conversion, and utility methods.
 */
class TypeMapperTest {
    
    private TypeMapper typeMapper;
    
    @BeforeEach
    void setUp() {
        typeMapper = new TypeMapper();
        TypeMapper.clearCaches(); // Ensure clean state for each test
    }
    
    @Test
    @DisplayName("Should map Thrift base types to SDO types correctly")
    void shouldMapThriftBaseTypesToSDOTypes() {
        assertThat(TypeMapper.mapThriftToSDO(TType.BOOL)).isEqualTo(Boolean.class);
        assertThat(TypeMapper.mapThriftToSDO(TType.BYTE)).isEqualTo(Byte.class);
        assertThat(TypeMapper.mapThriftToSDO(TType.I16)).isEqualTo(Short.class);
        assertThat(TypeMapper.mapThriftToSDO(TType.I32)).isEqualTo(Integer.class);
        assertThat(TypeMapper.mapThriftToSDO(TType.I64)).isEqualTo(Long.class);
        assertThat(TypeMapper.mapThriftToSDO(TType.DOUBLE)).isEqualTo(Double.class);
        assertThat(TypeMapper.mapThriftToSDO(TType.STRING)).isEqualTo(String.class);
    }
    
    @Test
    @DisplayName("Should return null for unsupported Thrift types")
    void shouldReturnNullForUnsupportedThriftTypes() {
        assertThat(TypeMapper.mapThriftToSDO(TType.LIST)).isNull();
        assertThat(TypeMapper.mapThriftToSDO(TType.SET)).isNull();
        assertThat(TypeMapper.mapThriftToSDO(TType.MAP)).isNull();
        assertThat(TypeMapper.mapThriftToSDO(TType.STRUCT)).isNull();
    }
    
    @Test
    @DisplayName("Should map SDO types to Thrift types correctly")
    void shouldMapSDOBaseTypesToThriftTypes() {
        assertThat(TypeMapper.mapSDOToThrift(Boolean.class)).isEqualTo(TType.BOOL);
        assertThat(TypeMapper.mapSDOToThrift(Byte.class)).isEqualTo(TType.BYTE);
        assertThat(TypeMapper.mapSDOToThrift(Short.class)).isEqualTo(TType.I16);
        assertThat(TypeMapper.mapSDOToThrift(Integer.class)).isEqualTo(TType.I32);
        assertThat(TypeMapper.mapSDOToThrift(Long.class)).isEqualTo(TType.I64);
        assertThat(TypeMapper.mapSDOToThrift(Double.class)).isEqualTo(TType.DOUBLE);
        assertThat(TypeMapper.mapSDOToThrift(String.class)).isEqualTo(TType.STRING);
    }
    
    @Test
    @DisplayName("Should map SDO primitive types to Thrift types")
    void shouldMapSDOPrimitiveTypesToThriftTypes() {
        assertThat(TypeMapper.mapSDOToThrift(boolean.class)).isEqualTo(TType.BOOL);
        assertThat(TypeMapper.mapSDOToThrift(byte.class)).isEqualTo(TType.BYTE);
        assertThat(TypeMapper.mapSDOToThrift(short.class)).isEqualTo(TType.I16);
        assertThat(TypeMapper.mapSDOToThrift(int.class)).isEqualTo(TType.I32);
        assertThat(TypeMapper.mapSDOToThrift(long.class)).isEqualTo(TType.I64);
        assertThat(TypeMapper.mapSDOToThrift(double.class)).isEqualTo(TType.DOUBLE);
    }
    
    @Test
    @DisplayName("Should return null for unsupported SDO types")
    void shouldReturnNullForUnsupportedSDOTypes() {
        assertThat(TypeMapper.mapSDOToThrift(Object.class)).isNull();
        assertThat(TypeMapper.mapSDOToThrift(TestDataGenerator.class)).isNull();
    }
    
    @ParameterizedTest
    @ValueSource(bytes = {TType.LIST, TType.SET, TType.MAP})
    @DisplayName("Should identify container types correctly")
    void shouldIdentifyContainerTypes(byte thriftType) {
        assertThat(TypeMapper.isContainerType(thriftType)).isTrue();
    }
    
    @ParameterizedTest
    @ValueSource(bytes = {TType.BOOL, TType.BYTE, TType.I16, TType.I32, TType.I64, TType.DOUBLE, TType.STRING})
    @DisplayName("Should identify base types correctly")
    void shouldIdentifyBaseTypes(byte thriftType) {
        assertThat(TypeMapper.isBaseType(thriftType)).isTrue();
    }
    
    @Test
    @DisplayName("Should identify struct types correctly")
    void shouldIdentifyStructTypes() {
        assertThat(TypeMapper.isStructType(TType.STRUCT)).isTrue();
        assertThat(TypeMapper.isStructType(TType.BOOL)).isFalse();
    }
    
    @Test
    @DisplayName("Should convert values between compatible types")
    void shouldConvertValuesBetweenCompatibleTypes() {
        // Test number conversions
        assertThat(TypeMapper.convertValue(123, Long.class)).isEqualTo(123L);
        assertThat(TypeMapper.convertValue(123L, Integer.class)).isEqualTo(123);
        assertThat(TypeMapper.convertValue(3.14, Double.class)).isEqualTo(3.14);
        assertThat(TypeMapper.convertValue(3.14f, Double.class)).isEqualTo(3.14d);
        
        // Test string conversions
        assertThat(TypeMapper.convertValue("123", Integer.class)).isEqualTo(123);
        assertThat(TypeMapper.convertValue("true", Boolean.class)).isEqualTo(true);
        assertThat(TypeMapper.convertValue("3.14", Double.class)).isEqualTo(3.14);
        
        // Test boolean conversions
        assertThat(TypeMapper.convertValue(true, Boolean.class)).isEqualTo(true);
        assertThat(TypeMapper.convertValue(false, Boolean.class)).isEqualTo(false);
    }
    
    @Test
    @DisplayName("Should handle null conversions")
    void shouldHandleNullConversions() {
        assertThat(TypeMapper.convertValue(null, String.class)).isNull();
        assertThat(TypeMapper.convertValue(null, Integer.class)).isNull();
        assertThat(TypeMapper.convertValue(null, Object.class)).isNull();
    }
    
    @Test
    @DisplayName("Should handle same type conversions")
    void shouldHandleSameTypeConversions() {
        String stringValue = "test";
        assertThat(TypeMapper.convertValue(stringValue, String.class)).isSameAs(stringValue);
        
        Integer intValue = 42;
        assertThat(TypeMapper.convertValue(intValue, Integer.class)).isSameAs(intValue);
    }
    
    @Test
    @DisplayName("Should convert primitive to wrapper types")
    void shouldConvertPrimitiveToWrapperTypes() {
        assertThat(TypeMapper.convertValue(true, Boolean.class)).isEqualTo(true);
        assertThat(TypeMapper.convertValue((byte) 1, Byte.class)).isEqualTo((byte) 1);
        assertThat(TypeMapper.convertValue((short) 2, Short.class)).isEqualTo((short) 2);
        assertThat(TypeMapper.convertValue(3, Integer.class)).isEqualTo(3);
        assertThat(TypeMapper.convertValue(4L, Long.class)).isEqualTo(4L);
        assertThat(TypeMapper.convertValue(5.0, Double.class)).isEqualTo(5.0);
    }
    
    @Test
    @DisplayName("Should convert wrapper to primitive types")
    void shouldConvertWrapperToPrimitiveTypes() {
        assertThat(TypeMapper.convertValue(Boolean.TRUE, boolean.class)).isEqualTo(true);
        assertThat(TypeMapper.convertValue(Byte.valueOf((byte) 1), byte.class)).isEqualTo((byte) 1);
        assertThat(TypeMapper.convertValue(Short.valueOf((short) 2), short.class)).isEqualTo((short) 2);
        assertThat(TypeMapper.convertValue(Integer.valueOf(3), int.class)).isEqualTo(3);
        assertThat(TypeMapper.convertValue(Long.valueOf(4L), long.class)).isEqualTo(4L);
        assertThat(TypeMapper.convertValue(Double.valueOf(5.0), double.class)).isEqualTo(5.0);
    }
    
    @Test
    @DisplayName("Should get component type for collections")
    void shouldGetComponentTypeForCollections() {
        java.util.List<String> stringList = java.util.Arrays.asList("a", "b", "c");
        assertThat(TypeMapper.getComponentType(stringList)).isEqualTo(String.class);
        
        java.util.Set<Integer> intSet = java.util.Set.of(1, 2, 3);
        assertThat(TypeMapper.getComponentType(intSet)).isEqualTo(Integer.class);
        
        java.util.List<Object> emptyList = new java.util.ArrayList<>();
        assertThat(TypeMapper.getComponentType(emptyList)).isEqualTo(Object.class);
        
        java.util.Set<Object> emptySet = new java.util.HashSet<>();
        assertThat(TypeMapper.getComponentType(emptySet)).isEqualTo(Object.class);
    }
    
    @Test
    @DisplayName("Should throw exception for invalid conversions")
    void shouldThrowExceptionForInvalidConversions() {
        assertThatThrownBy(() -> TypeMapper.convertValue("invalid", Integer.class))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot convert value");
        
        assertThatThrownBy(() -> TypeMapper.convertValue(new Object(), String.class))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot convert value");
    }
    
    @ParameterizedTest
    @MethodSource("provideValidConversions")
    @DisplayName("Should handle all valid conversion scenarios")
    <T> void shouldHandleAllValidConversionScenarios(Object input, Class<T> targetClass, T expected) {
        T result = TypeMapper.convertValue(input, targetClass);
        assertThat(result).isEqualTo(expected);
    }
    
    private static Stream<Arguments> provideValidConversions() {
        return Stream.of(
            Arguments.of("hello", String.class, "hello"),
            Arguments.of(42, Integer.class, 42),
            Arguments.of(42L, Long.class, 42L),
            Arguments.of(true, Boolean.class, true),
            Arguments.of(3.14, Double.class, 3.14),
            Arguments.of("123", Integer.class, 123),
            Arguments.of("true", Boolean.class, true),
            Arguments.of("3.14", Double.class, 3.14),
            Arguments.of(Byte.valueOf((byte) 1), byte.class, (byte) 1),
            Arguments.of(Integer.valueOf(42), int.class, 42)
        );
    }
    
    @Test
    @DisplayName("Should get field metadata for Thrift classes")
    void shouldGetFieldMetadataForThriftClasses() {
        Map<?, ?> metadata = TypeMapper.getFieldMetaData(TestDataGenerator.TestThriftStruct.class);
        assertThat(metadata).isNotNull();
        assertThat(metadata).isNotEmpty();
        
        // Should contain all expected fields
        assertThat(metadata).hasSize(7); // id, name, active, score, tags, properties, nested
    }
    
    @Test
    @DisplayName("Should cache field metadata for performance")
    void shouldCacheFieldMetadataForPerformance() {
        // First call should populate cache
        Map<?, ?> metadata1 = TypeMapper.getFieldMetaData(TestDataGenerator.TestThriftStruct.class);
        assertThat(metadata1).isNotNull();
        
        // Second call should return cached result
        Map<?, ?> metadata2 = TypeMapper.getFieldMetaData(TestDataGenerator.TestThriftStruct.class);
        assertThat(metadata2).isSameAs(metadata1);
        
        // Verify cache statistics
        var stats = TypeMapper.getCacheStatistics();
        assertThat(stats.get("fieldMetaDataCacheSize")).isGreaterThan(0);
    }
    
    @Test
    @DisplayName("Should get class fields with caching")
    void shouldGetClassFieldsWithCaching() {
        java.util.List<java.lang.reflect.Field> fields1 = TypeMapper.getClassFields(TestDataGenerator.TestThriftStruct.class);
        assertThat(fields1).isNotNull();
        assertThat(fields1).isNotEmpty();
        
        // Second call should return cached result
        java.util.List<java.lang.reflect.Field> fields2 = TypeMapper.getClassFields(TestDataGenerator.TestThriftStruct.class);
        assertThat(fields2).isSameAs(fields1);
        
        // Verify cache statistics
        var stats = TypeMapper.getCacheStatistics();
        assertThat(stats.get("classFieldsCacheSize")).isGreaterThan(0);
    }
    
    @Test
    @DisplayName("Should clear caches successfully")
    void shouldClearCachesSuccessfully() {
        // Populate caches
        TypeMapper.getFieldMetaData(TestDataGenerator.TestThriftStruct.class);
        TypeMapper.getClassFields(TestDataGenerator.TestThriftStruct.class);
        
        // Verify caches are populated
        var statsBefore = TypeMapper.getCacheStatistics();
        assertThat(statsBefore.get("fieldMetaDataCacheSize")).isGreaterThan(0);
        assertThat(statsBefore.get("classFieldsCacheSize")).isGreaterThan(0);
        
        // Clear caches
        TypeMapper.clearCaches();
        
        // Verify caches are cleared
        var statsAfter = TypeMapper.getCacheStatistics();
        assertThat(statsAfter.get("fieldMetaDataCacheSize")).isEqualTo(0);
        assertThat(statsAfter.get("classFieldsCacheSize")).isEqualTo(0);
    }
    
    @Test
    @DisplayName("Should provide cache statistics")
    void shouldProvideCacheStatistics() {
        // Initially empty
        var stats = TypeMapper.getCacheStatistics();
        assertThat(stats).containsKeys("fieldMetaDataCacheSize", "classFieldsCacheSize");
        assertThat(stats.get("fieldMetaDataCacheSize")).isEqualTo(0);
        assertThat(stats.get("classFieldsCacheSize")).isEqualTo(0);
        
        // After using metadata
        TypeMapper.getFieldMetaData(TestDataGenerator.TestThriftStruct.class);
        TypeMapper.getClassFields(TestDataGenerator.TestThriftStruct.class);
        
        stats = TypeMapper.getCacheStatistics();
        assertThat(stats.get("fieldMetaDataCacheSize")).isEqualTo(1);
        assertThat(stats.get("classFieldsCacheSize")).isEqualTo(1);
    }
    
    @Test
    @DisplayName("Should handle edge cases in value conversion")
    void shouldHandleEdgeCasesInValueConversion() {
        // Test extreme values
        assertThat(TypeMapper.convertValue(Integer.MAX_VALUE, Long.class)).isEqualTo((long) Integer.MAX_VALUE);
        assertThat(TypeMapper.convertValue(Long.MAX_VALUE, String.class)).isEqualTo(String.valueOf(Long.MAX_VALUE));
        
        // Test special floating point values
        assertThat(TypeMapper.convertValue(Double.POSITIVE_INFINITY, Double.class)).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(TypeMapper.convertValue(Double.NaN, Double.class)).isNaN();
        
        // Test empty and whitespace strings
        assertThat(TypeMapper.convertValue("", String.class)).isEqualTo("");
        assertThat(TypeMapper.convertValue("  ", String.class)).isEqualTo("  ");
        
        // Test numeric strings with whitespace
        assertThat(TypeMapper.convertValue(" 123 ", Integer.class)).isEqualTo(123);
    }
}