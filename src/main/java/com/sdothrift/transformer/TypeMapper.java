package com.sdothrift.transformer;

import org.apache.thrift.TBase;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for mapping Thrift types to SDO types and vice versa.
 * Provides type conversion utilities and metadata caching for performance optimization.
 */
public class TypeMapper {
    
    private static final Logger logger = LoggerFactory.getLogger(TypeMapper.class);
    
    // Cache for field metadata to improve performance
    private static final Map<String, Map<?, ?>> fieldMetaDataCache = new ConcurrentHashMap<>();
    
    // Cache for class field information
    private static final Map<String, List<Field>> classFieldsCache = new ConcurrentHashMap<>();
    
    /**
     * Thrift type to SDO type mapping for base types.
     */
    public static final Map<Byte, Class<?>> THRIFT_TO_SDO_TYPES = Map.of(
        TType.BOOL, Boolean.class,
        TType.BYTE, Byte.class,
        TType.I16, Short.class,
        TType.I32, Integer.class,
        TType.I64, Long.class,
        TType.DOUBLE, Double.class,
        TType.STRING, String.class
    );
    
    /**
     * SDO type to Thrift type mapping for base types.
     */
    public static final Map<Class<?>, Byte> SDO_TO_THRIFT_TYPES = Map.of(
        Boolean.class, TType.BOOL,
        Byte.class, TType.BYTE,
        Short.class, TType.I16,
        Integer.class, TType.I32,
        Long.class, TType.I64,
        Double.class, TType.DOUBLE,
        String.class, TType.STRING
    );
    
    /**
     * Maps Thrift type identifier to corresponding SDO class.
     *
     * @param thriftType the Thrift type identifier
     * @return the corresponding SDO class, or null if not found
     */
    public static Class<?> mapThriftToSDO(byte thriftType) {
        Class<?> sdoType = THRIFT_TO_SDO_TYPES.get(thriftType);
        if (sdoType == null) {
            logger.debug("No direct mapping found for Thrift type: {}", thriftType);
        }
        return sdoType;
    }
    
    /**
     * Maps SDO class to corresponding Thrift type identifier.
     *
     * @param sdoClass the SDO class
     * @return the corresponding Thrift type identifier, or null if not found
     */
    public static Byte mapSDOToThrift(Class<?> sdoClass) {
        Byte thriftType = SDO_TO_THRIFT_TYPES.get(sdoClass);
        if (thriftType == null) {
            // Handle primitive types
            if (sdoClass == boolean.class) return TType.BOOL;
            if (sdoClass == byte.class) return TType.BYTE;
            if (sdoClass == short.class) return TType.I16;
            if (sdoClass == int.class) return TType.I32;
            if (sdoClass == long.class) return TType.I64;
            if (sdoClass == double.class) return TType.DOUBLE;
            
            logger.debug("No direct mapping found for SDO class: {}", sdoClass.getName());
        }
        return thriftType;
    }
    
    /**
     * Determines if a Thrift type is a container type (list, set, map).
     *
     * @param thriftType the Thrift type identifier
     * @return true if it's a container type, false otherwise
     */
    public static boolean isContainerType(byte thriftType) {
        return thriftType == TType.LIST || thriftType == TType.SET || thriftType == TType.MAP;
    }
    
    /**
     * Determines if a Thrift type is a struct type.
     *
     * @param thriftType the Thrift type identifier
     * @return true if it's a struct type, false otherwise
     */
    public static boolean isStructType(byte thriftType) {
        return thriftType == TType.STRUCT;
    }
    
    /**
     * Determines if a Thrift type is a base type (primitive or string).
     *
     * @param thriftType the Thrift type identifier
     * @return true if it's a base type, false otherwise
     */
    public static boolean isBaseType(byte thriftType) {
        return THRIFT_TO_SDO_TYPES.containsKey(thriftType);
    }
    
    /**
     * Gets the component type of a container type (for lists and sets).
     *
     * @param containerType the container Thrift object
     * @return the component class type
     */
    public static Class<?> getComponentType(Object containerType) {
        if (containerType instanceof List) {
            if (!((List<?>) containerType).isEmpty()) {
                Object firstElement = ((List<?>) containerType).get(0);
                return firstElement != null ? firstElement.getClass() : Object.class;
            }
        } else if (containerType instanceof Set) {
            if (!((Set<?>) containerType).isEmpty()) {
                Object firstElement = ((Set<?>) containerType).iterator().next();
                return firstElement != null ? firstElement.getClass() : Object.class;
            }
        }
        return Object.class;
    }
    
    /**
     * Converts a value from one type to another, handling common type conversions.
     *
     * @param value the value to convert
     * @param targetClass the target class
     * @return the converted value
     * @throws IllegalArgumentException if conversion is not possible
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object value, Class<T> targetClass) {
        if (value == null) {
            return null;
        }
        
        if (targetClass.isInstance(value)) {
            return (T) value;
        }
        
        // Handle primitive to wrapper conversions
        if (targetClass == Boolean.class && value.getClass() == boolean.class) {
            return (T) Boolean.valueOf((boolean) value);
        }
        if (targetClass == Byte.class && value.getClass() == byte.class) {
            return (T) Byte.valueOf((byte) value);
        }
        if (targetClass == Short.class && value.getClass() == short.class) {
            return (T) Short.valueOf((short) value);
        }
        if (targetClass == Integer.class && value.getClass() == int.class) {
            return (T) Integer.valueOf((int) value);
        }
        if (targetClass == Long.class && value.getClass() == long.class) {
            return (T) Long.valueOf((long) value);
        }
        if (targetClass == Double.class && value.getClass() == double.class) {
            return (T) Double.valueOf((double) value);
        }
        
        // Handle wrapper to primitive conversions
        if (targetClass == boolean.class && value instanceof Boolean) {
            return (T) value;
        }
        if (targetClass == byte.class && value instanceof Byte) {
            return (T) value;
        }
        if (targetClass == short.class && value instanceof Short) {
            return (T) value;
        }
        if (targetClass == int.class && value instanceof Integer) {
            return (T) value;
        }
        if (targetClass == long.class && value instanceof Long) {
            return (T) value;
        }
        if (targetClass == double.class && value instanceof Double) {
            return (T) value;
        }
        
        // Handle numeric conversions
        if (value instanceof Number) {
            Number number = (Number) value;
            if (targetClass == Integer.class || targetClass == int.class) {
                return (T) Integer.valueOf(number.intValue());
            }
            if (targetClass == Long.class || targetClass == long.class) {
                return (T) Long.valueOf(number.longValue());
            }
            if (targetClass == Double.class || targetClass == double.class) {
                return (T) Double.valueOf(number.doubleValue());
            }
            if (targetClass == Float.class || targetClass == float.class) {
                return (T) Float.valueOf(number.floatValue());
            }
            if (targetClass == Short.class || targetClass == short.class) {
                return (T) Short.valueOf(number.shortValue());
            }
            if (targetClass == Byte.class || targetClass == byte.class) {
                return (T) Byte.valueOf(number.byteValue());
            }
        }
        
        // Handle string conversions
        if (targetClass == String.class) {
            return (T) value.toString();
        }
        
        if (value instanceof String) {
            String stringValue = (String) value;
            if (targetClass == Boolean.class || targetClass == boolean.class) {
                return (T) Boolean.valueOf(stringValue);
            }
            if (targetClass == Integer.class || targetClass == int.class) {
                return (T) Integer.valueOf(stringValue);
            }
            if (targetClass == Long.class || targetClass == long.class) {
                return (T) Long.valueOf(stringValue);
            }
            if (targetClass == Double.class || targetClass == double.class) {
                return (T) Double.valueOf(stringValue);
            }
        }
        
        throw new IllegalArgumentException("Cannot convert value of type " + 
            value.getClass().getName() + " to target type " + targetClass.getName());
    }
    
    /**
     * Gets the field metadata for a Thrift class, using caching for performance.
     *
     * @param thriftClass the Thrift class
     * @return the field metadata map
     */
    @SuppressWarnings("unchecked")
    public static Map<?, ?> getFieldMetaData(Class<? extends TBase> thriftClass) {
        String className = thriftClass.getName();
        return fieldMetaDataCache.computeIfAbsent(className, k -> {
            try {
                Field metaDataField = thriftClass.getField("metaDataMap");
                return (Map<?, ?>) metaDataField.get(null);
            } catch (Exception e) {
                logger.error("Failed to get field metadata for class: " + className, e);
                return new ConcurrentHashMap<>();
            }
        });
    }
    
    /**
     * Gets all fields for a class, using caching for performance.
     *
     * @param clazz the class to get fields for
     * @return list of fields
     */
    public static List<Field> getClassFields(Class<?> clazz) {
        String className = clazz.getName();
        return classFieldsCache.computeIfAbsent(className, k -> {
            java.util.List<Field> fields = new java.util.ArrayList<>();
            Class<?> currentClass = clazz;
            while (currentClass != null && currentClass != Object.class) {
                for (Field field : currentClass.getDeclaredFields()) {
                    if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        fields.add(field);
                    }
                }
                currentClass = currentClass.getSuperclass();
            }
            return fields;
        });
    }
    
    /**
     * Clears all caches. Useful for testing or memory management.
     */
    public static void clearCaches() {
        fieldMetaDataCache.clear();
        classFieldsCache.clear();
    }
    
    /**
     * Gets cache statistics for monitoring purposes.
     *
     * @return a map containing cache statistics
     */
    public static Map<String, Integer> getCacheStatistics() {
        Map<String, Integer> stats = new java.util.HashMap<>();
        stats.put("fieldMetaDataCacheSize", fieldMetaDataCache.size());
        stats.put("classFieldsCacheSize", classFieldsCache.size());
        return stats;
    }
}