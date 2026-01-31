package com.sdothrift.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import com.sdothrift.serializer.ThriftSerializer;
import org.apache.thrift.TBase;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transformer class for converting SDO DataObjects to Thrift objects.
 * Provides comprehensive type mapping and handles complex nested structures.
 */
public class SDOToThriftTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(SDOToThriftTransformer.class);
    
    private final ThriftSDOConfiguration configuration;
    private final ThriftSerializer thriftSerializer;
    private final ObjectMapper objectMapper;
    
    // Cache for Thrift class constructors to improve performance
    private final Map<String, Constructor<? extends TBase>> constructorCache = new ConcurrentHashMap<>();
    
    // Cache for field metadata to improve performance
    private final Map<String, Map<?, ?>> fieldMetaDataCache = new ConcurrentHashMap<>();
    
    /**
     * Constructs a new SDOToThriftTransformer with the given configuration.
     *
     * @param configuration the configuration to use
     */
    public SDOToThriftTransformer(ThriftSDOConfiguration configuration) {
        this.configuration = configuration;
        this.thriftSerializer = new ThriftSerializer(configuration);
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Transforms an SDO DataObject to a Thrift object.
     *
     * @param dataObject the SDO DataObject to transform
     * @param targetThriftClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return the transformed Thrift object
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    public <T extends TBase> T transformToThrift(EDataObject dataObject, Class<T> targetThriftClass) 
            throws ThriftSDODataHandlerException {
        
        if (dataObject == null) {
            return null;
        }
        
        try {
            // Convert SDO to JSON for easier processing
            JsonNode jsonNode = transformSDOToJson(dataObject);
            
            // Convert JSON to Thrift
            return transformJsonToThrift(jsonNode, targetThriftClass);
            
        } catch (Exception e) {
            logger.error("Failed to transform SDO to Thrift: {} -> {}", 
                dataObject.eClass().getName(), targetThriftClass.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.TRANSFORMATION_ERROR,
                "Failed to transform SDO to Thrift: " + dataObject.eClass().getName() + 
                " -> " + targetThriftClass.getName(),
                "SDO class: " + dataObject.eClass().getName() + 
                ", Thrift class: " + targetThriftClass.getName(),
                e
            );
        }
    }
    
    /**
     * Transforms an SDO DataObject to JSON representation.
     *
     * @param dataObject the SDO DataObject
     * @return the JSON representation
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private JsonNode transformSDOToJson(EDataObject dataObject) throws ThriftSDODataHandlerException {
        try {
            ObjectNode jsonNode = objectMapper.createObjectNode();
            EClass eClass = dataObject.eClass();
            
            for (EStructuralFeature feature : eClass.getEStructuralFeatures()) {
                if (!dataObject.eIsSet(feature)) {
                    handleNullSDOField(jsonNode, feature.getName(), feature);
                    continue;
                }
                
                Object value = dataObject.eGet(feature);
                JsonNode fieldValue = transformSDOFieldToJson(value, feature);
                
                if (fieldValue != null) {
                    jsonNode.set(feature.getName(), fieldValue);
                }
            }
            
            return jsonNode;
            
        } catch (Exception e) {
            logger.error("Failed to transform SDO to JSON: {}", dataObject.eClass().getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.SDO_PROCESSING_ERROR,
                "Failed to transform SDO to JSON: " + dataObject.eClass().getName(),
                "SDO class: " + dataObject.eClass().getName(),
                e
            );
        }
    }
    
    /**
     * Transforms an SDO field value to JSON.
     *
     * @param value the field value
     * @param feature the structural feature
     * @return the JSON representation
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private JsonNode transformSDOFieldToJson(Object value, EStructuralFeature feature) 
            throws ThriftSDODataHandlerException {
        
        try {
            if (value == null) {
                return objectMapper.getNodeFactory().nullNode();
            }
            
            if (feature instanceof EAttribute) {
                // Handle basic attributes
                return objectMapper.valueToTree(value);
                
            } else if (feature instanceof EReference) {
                // Handle nested objects (references)
                if (value instanceof EDataObject) {
                    return transformSDOToJson((EDataObject) value);
                } else if (value instanceof Collection) {
                    // Handle collection of nested objects
                    return transformCollectionToJson((Collection<?>) value);
                } else {
                    return objectMapper.valueToTree(value);
                }
            } else {
                // Default handling
                return objectMapper.valueToTree(value);
            }
            
        } catch (Exception e) {
            logger.error("Failed to transform SDO field to JSON: {}", feature.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.JSON_PROCESSING_ERROR,
                "Failed to transform SDO field to JSON: " + feature.getName(),
                "Feature: " + feature.getName() + ", Type: " + feature.getClass().getName(),
                e
            );
        }
    }
    
    /**
     * Transforms a collection to JSON array.
     *
     * @param collection the collection to transform
     * @return the JSON array
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private JsonNode transformCollectionToJson(Collection<?> collection) 
            throws ThriftSDODataHandlerException {
        
        try {
            return objectMapper.valueToTree(collection);
        } catch (Exception e) {
            logger.error("Failed to transform collection to JSON", e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.JSON_PROCESSING_ERROR,
                "Failed to transform collection to JSON",
                "Collection type: " + collection.getClass().getName(),
                e
            );
        }
    }
    
    /**
     * Transforms JSON to a Thrift object.
     *
     * @param jsonNode the JSON node
     * @param targetThriftClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return the Thrift object
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    @SuppressWarnings("unchecked")
    private <T extends TBase> T transformJsonToThrift(JsonNode jsonNode, Class<T> targetThriftClass) 
            throws ThriftSDODataHandlerException {
        
        try {
            // Convert JSON to string for Thrift deserializer
            String jsonString = objectMapper.writeValueAsString(jsonNode);
            return thriftSerializer.deserializeFromString(jsonString, targetThriftClass);
            
        } catch (Exception e) {
            logger.error("Failed to transform JSON to Thrift: {}", targetThriftClass.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.THRIFT_PROCESSING_ERROR,
                "Failed to transform JSON to Thrift: " + targetThriftClass.getName(),
                "Thrift class: " + targetThriftClass.getName(),
                e
            );
        }
    }
    
    /**
     * Handles null field values according to configuration.
     *
     * @param jsonNode the JSON node to modify
     * @param fieldName the field name
     * @param feature the structural feature
     */
    private void handleNullSDOField(ObjectNode jsonNode, String fieldName, EStructuralFeature feature) {
        switch (configuration.getNullHandlingStrategy()) {
            case PRESERVE:
                jsonNode.set(fieldName, objectMapper.getNodeFactory().nullNode());
                break;
            case DEFAULT:
                Object defaultValue = getDefaultValueForFeature(feature);
                if (defaultValue != null) {
                    jsonNode.set(fieldName, objectMapper.valueToTree(defaultValue));
                }
                break;
            case OMIT:
                // Don't add anything - field will be omitted
                break;
            case ERROR:
                throw new IllegalArgumentException("Null value encountered for field: " + fieldName);
            default:
                jsonNode.set(fieldName, objectMapper.getNodeFactory().nullNode());
        }
    }
    
    /**
     * Gets the default value for an SDO structural feature.
     *
     * @param feature the structural feature
     * @return the default value
     */
    private Object getDefaultValueForFeature(EStructuralFeature feature) {
        try {
            if (feature instanceof EAttribute) {
                EAttribute attribute = (EAttribute) feature;
                Class<?> instanceClass = attribute.getInstanceClass();
                
                if (instanceClass == boolean.class || instanceClass == Boolean.class) {
                    return false;
                } else if (instanceClass == byte.class || instanceClass == Byte.class) {
                    return (byte) 0;
                } else if (instanceClass == short.class || instanceClass == Short.class) {
                    return (short) 0;
                } else if (instanceClass == int.class || instanceClass == Integer.class) {
                    return 0;
                } else if (instanceClass == long.class || instanceClass == Long.class) {
                    return 0L;
                } else if (instanceClass == float.class || instanceClass == Float.class) {
                    return 0.0f;
                } else if (instanceClass == double.class || instanceClass == Double.class) {
                    return 0.0;
                } else if (instanceClass == String.class) {
                    return "";
                }
            } else if (feature instanceof EReference) {
                EReference reference = (EReference) feature;
                if (reference.isMany()) {
                    return new java.util.ArrayList<>();
                } else {
                    return null;
                }
            }
            
            return null;
        } catch (Exception e) {
            logger.warn("Failed to get default value for feature: {}", feature.getName(), e);
            return null;
        }
    }
    
    /**
     * Gets the Thrift field metadata for a class, using caching for performance.
     *
     * @param thriftClass the Thrift class
     * @return the field metadata map
     */
    private Map<?, ?> getThriftFieldMetaData(Class<? extends TBase> thriftClass) {
        String className = thriftClass.getName();
        return fieldMetaDataCache.computeIfAbsent(className, k -> {
            try {
                Field metaDataField = thriftClass.getField("metaDataMap");
                return (Map<?, ?>) metaDataField.get(null);
            } catch (Exception e) {
                logger.error("Failed to get field metadata for class: " + className, e);
                return new java.util.HashMap<>();
            }
        });
    }
    
    /**
     * Validates if the SDO DataObject can be transformed to the target Thrift class.
     *
     * @param dataObject the SDO DataObject
     * @param targetThriftClass the target Thrift class
     * @return true if validation passes, false otherwise
     */
    public boolean validateTransformation(EDataObject dataObject, Class<? extends TBase> targetThriftClass) {
        if (dataObject == null || targetThriftClass == null) {
            return false;
        }
        
        try {
            // Get Thrift field metadata
            Map<?, ?> thriftFields = getThriftFieldMetaData(targetThriftClass);
            
            // Get SDO structural features
            List<EStructuralFeature> sdoFeatures = dataObject.eClass().getEStructuralFeatures();
            
            // Basic validation: check if SDO has at least the required fields
            int requiredThriftFields = 0;
            for (Object entry : thriftFields.values()) {
                if (entry instanceof FieldMetaData) {
                    FieldMetaData metaData = (FieldMetaData) entry;
                    if (metaData.requirementType == FieldMetaData.REQUIRED) {
                        requiredThriftFields++;
                    }
                }
            }
            
            int availableSdoFields = 0;
            for (EStructuralFeature feature : sdoFeatures) {
                if (dataObject.eIsSet(feature)) {
                    availableSdoFields++;
                }
            }
            
            if (configuration.isStrictValidationEnabled()) {
                return availableSdoFields >= requiredThriftFields;
            } else {
                return true; // Lenient validation
            }
            
        } catch (Exception e) {
            logger.warn("Validation failed for transformation: {} -> {}", 
                dataObject.eClass().getName(), targetThriftClass.getName(), e);
            return false;
        }
    }
    
    /**
     * Gets the Thrift class constructor, using caching for performance.
     *
     * @param thriftClass the Thrift class
     * @param <T> the type
     * @return the constructor
     * @throws ThriftSDODataHandlerException if constructor cannot be found
     */
    @SuppressWarnings("unchecked")
    private <T extends TBase> Constructor<T> getThriftConstructor(Class<T> thriftClass) 
            throws ThriftSDODataHandlerException {
        
        String className = thriftClass.getName();
        return (Constructor<T>) constructorCache.computeIfAbsent(className, k -> {
            try {
                Constructor<?> constructor = thriftClass.getDeclaredConstructor();
                constructor.setAccessible(true);
                return constructor;
            } catch (Exception e) {
                logger.error("Failed to get constructor for Thrift class: {}", className, e);
                throw new RuntimeException("Failed to get constructor for Thrift class: " + className, e);
            }
        });
    }
    
    /**
     * Clears all caches. Useful for testing or memory management.
     */
    public void clearCaches() {
        constructorCache.clear();
        fieldMetaDataCache.clear();
        TypeMapper.clearCaches();
    }
    
    /**
     * Gets cache statistics for monitoring purposes.
     *
     * @return a map containing cache statistics
     */
    public Map<String, Integer> getCacheStatistics() {
        Map<String, Integer> stats = new java.util.HashMap<>();
        stats.put("constructorCacheSize", constructorCache.size());
        stats.put("fieldMetaDataCacheSize", fieldMetaDataCache.size());
        stats.putAll(TypeMapper.getCacheStatistics());
        return stats;
    }
    
    /**
     * Gets the current configuration.
     *
     * @return the configuration
     */
    public ThriftSDOConfiguration getConfiguration() {
        return configuration;
    }
}