package com.sdothrift.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import com.sdothrift.serializer.ThriftSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TType;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.impl.EObjectImpl;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.eclipse.emf.ecore.sdo.util.SDOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transformer class for converting Thrift objects to SDO DataObjects.
 * Provides comprehensive type mapping and handles complex nested structures.
 */
public class ThriftToSDOTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(ThriftToSDOTransformer.class);
    
    private final ThriftSDOConfiguration configuration;
    private final ThriftSerializer thriftSerializer;
    private final ObjectMapper objectMapper;
    
    // Cache for generated EClasses to improve performance
    private final Map<String, EClass> eclassCache = new ConcurrentHashMap<>();
    
    /**
     * Constructs a new ThriftToSDOTransformer with the given configuration.
     *
     * @param configuration the configuration to use
     */
    public ThriftToSDOTransformer(ThriftSDOConfiguration configuration) {
        this.configuration = configuration;
        this.thriftSerializer = new ThriftSerializer(configuration);
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Transforms a Thrift object to an SDO DataObject.
     *
     * @param thriftObject the Thrift object to transform
     * @return the transformed SDO DataObject
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    public EDataObject transformToSDO(TBase thriftObject) throws ThriftSDODataHandlerException {
        if (thriftObject == null) {
            return null;
        }
        
        try {
            // Convert Thrift to JSON for easier processing
            String jsonRepresentation = thriftSerializer.serializeToString(thriftObject);
            if (jsonRepresentation == null || jsonRepresentation.trim().isEmpty()) {
                return createEmptySDO(thriftObject.getClass());
            }
            
            JsonNode jsonNode = objectMapper.readTree(jsonRepresentation);
            return transformJsonToSDO(jsonNode, thriftObject.getClass());
            
        } catch (Exception e) {
            logger.error("Failed to transform Thrift object to SDO: {}", thriftObject.getClass().getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.TRANSFORMATION_ERROR,
                "Failed to transform Thrift object to SDO: " + thriftObject.getClass().getName(),
                "Thrift class: " + thriftObject.getClass().getName(),
                e
            );
        }
    }
    
    /**
     * Transforms a JSON representation of a Thrift object to SDO DataObject.
     *
     * @param jsonNode the JSON node representing the Thrift object
     * @param thriftClass the original Thrift class
     * @return the transformed SDO DataObject
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private EDataObject transformJsonToSDO(JsonNode jsonNode, Class<? extends TBase> thriftClass) 
            throws ThriftSDODataHandlerException {
        
        try {
            // Create or get cached EClass for the Thrift class
            EClass eClass = getOrCreateEClass(thriftClass);
            
            // Create SDO DataObject instance
            EDataObject dataObject = (EDataObject) EcoreFactory.eINSTANCE.create(eClass);
            
            // Get Thrift field metadata
            Map<?, ?> fieldMetaData = TypeMapper.getFieldMetaData(thriftClass);
            
            // Process each field
            for (Map.Entry<?, ?> entry : fieldMetaData.entrySet()) {
                Object fieldId = entry.getKey();
                FieldMetaData metaData = (FieldMetaData) entry.getValue();
                
                String fieldName = metaData.fieldName;
                byte fieldType = metaData.valueMetaData.type;
                
                // Get the value from JSON
                JsonNode fieldValueNode = jsonNode.get(fieldName);
                if (fieldValueNode == null || fieldValueNode.isNull()) {
                    handleNullField(dataObject, fieldName, fieldType);
                    continue;
                }
                
                // Transform the field value
                Object sdoValue = transformFieldToSDO(fieldValueNode, fieldType, metaData);
                
                // Set the value in the SDO object
                setSDOFieldValue(dataObject, fieldName, sdoValue);
            }
            
            return dataObject;
            
        } catch (Exception e) {
            logger.error("Failed to transform JSON to SDO for class: {}", thriftClass.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.SDO_PROCESSING_ERROR,
                "Failed to transform JSON to SDO for class: " + thriftClass.getName(),
                "Thrift class: " + thriftClass.getName(),
                e
            );
        }
    }
    
    /**
     * Transforms a field value from JSON to SDO format.
     *
     * @param fieldValueNode the JSON node containing the field value
     * @param fieldType the Thrift field type
     * @param metaData the field metadata
     * @return the transformed SDO value
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private Object transformFieldToSDO(JsonNode fieldValueNode, byte fieldType, FieldMetaData metaData) 
            throws ThriftSDODataHandlerException {
        
        try {
            switch (fieldType) {
                case TType.BOOL:
                    return fieldValueNode.asBoolean();
                    
                case TType.BYTE:
                    return (byte) fieldValueNode.asInt();
                    
                case TType.I16:
                    return (short) fieldValueNode.asInt();
                    
                case TType.I32:
                    return fieldValueNode.asInt();
                    
                case TType.I64:
                    return fieldValueNode.asLong();
                    
                case TType.DOUBLE:
                    return fieldValueNode.asDouble();
                    
                case TType.STRING:
                    return fieldValueNode.asText();
                    
                case TType.LIST:
                    return transformListToSDO(fieldValueNode, metaData);
                    
                case TType.SET:
                    return transformSetToSDO(fieldValueNode, metaData);
                    
                case TType.MAP:
                    return transformMapToSDO(fieldValueNode, metaData);
                    
                case TType.STRUCT:
                    return transformStructToSDO(fieldValueNode, metaData);
                    
                default:
                    logger.warn("Unsupported Thrift field type: {}", fieldType);
                    return null;
            }
        } catch (Exception e) {
            logger.error("Failed to transform field to SDO, type: {}", fieldType, e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.TYPE_MAPPING_ERROR,
                "Failed to transform field to SDO, type: " + fieldType,
                "Field metadata: " + metaData.toString(),
                e
            );
        }
    }
    
    /**
     * Transforms a JSON array to SDO List.
     *
     * @param arrayNode the JSON array node
     * @param metaData the field metadata
     * @return the SDO List
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private List<Object> transformListToSDO(JsonNode arrayNode, FieldMetaData metaData) 
            throws ThriftSDODataHandlerException {
        
        List<Object> sdoList = new java.util.ArrayList<>();
        
        for (JsonNode elementNode : arrayNode) {
            Object element = transformCollectionElementToSDO(elementNode, metaData);
            sdoList.add(element);
        }
        
        return sdoList;
    }
    
    /**
     * Transforms a JSON array to SDO Set.
     *
     * @param arrayNode the JSON array node
     * @param metaData the field metadata
     * @return the SDO Set
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private Set<Object> transformSetToSDO(JsonNode arrayNode, FieldMetaData metaData) 
            throws ThriftSDODataHandlerException {
        
        Set<Object> sdoSet = new java.util.HashSet<>();
        
        for (JsonNode elementNode : arrayNode) {
            Object element = transformCollectionElementToSDO(elementNode, metaData);
            sdoSet.add(element);
        }
        
        return sdoSet;
    }
    
    /**
     * Transforms a JSON object to SDO Map.
     *
     * @param objectNode the JSON object node
     * @param metaData the field metadata
     * @return the SDO Map
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private Map<Object, Object> transformMapToSDO(JsonNode objectNode, FieldMetaData metaData) 
            throws ThriftSDODataHandlerException {
        
        Map<Object, Object> sdoMap = new java.util.HashMap<>();
        
        for (Map.Entry<String, JsonNode> entry : objectNode.fields()) {
            String key = entry.getKey();
            JsonNode valueNode = entry.getValue();
            
            Object sdoKey = convertToSDOType(key, String.class);
            Object sdoValue = transformCollectionElementToSDO(valueNode, metaData);
            
            sdoMap.put(sdoKey, sdoValue);
        }
        
        return sdoMap;
    }
    
    /**
     * Transforms a JSON object to SDO struct (nested DataObject).
     *
     * @param structNode the JSON object node
     * @param metaData the field metadata
     * @return the SDO DataObject
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private EDataObject transformStructToSDO(JsonNode structNode, FieldMetaData metaData) 
            throws ThriftSDODataHandlerException {
        
        try {
            // For struct types, we need to extract the class name or handle differently
            // This is a simplified approach - in real implementation, you'd need more sophisticated handling
            String structName = metaData.fieldName + "Struct";
            Class<?> structClass = null; // This would need proper resolution from Thrift metadata
            
            // For now, create a generic approach
            return createGenericSDOFromJson(structNode, structName);
            
            // Create a temporary Thrift object to get the field metadata
            TBase tempObject = (TBase) structClass.getDeclaredConstructor().newInstance();
            
            // Transform the JSON to SDO
            return transformJsonToSDO(structNode, tempObject.getClass());
            
        } catch (Exception e) {
            logger.error("Failed to transform struct to SDO: {}", metaData.toString(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.TRANSFORMATION_ERROR,
                "Failed to transform struct to SDO",
                "Field metadata: " + metaData.toString(),
                e
            );
        }
    }
    
    /**
     * Transforms a collection element to SDO format.
     *
     * @param elementNode the JSON node for the element
     * @param metaData the field metadata
     * @return the transformed element
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private Object transformCollectionElementToSDO(JsonNode elementNode, FieldMetaData metaData) 
            throws ThriftSDODataHandlerException {
        
        if (elementNode.isNull()) {
            return null;
        }
        
                // For collection elements, assume basic types unless specified
                byte elementType = TType.STRING; // Default, should be enhanced based on metadata
                if (metaData.valueMetaData != null) {
                    elementType = metaData.valueMetaData.type;
                }
                
                return transformFieldToSDO(elementNode, elementType, metaData);
    }
    

    
    /**
     * Sets a field value in an SDO DataObject.
     *
     * @param dataObject the SDO DataObject
     * @param fieldName the field name
     * @param value the value to set
     */
    private void setSDOFieldValue(EDataObject dataObject, String fieldName, Object value) {
        try {
            EStructuralFeature feature = dataObject.eClass().getEStructuralFeature(fieldName);
            if (feature != null) {
                dataObject.eSet(feature, value);
            } else {
                logger.warn("Feature not found in SDO: {}", fieldName);
            }
        } catch (Exception e) {
            logger.error("Failed to set SDO field value: {}", fieldName, e);
        }
    }
    
    /**
     * Handles null field values according to configuration.
     *
     * @param dataObject the SDO DataObject
     * @param fieldName the field name
     * @param fieldType the field type
     */
    private void handleNullField(EDataObject dataObject, String fieldName, byte fieldType) {
        switch (configuration.getNullHandlingStrategy()) {
            case PRESERVE:
                setSDOFieldValue(dataObject, fieldName, null);
                break;
            case DEFAULT:
                setSDOFieldValue(dataObject, fieldName, getDefaultValueForType(fieldType));
                break;
            case OMIT:
                // Don't set anything - field will be omitted
                break;
            case ERROR:
                throw new IllegalArgumentException("Null value encountered for field: " + fieldName);
            default:
                setSDOFieldValue(dataObject, fieldName, null);
        }
    }
    
    /**
     * Gets the default value for a Thrift type.
     *
     * @param thriftType the Thrift type
     * @return the default value
     */
    private Object getDefaultValueForType(byte thriftType) {
        switch (thriftType) {
            case TType.BOOL:
                return false;
            case TType.BYTE:
                return (byte) 0;
            case TType.I16:
                return (short) 0;
            case TType.I32:
                return 0;
            case TType.I64:
                return 0L;
            case TType.DOUBLE:
                return 0.0;
            case TType.STRING:
                return "";
            case TType.LIST:
            case TType.SET:
                return new java.util.ArrayList<>();
            case TType.MAP:
                return new java.util.HashMap<>();
            default:
                return null;
        }
    }
    
    /**
     * Converts a value to the appropriate SDO type.
     *
     * @param value the value to convert
     * @param targetClass the target class
     * @return the converted value
     */
    private Object convertToSDOType(Object value, Class<?> targetClass) {
        try {
            return TypeMapper.convertValue(value, targetClass);
        } catch (Exception e) {
            logger.warn("Failed to convert to SDO type, using original value: {}", value.getClass().getName());
            return value;
        }
    }
    
    /**
     * Creates an empty SDO DataObject for the given Thrift class.
     *
     * @param thriftClass the Thrift class
     * @return the empty SDO DataObject
     * @throws ThriftSDODataHandlerException if creation fails
     */
    private EDataObject createEmptySDO(Class<? extends TBase> thriftClass) 
            throws ThriftSDODataHandlerException {
        
        try {
            EClass eClass = getOrCreateEClass(thriftClass);
            return (EDataObject) EcoreFactory.eINSTANCE.create(eClass);
        } catch (Exception e) {
            logger.error("Failed to create empty SDO for class: {}", thriftClass.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.SDO_PROCESSING_ERROR,
                "Failed to create empty SDO for class: " + thriftClass.getName(),
                "Thrift class: " + thriftClass.getName(),
                e
            );
        }
    }
    
    /**
     * Gets or creates an EClass for the given Thrift class.
     *
     * @param thriftClass the Thrift class
     * @return the corresponding EClass
     * @throws ThriftSDODataHandlerException if creation fails
     */
    private EClass getOrCreateEClass(Class<? extends TBase> thriftClass) 
            throws ThriftSDODataHandlerException {
        
        String className = thriftClass.getName();
        return eclassCache.computeIfAbsent(className, k -> {
            try {
                return createEClassFromThrift(thriftClass);
            } catch (Exception e) {
                logger.error("Failed to create EClass for: {}", className, e);
                throw new RuntimeException("Failed to create EClass for: " + className, e);
            }
        });
    }
    
    /**
     * Creates an EClass from a Thrift class definition.
     *
     * @param thriftClass the Thrift class
     * @return the created EClass
     * @throws Exception if creation fails
     */
    private EClass createEClassFromThrift(Class<? extends TBase> thriftClass) throws Exception {
        EClass eClass = EcoreFactory.eINSTANCE.createEClass();
        eClass.setName(StringUtils.substringAfterLast(thriftClass.getName(), "."));
        
        // Get field metadata and create EAttributes/EReferences
        Map<?, ?> fieldMetaData = TypeMapper.getFieldMetaData(thriftClass);
        
        for (Map.Entry<?, ?> entry : fieldMetaData.entrySet()) {
            FieldMetaData metaData = (FieldMetaData) entry.getValue();
            
            if (TypeMapper.isBaseType(metaData.valueMetaData.type)) {
                // Create EAttribute for base types
                EAttribute attribute = EcoreFactory.eINSTANCE.createEAttribute();
                attribute.setName(metaData.fieldName);
                attribute.setEType(getEDataTypeForThriftType(metaData.valueMetaData.type));
                eClass.getEStructuralFeatures().add(attribute);
            } else if (TypeMapper.isStructType(metaData.valueMetaData.type)) {
                // Create EReference for struct types
                EReference reference = EcoreFactory.eINSTANCE.createEReference();
                reference.setName(metaData.fieldName);
                reference.setEType(getOrCreateEClass(metaData.valueMetaData.structClass));
                eClass.getEStructuralFeatures().add(reference);
            }
        }
        
        return eClass;
    }
    
    /**
     * Creates a generic SDO DataObject from JSON when struct class is not available.
     *
     * @param structNode the JSON node
     * @param structName the struct name
     * @return the created SDO DataObject
     * @throws ThriftSDODataHandlerException if creation fails
     */
    private EDataObject createGenericSDOFromJson(JsonNode structNode, String structName) 
            throws ThriftSDODataHandlerException {
        
        try {
            EClass eClass = EcoreFactory.eINSTANCE.createEClass();
            eClass.setName(structName);
            
            // Add attributes based on JSON fields
            for (Map.Entry<String, JsonNode> entry : structNode.fields()) {
                String fieldName = entry.getKey();
                EAttribute attribute = EcoreFactory.eINSTANCE.createEAttribute();
                attribute.setName(fieldName);
                attribute.setEType(EcoreFactory.eINSTANCE.getEString()); // Default to String
                eClass.getEStructuralFeatures().add(attribute);
            }
            
            EDataObject dataObject = (EDataObject) EcoreFactory.eINSTANCE.create(eClass);
            
            // Set values
            for (Map.Entry<String, JsonNode> entry : structNode.fields()) {
                String fieldName = entry.getKey();
                JsonNode valueNode = entry.getValue();
                
                Object value = null;
                if (!valueNode.isNull()) {
                    if (valueNode.isTextual()) {
                        value = valueNode.asText();
                    } else if (valueNode.isBoolean()) {
                        value = valueNode.asBoolean();
                    } else if (valueNode.isInt()) {
                        value = valueNode.asInt();
                    } else if (valueNode.isLong()) {
                        value = valueNode.asLong();
                    } else if (valueNode.isDouble()) {
                        value = valueNode.asDouble();
                    }
                }
                
                setSDOFieldValue(dataObject, fieldName, value);
            }
            
            return dataObject;
            
        } catch (Exception e) {
            logger.error("Failed to create generic SDO from JSON: {}", structName, e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.SDO_PROCESSING_ERROR,
                "Failed to create generic SDO from JSON: " + structName,
                "Struct name: " + structName,
                e
            );
        }
    }
    
    /**
     * Gets the appropriate EDataType for a Thrift type.
     *
     * @param thriftType the Thrift type
     * @return the corresponding EDataType
     */
    private org.eclipse.emf.ecore.EDataType getEDataTypeForThriftType(byte thriftType) {
        Class<?> javaType = TypeMapper.mapThriftToSDO(thriftType);
        if (javaType != null) {
            org.eclipse.emf.ecore.EDataType eDataType = EcoreFactory.eINSTANCE.createEDataType();
            eDataType.setInstanceClassName(javaType.getName());
            return eDataType;
        }
        
        // Default to String type
        org.eclipse.emf.ecore.EDataType eDataType = EcoreFactory.eINSTANCE.createEDataType();
        eDataType.setInstanceClassName(String.class.getName());
        return eDataType;
    }
    
    /**
     * Clears all caches. Useful for testing or memory management.
     */
    public void clearCaches() {
        eclassCache.clear();
        TypeMapper.clearCaches();
    }
    
    /**
     * Gets cache statistics for monitoring purposes.
     *
     * @return a map containing cache statistics
     */
    public Map<String, Integer> getCacheStatistics() {
        Map<String, Integer> stats = new java.util.HashMap<>();
        stats.put("eclassCacheSize", eclassCache.size());
        stats.putAll(TypeMapper.getCacheStatistics());
        return stats;
    }
}