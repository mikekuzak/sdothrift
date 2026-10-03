package com.sdothrift.transformer;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transformer class for converting SDO DataObjects to Thrift objects.
 * Maps SDO directly to Thrift via the target's Thrift metadata (target metadata
 * field -> SDO feature lookup -> presence/value -> recursive conversion ->
 * setFieldValue) without routing through wire serialization.
 */
public class SDOToThriftTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(SDOToThriftTransformer.class);
    
    private final ThriftSDOConfiguration configuration;
    
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
    }
    
    /**
     * Transforms an SDO DataObject to a Thrift object using direct metadata mapping.
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
            T thriftObject = createThriftInstance(targetThriftClass);
            
            Map<?, ?> thriftFields = getThriftFieldMetaData(targetThriftClass);
            EClass sdoClass = dataObject.eClass();
            
            for (Map.Entry<?, ?> entry : thriftFields.entrySet()) {
                FieldMetaData metaData = (FieldMetaData) entry.getValue();
                TFieldIdEnum fieldId = (TFieldIdEnum) entry.getKey();
                
                EStructuralFeature feature = sdoClass.getEStructuralFeature(metaData.fieldName);
                
                if (feature == null) {
                    if (metaData.requirementType == TFieldRequirementType.REQUIRED) {
                        throw new IllegalArgumentException("Required field missing in SDO schema: " 
                            + metaData.fieldName);
                    }
                    continue;
                }
                
                if (dataObject.eIsSet(feature)) {
                    Object sdoValue = dataObject.eGet(feature);
                    if (sdoValue == null) {
                        applyFieldNullPolicy(thriftObject, fieldId, metaData);
                    } else {
                        Object thriftValue = convertSDOValueToThrift(sdoValue, metaData.valueMetaData);
                        setThriftFieldValue(thriftObject, fieldId, metaData.fieldName, thriftValue);
                    }
                } else {
                    applyFieldNullPolicy(thriftObject, fieldId, metaData);
                }
            }
            
            return thriftObject;
            
        } catch (ThriftSDODataHandlerException e) {
            throw e;
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
     * Creates a new instance of the target Thrift class via the cached constructor.
     *
     * @param thriftClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return a new Thrift instance
     * @throws ThriftSDODataHandlerException if instantiation fails
     */
    private <T extends TBase> T createThriftInstance(Class<T> thriftClass) 
            throws ThriftSDODataHandlerException {
        try {
            return getThriftConstructor(thriftClass).newInstance();
        } catch (Exception e) {
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.THRIFT_PROCESSING_ERROR,
                "Failed to instantiate Thrift class: " + thriftClass.getName(),
                "Thrift class: " + thriftClass.getName(),
                e
            );
        }
    }
    
    /**
     * Sets a field value on a Thrift object. Fails loudly on failure; never
     * logs-and-continues.
     *
     * @param thriftObject the Thrift object
     * @param fieldId the field identifier
     * @param fieldName the field name (for error reporting)
     * @param value the value to set
     */
    private void setThriftFieldValue(TBase thriftObject, TFieldIdEnum fieldId, 
            String fieldName, Object value) {
        try {
            thriftObject.setFieldValue(fieldId, value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to set Thrift field value: " + fieldName, e);
        }
    }
    
    /**
     * Applies the configured null policy for an unset or null SDO field.
     *
     * @param thriftObject the Thrift object
     * @param fieldId the field identifier
     * @param metaData the field metadata
     */
    private void applyFieldNullPolicy(TBase thriftObject, TFieldIdEnum fieldId, FieldMetaData metaData) {
        switch (configuration.getNullHandlingStrategy()) {
            case ERROR:
                throw new IllegalArgumentException("Null value encountered for field: " + metaData.fieldName);
            case DEFAULT:
                setThriftFieldValue(thriftObject, fieldId, metaData.fieldName, 
                    getDefaultValueForThriftType(metaData.valueMetaData));
                break;
            case PRESERVE:
            case OMIT:
            default:
                // Leave the field unset
                break;
        }
    }
    
    /**
     * Gets the default value for a Thrift type.
     *
     * @param metaData the value metadata
     * @return the default value
     */
    private Object getDefaultValueForThriftType(FieldValueMetaData metaData) {
        if (metaData instanceof ListMetaData) {
            return new java.util.ArrayList<>();
        }
        if (metaData instanceof SetMetaData) {
            return new LinkedHashSet<>();
        }
        if (metaData instanceof MapMetaData) {
            return new LinkedHashMap<>();
        }
        if (metaData instanceof StructMetaData) {
            return null; // Do not recursively invent nested structs
        }
        switch (metaData.type) {
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
            default:
                return null;
        }
    }
    
    /**
     * Converts an SDO value to its Thrift representation, recursing on the
     * element/key/value metadata (never on the parent container type).
     *
     * @param value the SDO value
     * @param metaData the value metadata describing the target
     * @return the Thrift representation
     */
    @SuppressWarnings("unchecked")
    private Object convertSDOValueToThrift(Object value, FieldValueMetaData metaData) 
            throws ThriftSDODataHandlerException {
        if (value == null) {
            return null;
        }
        
        if (metaData instanceof StructMetaData) {
            if (!(value instanceof EDataObject)) {
                throw new IllegalArgumentException("Expected EDataObject for struct field but found: " 
                    + value.getClass().getName());
            }
            return transformToThrift((EDataObject) value, 
                (Class<? extends TBase>) ((StructMetaData) metaData).structClass);
        }
        
        if (metaData instanceof ListMetaData) {
            if (!(value instanceof List)) {
                throw new IllegalArgumentException("Expected List for list field but found: " 
                    + value.getClass().getName());
            }
            List<Object> thriftList = new java.util.ArrayList<>();
            for (Object element : (List<?>) value) {
                thriftList.add(convertSDOValueToThrift(element, ((ListMetaData) metaData).elemMetaData));
            }
            return thriftList;
        }
        
        if (metaData instanceof SetMetaData) {
            // SDO models sets as lists; reconstruct the Java Set
            if (!(value instanceof List) && !(value instanceof java.util.Set)) {
                throw new IllegalArgumentException("Expected collection for set field but found: " 
                    + value.getClass().getName());
            }
            Set<Object> thriftSet = new LinkedHashSet<>();
            for (Object element : (java.util.Collection<?>) value) {
                thriftSet.add(convertSDOValueToThrift(element, ((SetMetaData) metaData).elemMetaData));
            }
            return thriftSet;
        }
        
        if (metaData instanceof MapMetaData) {
            // Maps are represented as lists of PropertiesEntry{key,value} SDO objects
            if (!(value instanceof List)) {
                throw new IllegalArgumentException("Expected entry list for map field but found: " 
                    + value.getClass().getName());
            }
            MapMetaData mapMetaData = (MapMetaData) metaData;
            Map<Object, Object> thriftMap = new LinkedHashMap<>();
            for (Object entryObject : (List<?>) value) {
                if (!(entryObject instanceof EDataObject)) {
                    throw new IllegalArgumentException("Expected EDataObject map entry but found: " 
                        + (entryObject == null ? "null" : entryObject.getClass().getName()));
                }
                EDataObject entry = (EDataObject) entryObject;
                thriftMap.put(
                    convertSDOValueToThrift(getEntryFeatureValue(entry, "key"), mapMetaData.keyMetaData),
                    convertSDOValueToThrift(getEntryFeatureValue(entry, "value"), mapMetaData.valueMetaData));
            }
            return thriftMap;
        }

        if (metaData instanceof EnumMetaData) {
            return TypeMapper.enumFromValue(((EnumMetaData) metaData).enumClass,
                ((Number) value).intValue());
        }
        if (metaData.type == TType.UUID) {
            return java.util.UUID.fromString(value.toString());
        }
        if (metaData.type == TType.STRING && metaData.isBinary()) {
            return Base64.getDecoder().decode(value.toString());
        }
        
        // Base scalar
        Class<?> javaType = TypeMapper.mapThriftToSDO(metaData.type);
        if (javaType == null) {
            throw new IllegalArgumentException("Unsupported Thrift field type: " + metaData.type);
        }
        return TypeMapper.convertValue(value, javaType);
    }
    
    private Object getEntryFeatureValue(EDataObject entry, String featureName) {
        EStructuralFeature feature = entry.eClass().getEStructuralFeature(featureName);
        if (feature == null) {
            throw new IllegalArgumentException("Map entry feature not found: " + featureName);
        }
        return entry.eGet(feature);
    }
    
    /**
     * Gets the Thrift field metadata for a class, using caching for performance.
     *
     * @param thriftClass the Thrift class
     * @return the field metadata map
     * @throws ThriftSDODataHandlerException if the metadata cannot be read
     */
    private Map<?, ?> getThriftFieldMetaData(Class<? extends TBase> thriftClass) 
            throws ThriftSDODataHandlerException {
        String className = thriftClass.getName();
        Map<?, ?> cached = fieldMetaDataCache.get(className);
        if (cached != null) {
            return cached;
        }
        try {
            java.lang.reflect.Field metaDataField = thriftClass.getField("metaDataMap");
            Map<?, ?> metaData = (Map<?, ?>) metaDataField.get(null);
            fieldMetaDataCache.put(className, metaData);
            return metaData;
        } catch (Exception e) {
            logger.error("Failed to get field metadata for class: {}", className, e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.THRIFT_PROCESSING_ERROR,
                "Failed to get field metadata for class: " + className,
                "Thrift class: " + className,
                e
            );
        }
    }
    
    /**
     * Validates if the SDO DataObject can be transformed to the target Thrift class.
     * Each REQUIRED Thrift field is verified by NAME: the SDO feature must exist,
     * its value/presence must be acceptable (DEFAULT policy considered), and the
     * value must convert cleanly. Valid zero/false/empty-string values are accepted.
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
            Map<?, ?> thriftFields = getThriftFieldMetaData(targetThriftClass);
            
            for (Object value : thriftFields.values()) {
                if (!(value instanceof FieldMetaData)) {
                    continue;
                }
                FieldMetaData metaData = (FieldMetaData) value;
                if (metaData.requirementType != TFieldRequirementType.REQUIRED) {
                    continue;
                }
                if (!isRequiredFieldSatisfied(dataObject, metaData)) {
                    return false;
                }
            }
            
            return true;
            
        } catch (Exception e) {
            logger.warn("Validation failed for transformation: {} -> {}", 
                dataObject.eClass().getName(), targetThriftClass.getName(), e);
            return false;
        }
    }
    
    /**
     * Checks a single REQUIRED Thrift field against the SDO by name.
     *
     * @param dataObject the SDO DataObject
     * @param metaData the required field metadata
     * @return true if the field is satisfied
     */
    private boolean isRequiredFieldSatisfied(EDataObject dataObject, FieldMetaData metaData) {
        EStructuralFeature feature = dataObject.eClass().getEStructuralFeature(metaData.fieldName);
        if (feature == null) {
            return false;
        }
        
        if (dataObject.eIsSet(feature)) {
            Object sdoValue = dataObject.eGet(feature);
            if (sdoValue == null) {
                // Acceptable only if the DEFAULT policy will supply a value
                return configuration.getNullHandlingStrategy() == ThriftSDOConfiguration.NullHandlingStrategy.DEFAULT;
            }
            try {
                convertSDOValueToThrift(sdoValue, metaData.valueMetaData);
                return true;
            } catch (Exception e) {
                logger.debug("Required field '{}' cannot be converted: {}", metaData.fieldName, e.getMessage());
                return false;
            }
        }
        
        // Unset required field: acceptable only under DEFAULT policy
        return configuration.getNullHandlingStrategy() == ThriftSDOConfiguration.NullHandlingStrategy.DEFAULT;
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
        Constructor<? extends TBase> cached = constructorCache.get(className);
        if (cached != null) {
            return (Constructor<T>) cached;
        }
        try {
            Constructor<? extends TBase> constructor = 
                (Constructor<? extends TBase>) thriftClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            constructorCache.put(className, constructor);
            return (Constructor<T>) constructor;
        } catch (Exception e) {
            logger.error("Failed to get constructor for Thrift class: {}", className, e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.THRIFT_PROCESSING_ERROR,
                "Failed to get constructor for Thrift class: " + className,
                "Thrift class: " + className,
                e
            );
        }
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
     * Gets cache statistics for monitoring purposes. Global TypeMapper statistics
     * are merged first so the local "fieldMetaDataCacheSize" key is preserved.
     *
     * @return a map containing cache statistics
     */
    public Map<String, Integer> getCacheStatistics() {
        Map<String, Integer> stats = new java.util.HashMap<>();
        stats.putAll(TypeMapper.getCacheStatistics());
        stats.put("constructorCacheSize", constructorCache.size());
        stats.put("fieldMetaDataCacheSize", fieldMetaDataCache.size());
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
