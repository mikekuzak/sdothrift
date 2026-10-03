package com.sdothrift.transformer;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.ETypedElement;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.eclipse.emf.ecore.sdo.impl.DynamicEDataObjectImpl;
import org.eclipse.emf.ecore.sdo.util.SDOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transformer class for converting Thrift objects to SDO DataObjects.
 * Maps Thrift metadata directly to SDO (metadata field -> isSet/getFieldValue ->
 * recursive conversion -> eSet) without routing through wire serialization.
 */
public class ThriftToSDOTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(ThriftToSDOTransformer.class);
    
    private final ThriftSDOConfiguration configuration;
    private final EPackage dynamicEPackage;
    
    // Cache for generated EClasses to improve performance
    private final Map<String, EClass> eclassCache = new ConcurrentHashMap<>();
    
    // Cache for generated map-entry EClasses (per key/value type signature)
    private final Map<String, EClass> mapEntryCache = new ConcurrentHashMap<>();
    
    // Lock guarding two-phase EClass creation so recursive Thrift types work
    private final Object schemaLock = new Object();
    
    /**
     * Constructs a new ThriftToSDOTransformer with the given configuration.
     *
     * @param configuration the configuration to use
     */
    public ThriftToSDOTransformer(ThriftSDOConfiguration configuration) {
        this.configuration = configuration;
        this.dynamicEPackage = createDynamicEPackage();
    }
    
    private EPackage createDynamicEPackage() {
        EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
        ePackage.setName("sdothrift.dynamic");
        ePackage.setNsPrefix("sdothrift");
        ePackage.setNsURI("http://sdothrift/dynamic");
        ePackage.setEFactoryInstance(new DynamicEDataObjectImpl.FactoryImpl());
        return ePackage;
    }
    
    /**
     * Transforms a Thrift object to an SDO DataObject using direct metadata mapping.
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
            @SuppressWarnings("unchecked")
            Class<? extends TBase> thriftClass = (Class<? extends TBase>) thriftObject.getClass();
            
            EClass eClass = getOrCreateEClass(thriftClass);
            EDataObject dataObject = SDOUtil.create(eClass);
            
            Map<?, ?> fieldMetaData = TypeMapper.getFieldMetaData(thriftClass);
            
            for (Map.Entry<?, ?> entry : fieldMetaData.entrySet()) {
                FieldMetaData metaData = (FieldMetaData) entry.getValue();
                TFieldIdEnum fieldId = (TFieldIdEnum) entry.getKey();
                
                if (thriftObject.isSet(fieldId)) {
                    Object thriftValue = thriftObject.getFieldValue(fieldId);
                    Object sdoValue = convertThriftValueToSDO(thriftValue, metaData.valueMetaData);
                    setSDOFieldValue(dataObject, metaData.fieldName, sdoValue);
                } else {
                    handleNullField(dataObject, metaData.fieldName, metaData.valueMetaData.type);
                }
            }
            
            return dataObject;
            
        } catch (ThriftSDODataHandlerException e) {
            throw e;
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
     * Converts a Thrift field value to its SDO representation, recursing on the
     * element/key/value metadata (never on the parent container type).
     *
     * @param value the Thrift field value
     * @param metaData the value metadata describing the value
     * @return the SDO representation
     */
    private Object convertThriftValueToSDO(Object value, FieldValueMetaData metaData) 
            throws ThriftSDODataHandlerException {
        if (value == null) {
            return null;
        }
        
        if (metaData instanceof StructMetaData) {
            if (!(value instanceof TBase)) {
                throw new IllegalArgumentException("Expected TBase for struct field but found: " 
                    + value.getClass().getName());
            }
            return transformToSDO((TBase) value);
        }
        
        if (metaData instanceof ListMetaData) {
            if (!(value instanceof List)) {
                throw new IllegalArgumentException("Expected List for list field but found: " 
                    + value.getClass().getName());
            }
            List<Object> sdoList = new java.util.ArrayList<>();
            for (Object element : (List<?>) value) {
                sdoList.add(convertThriftValueToSDO(element, ((ListMetaData) metaData).elemMetaData));
            }
            return sdoList;
        }
        
        if (metaData instanceof SetMetaData) {
            if (!(value instanceof java.util.Set)) {
                throw new IllegalArgumentException("Expected Set for set field but found: " 
                    + value.getClass().getName());
            }
            // Sets are represented as SDO lists; uniqueness is modeled in the EClass
            List<Object> sdoList = new java.util.ArrayList<>();
            for (Object element : (java.util.Set<?>) value) {
                sdoList.add(convertThriftValueToSDO(element, ((SetMetaData) metaData).elemMetaData));
            }
            return sdoList;
        }
        
        if (metaData instanceof MapMetaData) {
            if (!(value instanceof Map)) {
                throw new IllegalArgumentException("Expected Map for map field but found: " 
                    + value.getClass().getName());
            }
            MapMetaData mapMetaData = (MapMetaData) metaData;
            EClass entryEClass = getOrCreateMapEntryEClass(mapMetaData);
            List<Object> entries = new java.util.ArrayList<>();
            for (Map.Entry<?, ?> mapEntry : ((Map<?, ?>) value).entrySet()) {
                EDataObject entryObject = SDOUtil.create(entryEClass);
                setSDOFieldValue(entryObject, "key", 
                    convertThriftValueToSDO(mapEntry.getKey(), mapMetaData.keyMetaData));
                setSDOFieldValue(entryObject, "value", 
                    convertThriftValueToSDO(mapEntry.getValue(), mapMetaData.valueMetaData));
                entries.add(entryObject);
            }
            return entries;
        }

        if (metaData instanceof EnumMetaData) {
            return TypeMapper.enumValue(value);
        }
        if (metaData.type == TType.UUID) {
            return value.toString();
        }
        if (metaData.type == TType.STRING && metaData.isBinary()) {
            byte[] bytes;
            if (value instanceof byte[]) {
                bytes = (byte[]) value;
            } else if (value instanceof ByteBuffer) {
                ByteBuffer buffer = ((ByteBuffer) value).duplicate();
                bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
            } else {
                throw new IllegalArgumentException("Expected byte[] or ByteBuffer for binary field but found: "
                    + value.getClass().getName());
            }
            return Base64.getEncoder().encodeToString(bytes);
        }
        
        // Base scalar
        Class<?> sdoClass = TypeMapper.mapThriftToSDO(metaData.type);
        if (sdoClass == null) {
            throw new IllegalArgumentException("Unsupported Thrift field type: " + metaData.type);
        }
        return TypeMapper.convertValue(value, sdoClass);
    }
    
    /**
     * Sets a field value in an SDO DataObject. Fails loudly on missing features
     * or failed eSet; never logs-and-continues.
     *
     * @param dataObject the SDO DataObject
     * @param fieldName the field name
     * @param value the value to set
     */
    private void setSDOFieldValue(EDataObject dataObject, String fieldName, Object value) {
        EStructuralFeature feature = dataObject.eClass().getEStructuralFeature(fieldName);
        if (feature == null) {
            throw new IllegalArgumentException("Feature not found in SDO: " + fieldName);
        }
        try {
            dataObject.eSet(feature, value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to set SDO field value: " + fieldName, e);
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
                preserveNullField(dataObject, fieldName);
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
                preserveNullField(dataObject, fieldName);
        }
    }

    private void preserveNullField(EDataObject dataObject, String fieldName) {
        EStructuralFeature feature = dataObject.eClass().getEStructuralFeature(fieldName);
        if (feature == null) {
            throw new IllegalArgumentException("Feature not found in SDO: " + fieldName);
        }
        // Null collections are absent, not set-to-null; eSet(null) fails for many features.
        if (!feature.isMany()) {
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
                // SDO maps are represented as many-valued lists of entry objects.
                return new java.util.ArrayList<>();
            default:
                return null;
        }
    }
    
    /**
     * Gets or creates an EClass for the given Thrift class using synchronized
     * two-phase creation: the EClass shell is registered in the cache and the
     * shared dynamic package before its features are populated, so recursive
     * Thrift struct types terminate correctly.
     *
     * @param thriftClass the Thrift class
     * @return the corresponding EClass
     * @throws ThriftSDODataHandlerException if creation fails
     */
    private EClass getOrCreateEClass(Class<? extends TBase> thriftClass) 
            throws ThriftSDODataHandlerException {
        
        String className = thriftClass.getName();
        synchronized (schemaLock) {
            EClass existing = eclassCache.get(className);
            if (existing != null) {
                return existing;
            }
            
            // Phase 1: create and register the shell
            EClass eClass = EcoreFactory.eINSTANCE.createEClass();
            eClass.setName(StringUtils.substringAfterLast(thriftClass.getName(), "."));
            addEClassToDynamicPackage(eClass);
            eclassCache.put(className, eClass);
            
            // Phase 2: populate features (may recurse into getOrCreateEClass)
            try {
                populateEClassFeatures(eClass, thriftClass);
            } catch (ThriftSDODataHandlerException e) {
                throw e;
            } catch (Exception e) {
                logger.error("Failed to create EClass for: {}", className, e);
                throw new ThriftSDODataHandlerException(
                    ThriftSDODataHandlerException.ErrorCodes.SDO_PROCESSING_ERROR,
                    "Failed to create EClass for: " + className,
                    "Thrift class: " + className,
                    e
                );
            }
            
            return eClass;
        }
    }
    
    /**
     * Populates an EClass shell with features derived from Thrift field metadata.
     *
     * @param eClass the EClass shell to populate
     * @param thriftClass the Thrift class the features are derived from
     */
    private void populateEClassFeatures(EClass eClass, Class<? extends TBase> thriftClass) 
            throws ThriftSDODataHandlerException {
        Map<?, ?> fieldMetaData = TypeMapper.getFieldMetaData(thriftClass);
        
        for (Map.Entry<?, ?> entry : fieldMetaData.entrySet()) {
            FieldMetaData metaData = (FieldMetaData) entry.getValue();
            EStructuralFeature feature = createFeatureForMetaData(metaData.fieldName, metaData.valueMetaData);
            eClass.getEStructuralFeatures().add(feature);
        }
    }
    
    /**
     * Creates an EAttribute or containment EReference for the given value metadata.
     * Collections are modeled per the shared SDO contract:
     * LIST = many-valued ordered non-unique; SET = many-valued unique;
     * MAP = many-valued list of containment PropertiesEntry{key,value}.
     *
     * @param name the feature name
     * @param metaData the value metadata
     * @return the structural feature
     */
    private EStructuralFeature createFeatureForMetaData(String name, FieldValueMetaData metaData) 
            throws ThriftSDODataHandlerException {
        if (metaData instanceof StructMetaData) {
            EReference reference = EcoreFactory.eINSTANCE.createEReference();
            reference.setName(name);
            reference.setEType(getOrCreateEClass(((StructMetaData) metaData).structClass));
            reference.setContainment(true);
            return reference;
        }
        
        if (metaData instanceof ListMetaData) {
            EStructuralFeature elementFeature = 
                createFeatureForMetaData(name, ((ListMetaData) metaData).elemMetaData);
            elementFeature.setUpperBound(ETypedElement.UNBOUNDED_MULTIPLICITY);
            elementFeature.setOrdered(true);
            elementFeature.setUnique(false);
            return elementFeature;
        }
        
        if (metaData instanceof SetMetaData) {
            EStructuralFeature elementFeature = 
                createFeatureForMetaData(name, ((SetMetaData) metaData).elemMetaData);
            elementFeature.setUpperBound(ETypedElement.UNBOUNDED_MULTIPLICITY);
            elementFeature.setOrdered(true);
            elementFeature.setUnique(true);
            return elementFeature;
        }
        
        if (metaData instanceof MapMetaData) {
            EReference reference = EcoreFactory.eINSTANCE.createEReference();
            reference.setName(name);
            reference.setEType(getOrCreateMapEntryEClass((MapMetaData) metaData));
            reference.setContainment(true);
            reference.setUpperBound(ETypedElement.UNBOUNDED_MULTIPLICITY);
            reference.setOrdered(true);
            reference.setUnique(false);
            return reference;
        }
        
        // Base scalar attribute; presence matters, so scalars are unsettable
        EAttribute attribute = EcoreFactory.eINSTANCE.createEAttribute();
        attribute.setName(name);
        attribute.setEType(getEDataTypeForThriftType(metaData.type));
        attribute.setUnsettable(true);
        return attribute;
    }
    
    /**
     * Gets or creates the entry EClass for a map field: PropertiesEntry{key,value}
     * with key/value features derived from the MapMetaData key/value metadata.
     *
     * @param mapMetaData the map metadata
     * @return the entry EClass
     */
    private EClass getOrCreateMapEntryEClass(MapMetaData mapMetaData) 
            throws ThriftSDODataHandlerException {
        String signature = mapEntryTypeSignature(mapMetaData.keyMetaData) 
            + "_" + mapEntryTypeSignature(mapMetaData.valueMetaData);
        
        synchronized (schemaLock) {
            EClass existing = mapEntryCache.get(signature);
            if (existing != null) {
                return existing;
            }
            
            EClass entryEClass = EcoreFactory.eINSTANCE.createEClass();
            entryEClass.setName("PropertiesEntry_" + signature);
            addEClassToDynamicPackage(entryEClass);
            mapEntryCache.put(signature, entryEClass);
            
            entryEClass.getEStructuralFeatures().add(
                createFeatureForMetaData("key", mapMetaData.keyMetaData));
            entryEClass.getEStructuralFeatures().add(
                createFeatureForMetaData("value", mapMetaData.valueMetaData));
            
            return entryEClass;
        }
    }
    
    private String mapEntryTypeSignature(FieldValueMetaData metaData) {
        if (metaData instanceof StructMetaData) {
            return ((StructMetaData) metaData).structClass.getSimpleName();
        }
        Class<?> sdoClass = TypeMapper.mapThriftToSDO(metaData.type);
        return sdoClass != null ? sdoClass.getSimpleName() : "Unknown" + metaData.type;
    }
    
    private void addEClassToDynamicPackage(EClass eClass) {
        synchronized (dynamicEPackage) {
            if (eClass.getEPackage() == null) {
                dynamicEPackage.getEClassifiers().add(eClass);
            }
        }
    }
    
    /**
     * Gets the standard boxed Ecore datatype for a Thrift base type.
     *
     * @param thriftType the Thrift type
     * @return the corresponding Ecore datatype
     */
    private EDataType getEDataTypeForThriftType(byte thriftType) {
        switch (thriftType) {
            case TType.BOOL:
                return EcorePackage.eINSTANCE.getEBooleanObject();
            case TType.BYTE:
                return EcorePackage.eINSTANCE.getEByteObject();
            case TType.I16:
                return EcorePackage.eINSTANCE.getEShortObject();
            case TType.I32:
            case TType.ENUM:
                return EcorePackage.eINSTANCE.getEIntegerObject();
            case TType.I64:
                return EcorePackage.eINSTANCE.getELongObject();
            case TType.DOUBLE:
                return EcorePackage.eINSTANCE.getEDoubleObject();
            case TType.STRING:
                return EcorePackage.eINSTANCE.getEString();
            case TType.UUID:
                return EcorePackage.eINSTANCE.getEString();
            default:
                throw new IllegalArgumentException("Unsupported Thrift type for SDO mapping: " + thriftType);
        }
    }
    
    /**
     * Clears all caches. Useful for testing or memory management.
     */
    public void clearCaches() {
        synchronized (schemaLock) {
            eclassCache.clear();
            mapEntryCache.clear();
        }
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
        stats.put("eclassCacheSize", eclassCache.size());
        stats.put("mapEntryCacheSize", mapEntryCache.size());
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
