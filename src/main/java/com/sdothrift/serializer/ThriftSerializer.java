package com.sdothrift.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for serializing and deserializing Thrift objects.
 * Supports multiple Thrift protocols and input/output formats.
 */
public class ThriftSerializer {
    
    private static final Logger logger = LoggerFactory.getLogger(ThriftSerializer.class);
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    
    private final ThriftSDOConfiguration configuration;
    
    /**
     * Constructs a new ThriftSerializer with the given configuration.
     *
     * @param configuration the configuration to use
     */
    public ThriftSerializer(ThriftSDOConfiguration configuration) {
        this.configuration = configuration;
    }
    
    /**
     * Serializes a Thrift object to a string representation.
     *
     * @param thriftObject the Thrift object to serialize
     * @return the serialized string representation
     * @throws ThriftSDODataHandlerException if serialization fails
     */
    public String serializeToString(TBase thriftObject) throws ThriftSDODataHandlerException {
        if (thriftObject == null) {
            return null;
        }
        
        try {
            return objectMapper.writeValueAsString(serializeStructToJson(thriftObject));
        } catch (Exception e) {
            logger.error("Failed to serialize Thrift object: {}", thriftObject.getClass().getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.SERIALIZATION_ERROR,
                "Failed to serialize Thrift object: " + thriftObject.getClass().getName(),
                "Protocol: " + configuration.getThriftProtocol(),
                e
            );
        }
    }
    
    /**
     * Serializes a Thrift object to bytes using the configured protocol.
     *
     * @param thriftObject the Thrift object to serialize
     * @return the serialized bytes
     * @throws ThriftSDODataHandlerException if serialization fails
     */
    public byte[] serializeToBytes(TBase thriftObject) throws ThriftSDODataHandlerException {
        if (thriftObject == null) {
            return null;
        }
        
        try {
            TMemoryBuffer transport = new TMemoryBuffer(configuration.getBufferSize());
            TProtocol protocol = createProtocol(transport);
            thriftObject.write(protocol);
            return java.util.Arrays.copyOf(transport.getArray(), transport.length());
        } catch (Exception e) {
            logger.error("Failed to serialize Thrift object to bytes: {}", thriftObject.getClass().getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.SERIALIZATION_ERROR,
                "Failed to serialize Thrift object to bytes: " + thriftObject.getClass().getName(),
                "Protocol: " + configuration.getThriftProtocol(),
                e
            );
        }
    }
    
    /**
     * Deserializes a Thrift object from a string representation.
     *
     * @param serializedData the serialized string data
     * @param targetClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return the deserialized Thrift object
     * @throws ThriftSDODataHandlerException if deserialization fails
     */
    public <T extends TBase> T deserializeFromString(String serializedData, Class<T> targetClass) 
            throws ThriftSDODataHandlerException {
        if (serializedData == null || serializedData.trim().isEmpty()) {
            return null;
        }
        if (targetClass == null) {
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.VALIDATION_ERROR,
                "Target Thrift class cannot be null",
                "A concrete Thrift target class is required"
            );
        }
        
        try {
            return deserializeFromJson(serializedData, targetClass);
        } catch (ThriftSDODataHandlerException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to deserialize Thrift object from string for class: {}", targetClass.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.DESERIALIZATION_ERROR,
                "Failed to deserialize Thrift object from string for class: " + targetClass.getName(),
                "Protocol: " + configuration.getThriftProtocol(),
                e
            );
        }
    }
    
    /**
     * Deserializes a Thrift object from JSON format.
     *
     * @param jsonData the JSON string
     * @param targetClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return the deserialized Thrift object
     * @throws Exception if deserialization fails
     */
    private <T extends TBase> T deserializeFromJson(String jsonData, Class<T> targetClass) throws Exception {
        JsonNode root = objectMapper.readTree(jsonData);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("Thrift field-name JSON root must be an object");
        }

        T thriftObject = instantiate(targetClass);
        Map<String, FieldMetaData> metadataByName = new LinkedHashMap<>();
        Map<?, ?> metadata = getFieldMetaData(targetClass);
        for (Map.Entry<?, ?> entry : metadata.entrySet()) {
            if (!(entry.getKey() instanceof TFieldIdEnum) || !(entry.getValue() instanceof FieldMetaData)) {
                throw new IllegalArgumentException("Unsupported Thrift field metadata in " + targetClass.getName());
            }
            FieldMetaData fieldMetadata = (FieldMetaData) entry.getValue();
            metadataByName.put(fieldMetadata.fieldName, fieldMetadata);
        }

        if (configuration.isStrictValidationEnabled()) {
            java.util.Iterator<String> names = root.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                if (!metadataByName.containsKey(name)) {
                    throw new IllegalArgumentException("Unknown Thrift field: " + name);
                }
            }
        }

        Set<String> providedFields = new LinkedHashSet<>();
        for (Map.Entry<?, ?> entry : metadata.entrySet()) {
            TFieldIdEnum field = (TFieldIdEnum) entry.getKey();
            FieldMetaData fieldMetadata = (FieldMetaData) entry.getValue();
            JsonNode value = root.get(fieldMetadata.fieldName);
            if (value != null && !value.isNull()) {
                Object converted = readValue(value, fieldMetadata.valueMetaData, fieldMetadata.fieldName);
                if (converted != OMIT) {
                    thriftObject.setFieldValue(field, converted);
                    providedFields.add(fieldMetadata.fieldName);
                }
            } else {
                applyNullPolicy(thriftObject, field, fieldMetadata);
            }
        }

        if (configuration.isStrictValidationEnabled()) {
            for (Map.Entry<?, ?> entry : metadata.entrySet()) {
                TFieldIdEnum field = (TFieldIdEnum) entry.getKey();
                FieldMetaData fieldMetadata = (FieldMetaData) entry.getValue();
                if (fieldMetadata.requirementType == TFieldRequirementType.REQUIRED &&
                        (!providedFields.contains(fieldMetadata.fieldName) || !thriftObject.isSet(field))) {
                    throw new IllegalArgumentException("Missing or invalid required Thrift field: " + fieldMetadata.fieldName);
                }
            }
        }
        return thriftObject;
    }
    
    /**
     * Deserializes a Thrift object from bytes using the configured protocol.
     *
     * @param data the serialized bytes
     * @param targetClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return the deserialized Thrift object
     * @throws ThriftSDODataHandlerException if deserialization fails
     */
    public <T extends TBase> T deserializeFromBytes(byte[] data, Class<T> targetClass) 
            throws ThriftSDODataHandlerException {
        if (data == null || data.length == 0) {
            return null;
        }
        if (targetClass == null) {
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.VALIDATION_ERROR,
                "Target Thrift class cannot be null",
                "A concrete Thrift target class is required"
            );
        }
        if (configuration.getThriftProtocol() == ThriftSDOConfiguration.ThriftProtocol.SIMPLE_JSON) {
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.DESERIALIZATION_ERROR,
                "SIMPLE_JSON protocol does not support reading",
                "TSimpleJSONProtocol is write-only"
            );
        }
        
        try {
            TMemoryInputTransport transport = new TMemoryInputTransport(data);
            TProtocol protocol = createProtocol(transport);
            T thriftObject = targetClass.getDeclaredConstructor().newInstance();
            thriftObject.read(protocol);
            return thriftObject;
        } catch (Exception e) {
            logger.error("Failed to deserialize Thrift object from bytes for class: {}", targetClass.getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.DESERIALIZATION_ERROR,
                "Failed to deserialize Thrift object from bytes for class: " + targetClass.getName(),
                "Protocol: " + configuration.getThriftProtocol(),
                e
            );
        }
    }
    
    /**
     * Creates a protocol instance based on the configuration.
     *
     * @param transport the transport to use
     * @return the configured protocol
     */
    private TProtocol createProtocol(org.apache.thrift.transport.TTransport transport) {
        switch (configuration.getThriftProtocol()) {
            case BINARY:
                return new TBinaryProtocol(transport);
            case COMPACT:
                return new TCompactProtocol(transport);
            case JSON:
                return new TJSONProtocol(transport);
            case SIMPLE_JSON:
                return new TSimpleJSONProtocol(transport);
            default:
                return new TBinaryProtocol(transport);
        }
    }

    private static final Object OMIT = new Object();

    private ObjectNode serializeStructToJson(TBase thriftObject) throws Exception {
        ObjectNode node = objectMapper.createObjectNode();
        Map<?, ?> metadata = getFieldMetaData(thriftObject.getClass());
        for (Map.Entry<?, ?> entry : metadata.entrySet()) {
            if (!(entry.getKey() instanceof TFieldIdEnum) || !(entry.getValue() instanceof FieldMetaData)) {
                throw new IllegalArgumentException("Unsupported Thrift field metadata in " + thriftObject.getClass().getName());
            }
            TFieldIdEnum field = (TFieldIdEnum) entry.getKey();
            FieldMetaData fieldMetadata = (FieldMetaData) entry.getValue();
            if (thriftObject.isSet(field)) {
                Object value = thriftObject.getFieldValue(field);
                JsonNode jsonValue = writeValue(value, fieldMetadata.valueMetaData, fieldMetadata.fieldName);
                if (jsonValue != null) {
                    node.set(fieldMetadata.fieldName, jsonValue);
                }
            }
        }
        return node;
    }

    private JsonNode writeValue(Object value, FieldValueMetaData metadata, String path) throws Exception {
        rejectUnsupportedMetadata(metadata, path);
        if (value == null) {
            return objectMapper.getNodeFactory().nullNode();
        }
        switch (metadata.type) {
            case TType.BOOL:
                if (!(value instanceof Boolean)) throw invalidShape(path, "Boolean");
                return objectMapper.getNodeFactory().booleanNode((Boolean) value);
            case TType.BYTE:
                if (!(value instanceof Byte)) throw invalidShape(path, "Byte");
                return objectMapper.getNodeFactory().numberNode((Byte) value);
            case TType.I16:
                if (!(value instanceof Short)) throw invalidShape(path, "Short");
                return objectMapper.getNodeFactory().numberNode((Short) value);
            case TType.I32:
                if (!(value instanceof Integer)) throw invalidShape(path, "Integer");
                return objectMapper.getNodeFactory().numberNode((Integer) value);
            case TType.I64:
                if (!(value instanceof Long)) throw invalidShape(path, "Long");
                return objectMapper.getNodeFactory().numberNode((Long) value);
            case TType.DOUBLE:
                if (!(value instanceof Double)) throw invalidShape(path, "Double");
                return objectMapper.getNodeFactory().numberNode((Double) value);
            case TType.STRING:
                if (!(value instanceof String)) throw invalidShape(path, "String (binary fields are unsupported)");
                return objectMapper.getNodeFactory().textNode((String) value);
            case TType.STRUCT:
                if (!(metadata instanceof StructMetaData) || !(value instanceof TBase)) {
                    throw invalidShape(path, "Thrift struct");
                }
                return serializeStructToJson((TBase) value);
            case TType.LIST: {
                if (!(metadata instanceof ListMetaData) || !(value instanceof Iterable)) {
                    throw invalidShape(path, "Thrift list");
                }
                ArrayNode array = objectMapper.createArrayNode();
                for (Object item : (Iterable<?>) value) {
                    array.add(writeValue(item, ((ListMetaData) metadata).elemMetaData, path + "[]"));
                }
                return array;
            }
            case TType.SET: {
                if (!(metadata instanceof SetMetaData) || !(value instanceof Iterable)) {
                    throw invalidShape(path, "Thrift set");
                }
                ArrayNode array = objectMapper.createArrayNode();
                for (Object item : (Iterable<?>) value) {
                    array.add(writeValue(item, ((SetMetaData) metadata).elemMetaData, path + "[]"));
                }
                return array;
            }
            case TType.MAP: {
                if (!(metadata instanceof MapMetaData) || !(value instanceof Map)) {
                    throw invalidShape(path, "Thrift map");
                }
                MapMetaData mapMetadata = (MapMetaData) metadata;
                if (mapMetadata.keyMetaData.type != TType.STRING) {
                    throw new IllegalArgumentException("Unsupported non-string map key metadata at " + path);
                }
                ObjectNode object = objectMapper.createObjectNode();
                for (Map.Entry<?, ?> mapEntry : ((Map<?, ?>) value).entrySet()) {
                    if (!(mapEntry.getKey() instanceof String)) {
                        throw invalidShape(path, "map with String keys");
                    }
                    object.set((String) mapEntry.getKey(), writeValue(mapEntry.getValue(), mapMetadata.valueMetaData, path + "." + mapEntry.getKey()));
                }
                return object;
            }
            default:
                throw new IllegalArgumentException("Unsupported Thrift metadata type " + metadata.type + " at " + path);
        }
    }

    private Object readValue(JsonNode node, FieldValueMetaData metadata, String path) throws Exception {
        rejectUnsupportedMetadata(metadata, path);
        if (node == null || node.isNull()) {
            return defaultOrNull(metadata, path);
        }
        switch (metadata.type) {
            case TType.BOOL:
                if (!node.isBoolean()) throw invalidJsonShape(path, "boolean");
                return node.booleanValue();
            case TType.BYTE:
                return integralValue(node, BigInteger.valueOf(Byte.MIN_VALUE), BigInteger.valueOf(Byte.MAX_VALUE), path).byteValue();
            case TType.I16:
                return integralValue(node, BigInteger.valueOf(Short.MIN_VALUE), BigInteger.valueOf(Short.MAX_VALUE), path).shortValue();
            case TType.I32:
                return integralValue(node, BigInteger.valueOf(Integer.MIN_VALUE), BigInteger.valueOf(Integer.MAX_VALUE), path).intValue();
            case TType.I64:
                return integralValue(node, BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE), path).longValue();
            case TType.DOUBLE:
                if (!node.isNumber()) throw invalidJsonShape(path, "number");
                double doubleValue = node.doubleValue();
                if (Double.isInfinite(doubleValue) || Double.isNaN(doubleValue)) {
                    throw new IllegalArgumentException("Numeric value out of range at " + path);
                }
                return doubleValue;
            case TType.STRING:
                if (!node.isTextual()) throw invalidJsonShape(path, "string (binary fields are unsupported)");
                return node.textValue();
            case TType.STRUCT: {
                if (!(metadata instanceof StructMetaData)) throw invalidJsonShape(path, "Thrift struct metadata");
                if (!node.isObject()) throw invalidJsonShape(path, "object");
                @SuppressWarnings("unchecked")
                Class<? extends TBase> structClass = ((StructMetaData) metadata).structClass;
                TBase nested = instantiate(structClass);
                populateStruct(node, nested, path);
                return nested;
            }
            case TType.LIST: {
                if (!(metadata instanceof ListMetaData)) throw invalidJsonShape(path, "Thrift list metadata");
                if (!node.isArray()) throw invalidJsonShape(path, "array");
                List<Object> list = new ArrayList<>();
                FieldValueMetaData elementMetadata = ((ListMetaData) metadata).elemMetaData;
                for (JsonNode item : node) {
                    Object converted = readValue(item, elementMetadata, path + "[]");
                    if (converted != OMIT) list.add(converted);
                }
                return list;
            }
            case TType.SET: {
                if (!(metadata instanceof SetMetaData)) throw invalidJsonShape(path, "Thrift set metadata");
                if (!node.isArray()) throw invalidJsonShape(path, "array");
                Set<Object> set = new LinkedHashSet<>();
                FieldValueMetaData elementMetadata = ((SetMetaData) metadata).elemMetaData;
                for (JsonNode item : node) {
                    Object converted = readValue(item, elementMetadata, path + "[]");
                    if (converted != OMIT) set.add(converted);
                }
                return set;
            }
            case TType.MAP: {
                if (!(metadata instanceof MapMetaData)) throw invalidJsonShape(path, "Thrift map metadata");
                if (!node.isObject()) throw invalidJsonShape(path, "object");
                MapMetaData mapMetadata = (MapMetaData) metadata;
                if (mapMetadata.keyMetaData.type != TType.STRING) {
                    throw new IllegalArgumentException("Unsupported non-string map key metadata at " + path);
                }
                Map<String, Object> map = new LinkedHashMap<>();
                java.util.Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    Object converted = readValue(field.getValue(), mapMetadata.valueMetaData, path + "." + field.getKey());
                    if (converted != OMIT) map.put(field.getKey(), converted);
                }
                return map;
            }
            default:
                throw new IllegalArgumentException("Unsupported Thrift metadata type " + metadata.type + " at " + path);
        }
    }

    private void populateStruct(JsonNode root, TBase target, String path) throws Exception {
        Map<?, ?> metadata = getFieldMetaData(target.getClass());
        Map<String, FieldMetaData> fieldsByName = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : metadata.entrySet()) {
            if (!(entry.getKey() instanceof TFieldIdEnum) || !(entry.getValue() instanceof FieldMetaData)) {
                throw new IllegalArgumentException("Unsupported Thrift field metadata in " + target.getClass().getName());
            }
            fieldsByName.put(((FieldMetaData) entry.getValue()).fieldName, (FieldMetaData) entry.getValue());
        }
        if (configuration.isStrictValidationEnabled()) {
            java.util.Iterator<String> names = root.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                if (!fieldsByName.containsKey(name)) throw new IllegalArgumentException("Unknown Thrift field: " + path + "." + name);
            }
        }
        for (Map.Entry<?, ?> entry : metadata.entrySet()) {
            TFieldIdEnum field = (TFieldIdEnum) entry.getKey();
            FieldMetaData fieldMetadata = (FieldMetaData) entry.getValue();
            JsonNode value = root.get(fieldMetadata.fieldName);
            if (value != null && !value.isNull()) {
                Object converted = readValue(value, fieldMetadata.valueMetaData, path + "." + fieldMetadata.fieldName);
                if (converted != OMIT) target.setFieldValue(field, converted);
            } else {
                applyNullPolicy(target, field, fieldMetadata);
            }
        }
        if (configuration.isStrictValidationEnabled()) {
            for (Map.Entry<?, ?> entry : metadata.entrySet()) {
                TFieldIdEnum field = (TFieldIdEnum) entry.getKey();
                FieldMetaData fieldMetadata = (FieldMetaData) entry.getValue();
                JsonNode value = root.get(fieldMetadata.fieldName);
                if (fieldMetadata.requirementType == TFieldRequirementType.REQUIRED &&
                        (value == null || value.isNull() || !target.isSet(field))) {
                    throw new IllegalArgumentException("Missing or invalid required Thrift field: " + path + "." + fieldMetadata.fieldName);
                }
            }
        }
    }

    private void applyNullPolicy(TBase target, TFieldIdEnum field, FieldMetaData metadata) throws Exception {
        switch (configuration.getNullHandlingStrategy()) {
            case ERROR:
                throw new ThriftSDODataHandlerException(
                    ThriftSDODataHandlerException.ErrorCodes.NULL_INPUT_ERROR,
                    "Null value encountered for field: " + metadata.fieldName,
                    "Thrift field is missing or null"
                );
            case DEFAULT:
                Object defaultValue = defaultValue(metadata.valueMetaData, metadata.fieldName);
                if (defaultValue != OMIT) target.setFieldValue(field, defaultValue);
                break;
            case OMIT:
            case PRESERVE:
            default:
                // Leaving the generated field unset preserves null/presence semantics.
                break;
        }
    }

    private Object defaultOrNull(FieldValueMetaData metadata, String path) throws Exception {
        switch (configuration.getNullHandlingStrategy()) {
            case ERROR:
                throw new ThriftSDODataHandlerException(
                    ThriftSDODataHandlerException.ErrorCodes.NULL_INPUT_ERROR,
                    "Null value encountered for field: " + path,
                    "Thrift field or value is null"
                );
            case DEFAULT:
                return defaultValue(metadata, path);
            case OMIT:
                return OMIT;
            case PRESERVE:
            default:
                return null;
        }
    }

    private Object defaultValue(FieldValueMetaData metadata, String path) throws Exception {
        rejectUnsupportedMetadata(metadata, path);
        switch (metadata.type) {
            case TType.BOOL: return Boolean.FALSE;
            case TType.BYTE: return Byte.valueOf((byte) 0);
            case TType.I16: return Short.valueOf((short) 0);
            case TType.I32: return Integer.valueOf(0);
            case TType.I64: return Long.valueOf(0L);
            case TType.DOUBLE: return Double.valueOf(0.0d);
            case TType.STRING: return "";
            case TType.LIST:
                if (!(metadata instanceof ListMetaData)) throw new IllegalArgumentException("Unsupported list metadata at " + path);
                return new ArrayList<>();
            case TType.SET:
                if (!(metadata instanceof SetMetaData)) throw new IllegalArgumentException("Unsupported set metadata at " + path);
                return new LinkedHashSet<>();
            case TType.MAP:
                if (!(metadata instanceof MapMetaData)) throw new IllegalArgumentException("Unsupported map metadata at " + path);
                if (((MapMetaData) metadata).keyMetaData.type != TType.STRING) {
                    throw new IllegalArgumentException("Unsupported non-string map key metadata at " + path);
                }
                return new LinkedHashMap<>();
            case TType.STRUCT:
                // DEFAULT does not fabricate nested structs.
                return OMIT;
            default:
                throw new IllegalArgumentException("Unsupported Thrift metadata type " + metadata.type + " at " + path);
        }
    }

    private BigInteger integralValue(JsonNode node, BigInteger minimum, BigInteger maximum, String path) {
        if (!node.isIntegralNumber()) throw invalidJsonShape(path, "integral number");
        BigInteger value = node.bigIntegerValue();
        if (value.compareTo(minimum) < 0 || value.compareTo(maximum) > 0) {
            throw new IllegalArgumentException("Integer value out of range at " + path);
        }
        return value;
    }

    private IllegalArgumentException invalidJsonShape(String path, String expected) {
        return new IllegalArgumentException("Invalid JSON value for " + path + ": expected " + expected);
    }

    private IllegalArgumentException invalidShape(String path, String expected) {
        return new IllegalArgumentException("Invalid Thrift value for " + path + ": expected " + expected);
    }

    private void rejectUnsupportedMetadata(FieldValueMetaData metadata, String path) {
        if (metadata instanceof EnumMetaData) {
            throw new IllegalArgumentException("Unsupported enum metadata at " + path);
        }
        if (metadata.type == TType.STRING && metadata.isBinary()) {
            throw new IllegalArgumentException("Unsupported binary metadata at " + path);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> getFieldMetaData(Class<?> thriftClass) throws Exception {
        Field metadataField = thriftClass.getField("metaDataMap");
        Object value = metadataField.get(null);
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Thrift metadata map is invalid for " + thriftClass.getName());
        }
        return (Map<?, ?>) value;
    }

    private <T extends TBase> T instantiate(Class<T> thriftClass) throws Exception {
        java.lang.reflect.Constructor<T> constructor = thriftClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }
    
    /**
     * Converts an input object to a string representation.
     * Handles various input types: InputStream, byte[], Reader, String.
     *
     * @param input the input object
     * @return the string representation
     * @throws ThriftSDODataHandlerException if conversion fails
     */
    public String convertInputToString(Object input) throws ThriftSDODataHandlerException {
        if (input == null) {
            return null;
        }
        
        try {
            if (input instanceof String) {
                return (String) input;
            } else if (input instanceof byte[]) {
                return new String((byte[]) input, configuration.getCharacterEncoding());
            } else if (input instanceof InputStream) {
                return readInputStreamToString((InputStream) input);
            } else if (input instanceof Reader) {
                return readReaderToString((Reader) input);
            } else if (input instanceof TBase) {
                return serializeToString((TBase) input);
            } else if (input instanceof Number || input instanceof Boolean || input instanceof java.util.Collection || input instanceof Map) {
                return objectMapper.writeValueAsString(input);
            } else {
                throw new ThriftSDODataHandlerException(
                    ThriftSDODataHandlerException.ErrorCodes.UNSUPPORTED_OPERATION,
                    "Unsupported transformation: " + input.getClass().getName() + " -> String",
                    "Only strings, Thrift objects, scalars, collections, maps, streams and readers can be rendered"
                );
            }
        } catch (ThriftSDODataHandlerException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to convert input to string: {}", input.getClass().getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.CONVERSION_ERROR,
                "Failed to convert input to string: " + input.getClass().getName(),
                "Input type: " + input.getClass().getName(),
                e
            );
        }
    }
    
    /**
     * Reads an InputStream to a string.
     *
     * @param inputStream the input stream
     * @return the string content
     * @throws IOException if reading fails
     */
    private String readInputStreamToString(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[configuration.getBufferSize()];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(configuration.getCharacterEncoding());
    }
    
    /**
     * Reads a Reader to a string.
     *
     * @param reader the reader
     * @return the string content
     * @throws IOException if reading fails
     */
    private String readReaderToString(Reader reader) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        char[] buffer = new char[configuration.getBufferSize()];
        int length;
        while ((length = reader.read(buffer)) != -1) {
            stringBuilder.append(buffer, 0, length);
        }
        return stringBuilder.toString();
    }
    
    /**
     * Validates if the input data is valid JSON.
     *
     * @param jsonData the JSON data to validate
     * @return true if valid JSON, false otherwise
     */
    public boolean isValidJson(String jsonData) {
        if (jsonData == null || jsonData.trim().isEmpty()) {
            return false;
        }
        
        try {
            objectMapper.readTree(jsonData);
            return true;
        } catch (JsonProcessingException e) {
            logger.debug("Invalid JSON detected: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Extracts JSON node from string if it's valid JSON.
     *
     * @param jsonData the JSON data
     * @return the JsonNode, or null if invalid
     */
    public JsonNode parseJson(String jsonData) {
        if (!isValidJson(jsonData)) {
            return null;
        }
        
        try {
            return objectMapper.readTree(jsonData);
        } catch (JsonProcessingException e) {
            logger.debug("Failed to parse JSON: {}", e.getMessage());
            return null;
        }
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
