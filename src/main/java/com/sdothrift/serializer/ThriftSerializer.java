package com.sdothrift.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

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
            switch (configuration.getThriftProtocol()) {
                case JSON:
                    return serializeToJson(thriftObject);
                case SIMPLE_JSON:
                    return serializeToSimpleJson(thriftObject);
                case BINARY:
                case COMPACT:
                default:
                    // For binary protocols, we'll convert to JSON for easier processing
                    return serializeToJson(thriftObject);
            }
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
     * Serializes a Thrift object to JSON format.
     *
     * @param thriftObject the Thrift object to serialize
     * @return the JSON string
     * @throws TException if serialization fails
     */
    private String serializeToJson(TBase thriftObject) throws TException {
        TMemoryBuffer transport = new TMemoryBuffer(configuration.getBufferSize());
        TProtocol protocol = new TJSONProtocol(transport);
        thriftObject.write(protocol);
        return new String(transport.getArray(), 0, transport.length(), StandardCharsets.UTF_8);
    }
    
    /**
     * Serializes a Thrift object to Simple JSON format.
     *
     * @param thriftObject the Thrift object to serialize
     * @return the simple JSON string
     * @throws TException if serialization fails
     */
    private String serializeToSimpleJson(TBase thriftObject) throws TException {
        TMemoryBuffer transport = new TMemoryBuffer(configuration.getBufferSize());
        TProtocol protocol = new TSimpleJSONProtocol(transport);
        thriftObject.write(protocol);
        return new String(transport.getArray(), 0, transport.length(), StandardCharsets.UTF_8);
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
            return transport.getArray();
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
        
        try {
            switch (configuration.getThriftProtocol()) {
                case JSON:
                    return deserializeFromJson(serializedData, targetClass);
                case SIMPLE_JSON:
                    return deserializeFromSimpleJson(serializedData, targetClass);
                case BINARY:
                case COMPACT:
                default:
                    // For binary protocols, try JSON as fallback
                    return deserializeFromJson(serializedData, targetClass);
            }
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
        TMemoryInputTransport transport = new TMemoryInputTransport(jsonData.getBytes(StandardCharsets.UTF_8));
        TProtocol protocol = new TJSONProtocol(transport);
        T thriftObject = targetClass.getDeclaredConstructor().newInstance();
        thriftObject.read(protocol);
        return thriftObject;
    }
    
    /**
     * Deserializes a Thrift object from Simple JSON format.
     *
     * @param jsonData the simple JSON string
     * @param targetClass the target Thrift class
     * @param <T> the type of the Thrift object
     * @return the deserialized Thrift object
     * @throws Exception if deserialization fails
     */
    private <T extends TBase> T deserializeFromSimpleJson(String jsonData, Class<T> targetClass) throws Exception {
        TMemoryInputTransport transport = new TMemoryInputTransport(jsonData.getBytes(StandardCharsets.UTF_8));
        TProtocol protocol = new TSimpleJSONProtocol(transport);
        T thriftObject = targetClass.getDeclaredConstructor().newInstance();
        thriftObject.read(protocol);
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
            } else {
                // Try to convert using ObjectMapper as fallback
                return objectMapper.writeValueAsString(input);
            }
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