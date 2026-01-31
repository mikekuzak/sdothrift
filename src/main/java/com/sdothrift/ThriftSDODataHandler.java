package com.sdothrift;

import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.exception.ThriftSDODataHandlerException;
import com.sdothrift.transformer.SDOToThriftTransformer;
import com.sdothrift.transformer.ThriftToSDOTransformer;
import com.sdothrift.serializer.ThriftSerializer;
import org.apache.thrift.TBase;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom IBM Integration Designer data handler for transforming Thrift Objects to SDO DataObjects and vice versa.
 * 
 * This data handler follows the IBM delegation pattern where it pre-processes inputs and delegates
 * to standard IBM data handlers for final transformation when appropriate.
 * 
 * Supports bidirectional transformation:
 * - Thrift Objects → SDO DataObjects
 * - SDO DataObjects → Thrift Objects
 * 
 * Handles various input formats:
 * - InputStream
 * - byte[]
 * - Reader  
 * - String
 * - Direct objects (Thrift or SDO)
 */
public class ThriftSDODataHandler implements commonj.connector.runtime.DataHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(ThriftSDODataHandler.class);
    
    private ThriftSDOConfiguration configuration;
    private ThriftToSDOTransformer thriftToSDOTransformer;
    private SDOToThriftTransformer sdoToThriftTransformer;
    private ThriftSerializer thriftSerializer;
    private Map<String, Object> bindingContext;
    
    /**
     * Default constructor.
     */
    public ThriftSDODataHandler() {
        this(new ThriftSDOConfiguration());
    }
    
    /**
     * Constructor with configuration.
     *
     * @param configuration the configuration to use
     */
    public ThriftSDODataHandler(ThriftSDOConfiguration configuration) {
        this.configuration = configuration;
        this.thriftSerializer = new ThriftSerializer(configuration);
        this.thriftToSDOTransformer = new ThriftToSDOTransformer(configuration);
        this.sdoToThriftTransformer = new SDOToThriftTransformer(configuration);
    }
    
    /**
     * Transforms data from source format to target format.
     * This is the main transformation method following IBM DataHandler pattern.
     *
     * @param source the source data to transform
     * @param targetClass the target class
     * @param options transformation options (can be null)
     * @return the transformed data
     * @throws commonj.connector.runtime.DataHandlerException if transformation fails
     */
    @Override
    public Object transform(Object source, Class<?> targetClass, Object options) 
            throws commonj.connector.runtime.DataHandlerException {
        
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Transforming source {} to target {}", 
                    source != null ? source.getClass().getName() : "null",
                    targetClass != null ? targetClass.getName() : "null");
            }
            
            // Handle null input
            if (source == null) {
                return handleNullInput(targetClass);
            }
            
            // Convert input to appropriate format for processing
            Object processedSource = preprocessInput(source);
            
            // Determine transformation direction
            if (isThriftToSDOTransformation(processedSource, targetClass)) {
                return transformThriftToSDO(processedSource, targetClass, options);
            } else if (isSDOToThriftTransformation(processedSource, targetClass)) {
                return transformSDOToThrift(processedSource, targetClass, options);
            } else {
                // Handle other transformation scenarios
                return handleGenericTransformation(processedSource, targetClass, options);
            }
            
        } catch (ThriftSDODataHandlerException e) {
            logger.error("Thrift-SDO transformation failed", e);
            throw new commonj.connector.runtime.DataHandlerException(e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error during transformation", e);
            throw new commonj.connector.runtime.DataHandlerException(
                "Unexpected error during transformation: " + e.getMessage(), e);
        }
    }
    
    /**
     * Transforms data from source format and writes directly to target object.
     * This method is used for stream-based transformations.
     *
     * @param source the source data
     * @param target the target object to populate
     * @param options transformation options (can be null)
     * @throws commonj.connector.runtime.DataHandlerException if transformation fails
     */
    @Override
    public void transformInto(Object source, Object target, Object options) 
            throws commonj.connector.runtime.DataHandlerException {
        
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Transforming source {} into target {}", 
                    source != null ? source.getClass().getName() : "null",
                    target != null ? target.getClass().getName() : "null");
            }
            
            if (source == null || target == null) {
                return;
            }
            
            // For transformInto, we typically transform to intermediate format and then populate target
            Object transformed = transform(source, target.getClass(), options);
            
            if (transformed != null) {
                copyTransformedData(transformed, target);
            }
            
        } catch (Exception e) {
            logger.error("TransformInto operation failed", e);
            throw new commonj.connector.runtime.DataHandlerException(
                "TransformInto operation failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Sets the binding context for this data handler.
     * The binding context contains configuration and runtime information.
     *
     * @param context the binding context map
     */
    @Override
    public void setBindingContext(Map<String, Object> context) {
        this.bindingContext = context;
        
        // Update configuration from binding context
        if (context != null) {
            ThriftSDOConfiguration newConfig = ThriftSDOConfiguration.fromBindingContext(context);
            updateConfiguration(newConfig);
        }
    }
    
    /**
     * Handles null input based on target class and configuration.
     *
     * @param targetClass the target class
     * @return null or appropriate default value
     * @throws ThriftSDODataHandlerException if null handling is configured to error
     */
    private Object handleNullInput(Class<?> targetClass) throws ThriftSDODataHandlerException {
        switch (configuration.getNullHandlingStrategy()) {
            case ERROR:
                throw new ThriftSDODataHandlerException(
                    ThriftSDODataHandlerException.ErrorCodes.NULL_INPUT_ERROR,
                    "Null input encountered while null handling is set to ERROR",
                    "Target class: " + targetClass.getName()
                );
            case DEFAULT:
                return getDefaultValueForClass(targetClass);
            case PRESERVE:
            case OMIT:
            default:
                return null;
        }
    }
    
    /**
     * Preprocesses input data to a format suitable for transformation.
     * Handles various input types following IBM pattern.
     *
     * @param source the source data
     * @return the processed source data
     * @throws ThriftSDODataHandlerException if preprocessing fails
     */
    private Object preprocessInput(Object source) throws ThriftSDODataHandlerException {
        try {
            if (source instanceof String || source instanceof TBase || source instanceof EDataObject) {
                return source; // Already in suitable format
            } else if (source instanceof InputStream) {
                return processInputStream((InputStream) source);
            } else if (source instanceof byte[]) {
                return processByteArray((byte[]) source);
            } else if (source instanceof Reader) {
                return processReader((Reader) source);
            } else {
                // Try to convert to string as fallback
                return thriftSerializer.convertInputToString(source);
            }
        } catch (Exception e) {
            logger.error("Failed to preprocess input: {}", source.getClass().getName(), e);
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.CONVERSION_ERROR,
                "Failed to preprocess input: " + source.getClass().getName(),
                "Input type: " + source.getClass().getName(),
                e
            );
        }
    }
    
    /**
     * Processes InputStream input.
     *
     * @param inputStream the input stream
     * @return processed data
     * @throws IOException if reading fails
     */
    private Object processInputStream(InputStream inputStream) throws IOException {
        // Reset stream if possible
        if (inputStream.markSupported()) {
            inputStream.reset();
        }
        
        // Try to detect content type and process accordingly
        String content = thriftSerializer.convertInputToString(inputStream);
        
        // Check if it's JSON
        if (thriftSerializer.isValidJson(content)) {
            return content;
        }
        
        // Try to deserialize as Thrift binary
        // For now, return as string - actual type detection would be more complex
        return content;
    }
    
    /**
     * Processes byte[] input.
     *
     * @param bytes the byte array
     * @return processed data
     */
    private Object processByteArray(byte[] bytes) {
        return thriftSerializer.convertInputToString(bytes);
    }
    
    /**
     * Processes Reader input.
     *
     * @param reader the reader
     * @return processed data
     * @throws IOException if reading fails
     */
    private Object processReader(Reader reader) throws IOException {
        // Reset reader if possible
        if (reader.markSupported()) {
            reader.reset();
        }
        
        return thriftSerializer.convertInputToString(reader);
    }
    
    /**
     * Determines if transformation is from Thrift to SDO.
     *
     * @param source the source object
     * @param targetClass the target class
     * @return true if Thrift to SDO transformation
     */
    private boolean isThriftToSDOTransformation(Object source, Class<?> targetClass) {
        return (source instanceof TBase || isValidThriftJson(source)) && 
               (targetClass == EDataObject.class || 
                (targetClass != null && EDataObject.class.isAssignableFrom(targetClass)));
    }
    
    /**
     * Determines if transformation is from SDO to Thrift.
     *
     * @param source the source object
     * @param targetClass the target class
     * @return true if SDO to Thrift transformation
     */
    private boolean isSDOToThriftTransformation(Object source, Class<?> targetClass) {
        return (source instanceof EDataObject || isValidSDOJson(source)) && 
               (targetClass != null && TBase.class.isAssignableFrom(targetClass));
    }
    
    /**
     * Checks if the source is valid Thrift JSON.
     *
     * @param source the source object
     * @return true if valid Thrift JSON
     */
    private boolean isValidThriftJson(Object source) {
        if (source instanceof String) {
            String jsonStr = (String) source;
            return thriftSerializer.isValidJson(jsonStr);
        }
        return false;
    }
    
    /**
     * Checks if the source is valid SDO JSON.
     * This is a simplified check - in practice, you'd need more sophisticated validation.
     *
     * @param source the source object
     * @return true if valid SDO JSON
     */
    private boolean isValidSDOJson(Object source) {
        if (source instanceof String) {
            String jsonStr = (String) source;
            return thriftSerializer.isValidJson(jsonStr);
        }
        return false;
    }
    
    /**
     * Performs Thrift to SDO transformation.
     *
     * @param source the source Thrift object
     * @param targetClass the target SDO class
     * @param options transformation options
     * @return the transformed SDO DataObject
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private Object transformThriftToSDO(Object source, Class<?> targetClass, Object options) 
            throws ThriftSDODataHandlerException {
        
        TBase thriftObject;
        
        if (source instanceof TBase) {
            thriftObject = (TBase) source;
        } else if (source instanceof String) {
            // Deserialize from JSON
            String jsonStr = (String) source;
            // Need to determine the Thrift class - this would require more logic
            // For now, assume we can determine it from options or context
            Class<? extends TBase> thriftClass = determineThriftClassFromOptions(options);
            if (thriftClass == null) {
                throw new ThriftSDODataHandlerException(
                    ThriftSDODataHandlerException.ErrorCodes.CONFIGURATION_ERROR,
                    "Cannot determine Thrift class for JSON transformation",
                    "JSON input: " + jsonStr.substring(0, Math.min(100, jsonStr.length()))
                );
            }
            thriftObject = thriftSerializer.deserializeFromString(jsonStr, thriftClass);
        } else {
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.VALIDATION_ERROR,
                "Invalid source type for Thrift to SDO transformation",
                "Source type: " + source.getClass().getName()
            );
        }
        
        return thriftToSDOTransformer.transformToSDO(thriftObject);
    }
    
    /**
     * Performs SDO to Thrift transformation.
     *
     * @param source the source SDO DataObject
     * @param targetClass the target Thrift class
     * @param options transformation options
     * @return the transformed Thrift object
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    @SuppressWarnings("unchecked")
    private Object transformSDOToThrift(Object source, Class<?> targetClass, Object options) 
            throws ThriftSDODataHandlerException {
        
        EDataObject sdoObject;
        
        if (source instanceof EDataObject) {
            sdoObject = (EDataObject) source;
        } else if (source instanceof String) {
            // This would require SDO JSON deserialization
            // For now, throw an exception as this is complex
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.UNSUPPORTED_OPERATION,
                "SDO JSON to Thrift transformation not yet supported",
                "Use EDataObject input instead"
            );
        } else {
            throw new ThriftSDODataHandlerException(
                ThriftSDODataHandlerException.ErrorCodes.VALIDATION_ERROR,
                "Invalid source type for SDO to Thrift transformation",
                "Source type: " + source.getClass().getName()
            );
        }
        
        return sdoToThriftTransformer.transformToThrift(sdoObject, (Class<? extends TBase>) targetClass);
    }
    
    /**
     * Handles generic transformation scenarios.
     *
     * @param source the source data
     * @param targetClass the target class
     * @param options transformation options
     * @return transformed data
     * @throws ThriftSDODataHandlerException if transformation fails
     */
    private Object handleGenericTransformation(Object source, Class<?> targetClass, Object options) 
            throws ThriftSDODataHandlerException {
        
        // For now, try to use standard mechanisms or return as-is if compatible
        if (targetClass != null && targetClass.isInstance(source)) {
            return source;
        }
        
        // Try string conversion
        if (targetClass == String.class) {
            return thriftSerializer.convertInputToString(source);
        }
        
        // If we reach here, the transformation is not supported
        throw new ThriftSDODataHandlerException(
            ThriftSDODataHandlerException.ErrorCodes.UNSUPPORTED_OPERATION,
            "Unsupported transformation: " + 
            (source != null ? source.getClass().getName() : "null") + " -> " + 
            (targetClass != null ? targetClass.getName() : "null"),
            "Source type: " + (source != null ? source.getClass().getName() : "null") + 
            ", Target type: " + (targetClass != null ? targetClass.getName() : "null")
        );
    }
    
    /**
     * Copies transformed data to target object.
     *
     * @param transformed the transformed data
     * @param target the target object
     * @throws ThriftSDODataHandlerException if copying fails
     */
    private void copyTransformedData(Object transformed, Object target) throws ThriftSDODataHandlerException {
        // This would involve reflection or specific copying logic
        // For now, this is a placeholder
        logger.warn("copyTransformedData not fully implemented - transformed: {}, target: {}", 
            transformed.getClass().getName(), target.getClass().getName());
    }
    
    /**
     * Determines the Thrift class from transformation options.
     *
     * @param options the transformation options
     * @return the Thrift class, or null if cannot be determined
     */
    @SuppressWarnings("unchecked")
    private Class<? extends TBase> determineThriftClassFromOptions(Object options) {
        if (options instanceof Class && TBase.class.isAssignableFrom((Class<?>) options)) {
            return (Class<? extends TBase>) options;
        }
        
        if (bindingContext != null) {
            Object thriftClass = bindingContext.get("thrift.target.class");
            if (thriftClass instanceof Class && TBase.class.isAssignableFrom((Class<?>) thriftClass)) {
                return (Class<? extends TBase>) thriftClass;
            }
        }
        
        return null;
    }
    
    /**
     * Gets a default value for the given class.
     *
     * @param targetClass the target class
     * @return the default value
     */
    private Object getDefaultValueForClass(Class<?> targetClass) {
        if (targetClass == null) {
            return null;
        }
        
        if (targetClass == boolean.class) return false;
        if (targetClass == byte.class) return (byte) 0;
        if (targetClass == short.class) return (short) 0;
        if (targetClass == int.class) return 0;
        if (targetClass == long.class) return 0L;
        if (targetClass == float.class) return 0.0f;
        if (targetClass == double.class) return 0.0;
        if (targetClass == String.class) return "";
        
        return null;
    }
    
    /**
     * Updates the configuration and reinitializes components.
     *
     * @param newConfig the new configuration
     */
    private void updateConfiguration(ThriftSDOConfiguration newConfig) {
        this.configuration = newConfig;
        this.thriftSerializer = new ThriftSerializer(configuration);
        this.thriftToSDOTransformer = new ThriftToSDOTransformer(configuration);
        this.sdoToThriftTransformer = new SDOToThriftTransformer(configuration);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Configuration updated: {}", configuration);
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
    
    /**
     * Gets the binding context.
     *
     * @return the binding context
     */
    public Map<String, Object> getBindingContext() {
        return bindingContext;
    }
    
    /**
     * Clears all caches in the transformers.
     */
    public void clearCaches() {
        if (thriftToSDOTransformer != null) {
            thriftToSDOTransformer.clearCaches();
        }
        if (sdoToThriftTransformer != null) {
            sdoToThriftTransformer.clearCaches();
        }
    }
    
    /**
     * Gets cache statistics from all transformers.
     *
     * @return a map containing cache statistics
     */
    public Map<String, Integer> getCacheStatistics() {
        Map<String, Integer> stats = new java.util.HashMap<>();
        
        if (thriftToSDOTransformer != null) {
            stats.putAll(thriftToSDOTransformer.getCacheStatistics());
        }
        
        if (sdoToThriftTransformer != null) {
            stats.putAll(sdoToThriftTransformer.getCacheStatistics());
        }
        
        return stats;
    }
    
    /**
     * Validates the current configuration.
     *
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validateConfiguration() {
        configuration.validate();
    }
}