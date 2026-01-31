package com.sdothrift.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Thrift-SDO Data Handler.
 * Provides configuration options for various aspects of the transformation process.
 */
public class ThriftSDOConfiguration {
    
    // Thrift Protocol Options
    public enum ThriftProtocol {
        BINARY("binary"),
        COMPACT("compact"),
        JSON("json"),
        SIMPLE_JSON("simple_json");
        
        private final String protocolName;
        
        ThriftProtocol(String protocolName) {
            this.protocolName = protocolName;
        }
        
        public String getProtocolName() {
            return protocolName;
        }
        
        public static ThriftProtocol fromString(String protocol) {
            if (protocol == null) {
                return BINARY; // Default
            }
            
            for (ThriftProtocol tp : ThriftProtocol.values()) {
                if (tp.protocolName.equalsIgnoreCase(protocol.trim())) {
                    return tp;
                }
            }
            return BINARY; // Default fallback
        }
    }
    
    // Null Handling Strategies
    public enum NullHandlingStrategy {
        PRESERVE("preserve"),
        DEFAULT("default"),
        ERROR("error"),
        OMIT("omit");
        
        private final String strategyName;
        
        NullHandlingStrategy(String strategyName) {
            this.strategyName = strategyName;
        }
        
        public String getStrategyName() {
            return strategyName;
        }
        
        public static NullHandlingStrategy fromString(String strategy) {
            if (strategy == null) {
                return PRESERVE; // Default
            }
            
            for (NullHandlingStrategy nhs : NullHandlingStrategy.values()) {
                if (nhs.strategyName.equalsIgnoreCase(strategy.trim())) {
                    return nhs;
                }
            }
            return PRESERVE; // Default fallback
        }
    }
    
    // Collection Type Preferences
    public enum CollectionTypePreference {
        LIST("list"),
        SET("set"),
        ARRAY("array");
        
        private final String typeName;
        
        CollectionTypePreference(String typeName) {
            this.typeName = typeName;
        }
        
        public String getTypeName() {
            return typeName;
        }
        
        public static CollectionTypePreference fromString(String type) {
            if (type == null) {
                return LIST; // Default
            }
            
            for (CollectionTypePreference ctp : CollectionTypePreference.values()) {
                if (ctp.typeName.equalsIgnoreCase(type.trim())) {
                    return ctp;
                }
            }
            return LIST; // Default fallback
        }
    }
    
    private ThriftProtocol thriftProtocol;
    private NullHandlingStrategy nullHandlingStrategy;
    private CollectionTypePreference collectionTypePreference;
    private boolean performanceCachingEnabled;
    private int maxCacheSize;
    private boolean debugLoggingEnabled;
    private int bufferSize;
    private String characterEncoding;
    private boolean strictValidationEnabled;
    private Map<String, Object> customProperties;
    
    /**
     * Default constructor with default configuration values.
     */
    public ThriftSDOConfiguration() {
        this.thriftProtocol = ThriftProtocol.BINARY;
        this.nullHandlingStrategy = NullHandlingStrategy.PRESERVE;
        this.collectionTypePreference = CollectionTypePreference.LIST;
        this.performanceCachingEnabled = true;
        this.maxCacheSize = 1000;
        this.debugLoggingEnabled = false;
        this.bufferSize = 8192;
        this.characterEncoding = "UTF-8";
        this.strictValidationEnabled = true;
        this.customProperties = new HashMap<>();
    }
    
    /**
     * Copy constructor.
     *
     * @param other the configuration to copy
     */
    public ThriftSDOConfiguration(ThriftSDOConfiguration other) {
        this.thriftProtocol = other.thriftProtocol;
        this.nullHandlingStrategy = other.nullHandlingStrategy;
        this.collectionTypePreference = other.collectionTypePreference;
        this.performanceCachingEnabled = other.performanceCachingEnabled;
        this.maxCacheSize = other.maxCacheSize;
        this.debugLoggingEnabled = other.debugLoggingEnabled;
        this.bufferSize = other.bufferSize;
        this.characterEncoding = other.characterEncoding;
        this.strictValidationEnabled = other.strictValidationEnabled;
        this.customProperties = new HashMap<>(other.customProperties);
    }
    
    /**
     * Creates configuration from binding context map.
     *
     * @param context the binding context map
     * @return a new ThriftSDOConfiguration instance
     */
    public static ThriftSDOConfiguration fromBindingContext(Map<String, Object> context) {
        ThriftSDOConfiguration config = new ThriftSDOConfiguration();
        
        if (context == null) {
            return config;
        }
        
        // Extract configuration from context
        Object protocol = context.get("thrift.protocol");
        if (protocol instanceof String) {
            config.setThriftProtocol(ThriftProtocol.fromString((String) protocol));
        }
        
        Object nullStrategy = context.get("null.handling.strategy");
        if (nullStrategy instanceof String) {
            config.setNullHandlingStrategy(NullHandlingStrategy.fromString((String) nullStrategy));
        }
        
        Object collType = context.get("collection.type.preferences");
        if (collType instanceof String) {
            config.setCollectionTypePreference(CollectionTypePreference.fromString((String) collType));
        }
        
        Object caching = context.get("performance.caching.enabled");
        if (caching instanceof Boolean) {
            config.setPerformanceCachingEnabled((Boolean) caching);
        }
        
        Object cacheSize = context.get("performance.cache.size");
        if (cacheSize instanceof Integer) {
            config.setMaxCacheSize((Integer) cacheSize);
        }
        
        Object debugLogging = context.get("debug.logging.enabled");
        if (debugLogging instanceof Boolean) {
            config.setDebugLoggingEnabled((Boolean) debugLogging);
        }
        
        Object bufferSize = context.get("buffer.size");
        if (bufferSize instanceof Integer) {
            config.setBufferSize((Integer) bufferSize);
        }
        
        Object encoding = context.get("character.encoding");
        if (encoding instanceof String) {
            config.setCharacterEncoding((String) encoding);
        }
        
        Object strictValidation = context.get("strict.validation.enabled");
        if (strictValidation instanceof Boolean) {
            config.setStrictValidationEnabled((Boolean) strictValidation);
        }
        
        return config;
    }
    
    // Getters and Setters
    
    public ThriftProtocol getThriftProtocol() {
        return thriftProtocol;
    }
    
    public void setThriftProtocol(ThriftProtocol thriftProtocol) {
        this.thriftProtocol = thriftProtocol;
    }
    
    public NullHandlingStrategy getNullHandlingStrategy() {
        return nullHandlingStrategy;
    }
    
    public void setNullHandlingStrategy(NullHandlingStrategy nullHandlingStrategy) {
        this.nullHandlingStrategy = nullHandlingStrategy;
    }
    
    public CollectionTypePreference getCollectionTypePreference() {
        return collectionTypePreference;
    }
    
    public void setCollectionTypePreference(CollectionTypePreference collectionTypePreference) {
        this.collectionTypePreference = collectionTypePreference;
    }
    
    public boolean isPerformanceCachingEnabled() {
        return performanceCachingEnabled;
    }
    
    public void setPerformanceCachingEnabled(boolean performanceCachingEnabled) {
        this.performanceCachingEnabled = performanceCachingEnabled;
    }
    
    public int getMaxCacheSize() {
        return maxCacheSize;
    }
    
    public void setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize = Math.max(1, maxCacheSize); // Ensure minimum size of 1
    }
    
    public boolean isDebugLoggingEnabled() {
        return debugLoggingEnabled;
    }
    
    public void setDebugLoggingEnabled(boolean debugLoggingEnabled) {
        this.debugLoggingEnabled = debugLoggingEnabled;
    }
    
    public int getBufferSize() {
        return bufferSize;
    }
    
    public void setBufferSize(int bufferSize) {
        this.bufferSize = Math.max(1024, bufferSize); // Ensure minimum buffer size
    }
    
    public String getCharacterEncoding() {
        return characterEncoding;
    }
    
    public void setCharacterEncoding(String characterEncoding) {
        this.characterEncoding = characterEncoding != null ? characterEncoding : "UTF-8";
    }
    
    public boolean isStrictValidationEnabled() {
        return strictValidationEnabled;
    }
    
    public void setStrictValidationEnabled(boolean strictValidationEnabled) {
        this.strictValidationEnabled = strictValidationEnabled;
    }
    
    public Map<String, Object> getCustomProperties() {
        return new HashMap<>(customProperties);
    }
    
    public void setCustomProperties(Map<String, Object> customProperties) {
        this.customProperties = customProperties != null ? new HashMap<>(customProperties) : new HashMap<>();
    }
    
    public void setCustomProperty(String key, Object value) {
        this.customProperties.put(key, value);
    }
    
    public Object getCustomProperty(String key) {
        return this.customProperties.get(key);
    }
    
    /**
     * Returns a string representation of this configuration.
     *
     * @return configuration details as string
     */
    @Override
    public String toString() {
        return "ThriftSDOConfiguration{" +
                "thriftProtocol=" + thriftProtocol +
                ", nullHandlingStrategy=" + nullHandlingStrategy +
                ", collectionTypePreference=" + collectionTypePreference +
                ", performanceCachingEnabled=" + performanceCachingEnabled +
                ", maxCacheSize=" + maxCacheSize +
                ", debugLoggingEnabled=" + debugLoggingEnabled +
                ", bufferSize=" + bufferSize +
                ", characterEncoding='" + characterEncoding + '\'' +
                ", strictValidationEnabled=" + strictValidationEnabled +
                ", customProperties=" + customProperties +
                '}';
    }
    
    /**
     * Validates the configuration settings.
     *
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validate() throws IllegalArgumentException {
        if (maxCacheSize < 1) {
            throw new IllegalArgumentException("maxCacheSize must be at least 1");
        }
        
        if (bufferSize < 1024) {
            throw new IllegalArgumentException("bufferSize must be at least 1024");
        }
        
        if (characterEncoding == null || characterEncoding.trim().isEmpty()) {
            throw new IllegalArgumentException("characterEncoding cannot be null or empty");
        }
    }
}