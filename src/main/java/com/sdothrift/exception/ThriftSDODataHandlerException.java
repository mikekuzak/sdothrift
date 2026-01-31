package com.sdothrift.exception;

/**
 * Custom exception for Thrift-SDO Data Handler operations.
 * This exception is thrown when errors occur during transformation
 * between Thrift objects and SDO DataObjects.
 */
public class ThriftSDODataHandlerException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    private String errorCode;
    private String details;
    
    /**
     * Constructs a new ThriftSDODataHandlerException with the specified detail message.
     *
     * @param message the detail message explaining the cause of the exception
     */
    public ThriftSDODataHandlerException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new ThriftSDODataHandlerException with the specified cause.
     *
     * @param cause the cause of the exception
     */
    public ThriftSDODataHandlerException(Throwable cause) {
        super(cause);
    }
    
    /**
     * Constructs a new ThriftSDODataHandlerException with the specified detail message and cause.
     *
     * @param message the detail message explaining the cause of the exception
     * @param cause the cause of the exception
     */
    public ThriftSDODataHandlerException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new ThriftSDODataHandlerException with error code, message, and details.
     *
     * @param errorCode the error code for categorizing the exception
     * @param message the detail message explaining the cause of the exception
     * @param details additional details about the exception
     */
    public ThriftSDODataHandlerException(String errorCode, String message, String details) {
        super(message);
        this.errorCode = errorCode;
        this.details = details;
    }
    
    /**
     * Constructs a new ThriftSDODataHandlerException with error code, message, details, and cause.
     *
     * @param errorCode the error code for categorizing the exception
     * @param message the detail message explaining the cause of the exception
     * @param details additional details about the exception
     * @param cause the cause of the exception
     */
    public ThriftSDODataHandlerException(String errorCode, String message, String details, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.details = details;
    }
    
    /**
     * Gets the error code associated with this exception.
     *
     * @return the error code, or null if not set
     */
    public String getErrorCode() {
        return errorCode;
    }
    
    /**
     * Sets the error code for this exception.
     *
     * @param errorCode the error code to set
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
    
    /**
     * Gets the additional details about this exception.
     *
     * @return the details, or null if not set
     */
    public String getDetails() {
        return details;
    }
    
    /**
     * Sets the additional details for this exception.
     *
     * @param details the details to set
     */
    public void setDetails(String details) {
        this.details = details;
    }
    
    /**
     * Returns a comprehensive string representation of this exception.
     *
     * @return a string containing error code, message, and details
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        if (errorCode != null) {
            sb.append(" [ErrorCode: ").append(errorCode).append("]");
        }
        if (details != null) {
            sb.append(" [Details: ").append(details).append("]");
        }
        return sb.toString();
    }
    
    /**
     * Common error codes used throughout the application.
     */
    public static class ErrorCodes {
        public static final String TYPE_MAPPING_ERROR = "TYPE_MAPPING_ERROR";
        public static final String SERIALIZATION_ERROR = "SERIALIZATION_ERROR";
        public static final String DESERIALIZATION_ERROR = "DESERIALIZATION_ERROR";
        public static final String CONFIGURATION_ERROR = "CONFIGURATION_ERROR";
        public static final String VALIDATION_ERROR = "VALIDATION_ERROR";
        public static final String NULL_INPUT_ERROR = "NULL_INPUT_ERROR";
        public static final String UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION";
        public static final String REFLECTION_ERROR = "REFLECTION_ERROR";
        public static final String IO_ERROR = "IO_ERROR";
        public static final String JSON_PROCESSING_ERROR = "JSON_PROCESSING_ERROR";
        public static final String SDO_PROCESSING_ERROR = "SDO_PROCESSING_ERROR";
        public static final String THRIFT_PROCESSING_ERROR = "THRIFT_PROCESSING_ERROR";
        public static final String TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR";
        public static final String CONVERSION_ERROR = "CONVERSION_ERROR";
    }
}