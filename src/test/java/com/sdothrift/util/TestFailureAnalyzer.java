package com.sdothrift.util;

import com.sdothrift.exception.ThriftSDODataHandlerException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for analyzing test failures and providing suggested fixes.
 * This helps in identifying common failure patterns and their solutions.
 */
public class TestFailureAnalyzer {
    
    /**
     * Result class containing analysis information.
     */
    public static class AnalysisResult {
        private String failureCategory;
        private String rootCause;
        private String suggestedFix;
        private List<String> affectedTests;
        private String severity;
        
        public AnalysisResult(String failureCategory, String rootCause, String suggestedFix, 
                           String severity, List<String> affectedTests) {
            this.failureCategory = failureCategory;
            this.rootCause = rootCause;
            this.suggestedFix = suggestedFix;
            this.severity = severity;
            this.affectedTests = new ArrayList<>(affectedTests);
        }
        
        // Getters
        public String getFailureCategory() { return failureCategory; }
        public String getRootCause() { return rootCause; }
        public String getSuggestedFix() { return suggestedFix; }
        public List<String> getAffectedTests() { return new ArrayList<>(affectedTests); }
        public String getSeverity() { return severity; }
        
        @Override
        public String toString() {
            return "AnalysisResult{" +
                   "category='" + failureCategory + '\'' +
                   ", cause='" + rootCause + '\'' +
                   ", fix='" + suggestedFix + '\'' +
                   ", severity='" + severity + '\'' +
                   ", affectedTests=" + affectedTests +
                   '}';
        }
    }
    
    /**
     * Analyzes a test failure and provides detailed analysis.
     *
     * @param failure the exception or failure that occurred
     * @param testName the name of the test that failed
     * @return analysis result with suggestions
     */
    public static AnalysisResult analyzeFailure(Throwable failure, String testName) {
        String failureCategory = categorizeFailure(failure);
        String rootCause = determineRootCause(failure);
        String suggestedFix = generateSuggestedFix(failureCategory, rootCause);
        String severity = determineSeverity(failureCategory, failure);
        List<String> affectedTests = new ArrayList<>();
        affectedTests.add(testName);
        
        return new AnalysisResult(failureCategory, rootCause, suggestedFix, severity, affectedTests);
    }
    
    /**
     * Analyzes multiple test failures and provides aggregated analysis.
     *
     * @param failures a map of test names to their failures
     * @return a list of analysis results
     */
    public static List<AnalysisResult> analyzeMultipleFailures(Map<String, Throwable> failures) {
        Map<String, List<String>> failuresByCategory = new HashMap<>();
        List<AnalysisResult> results = new ArrayList<>();
        
        // First pass: categorize all failures
        for (Map.Entry<String, Throwable> entry : failures.entrySet()) {
            String testName = entry.getKey();
            Throwable failure = entry.getValue();
            String category = categorizeFailure(failure);
            
            failuresByCategory.computeIfAbsent(category, k -> new ArrayList<>()).add(testName);
        }
        
        // Second pass: create analysis results for each category
        for (Map.Entry<String, List<String>> entry : failuresByCategory.entrySet()) {
            String category = entry.getKey();
            List<String> affectedTests = entry.getValue();
            
            // Get a representative failure from the first test in this category
            String representativeTest = affectedTests.get(0);
            Throwable representativeFailure = failures.get(representativeTest);
            
            String rootCause = determineRootCause(representativeFailure);
            String suggestedFix = generateSuggestedFix(category, rootCause);
            String severity = determineSeverity(category, representativeFailure);
            
            results.add(new AnalysisResult(category, rootCause, suggestedFix, severity, affectedTests));
        }
        
        return results;
    }
    
    /**
     * Categorizes the type of failure.
     *
     * @param failure the exception that occurred
     * @return the failure category
     */
    private static String categorizeFailure(Throwable failure) {
        if (failure == null) {
            return "UNKNOWN";
        }
        
        String className = failure.getClass().getSimpleName();
        String message = failure.getMessage();
        
        if (failure instanceof ThriftSDODataHandlerException) {
            return categorizeThriftSDOException((ThriftSDODataHandlerException) failure);
        } else if (className.contains("NullPointerException")) {
            return "NULL_POINTER";
        } else if (className.contains("ClassCastException")) {
            return "TYPE_CAST";
        } else if (className.contains("IllegalArgumentException")) {
            return "ILLEGAL_ARGUMENT";
        } else if (className.contains("IOException")) {
            return "IO_ERROR";
        } else if (className.contains("JSONException") || className.contains("JsonProcessingException")) {
            return "JSON_PROCESSING";
        } else if (className.contains("AssertionError")) {
            return "ASSERTION_FAILED";
        } else if (message != null && message.contains("reflection")) {
            return "REFLECTION_ERROR";
        } else if (message != null && message.contains("protocol")) {
            return "PROTOCOL_ERROR";
        } else if (message != null && message.contains("serialization")) {
            return "SERIALIZATION_ERROR";
        } else {
            return "UNKNOWN";
        }
    }
    
    /**
     * Categorizes ThriftSDODataHandlerException specifically.
     *
     * @param exception the ThriftSDODataHandlerException
     * @return the failure category
     */
    private static String categorizeThriftSDOException(ThriftSDODataHandlerException exception) {
        String errorCode = exception.getErrorCode();
        if (errorCode == null) {
            return "THRIFT_SDO_GENERIC";
        }
        
        switch (errorCode) {
            case ThriftSDODataHandlerException.ErrorCodes.TYPE_MAPPING_ERROR:
                return "TYPE_MAPPING";
            case ThriftSDODataHandlerException.ErrorCodes.SERIALIZATION_ERROR:
                return "SERIALIZATION";
            case ThriftSDODataHandlerException.ErrorCodes.DESERIALIZATION_ERROR:
                return "DESERIALIZATION";
            case ThriftSDODataHandlerException.ErrorCodes.CONFIGURATION_ERROR:
                return "CONFIGURATION";
            case ThriftSDODataHandlerException.ErrorCodes.VALIDATION_ERROR:
                return "VALIDATION";
            case ThriftSDODataHandlerException.ErrorCodes.NULL_INPUT_ERROR:
                return "NULL_INPUT";
            case ThriftSDODataHandlerException.ErrorCodes.UNSUPPORTED_OPERATION:
                return "UNSUPPORTED";
            case ThriftSDODataHandlerException.ErrorCodes.REFLECTION_ERROR:
                return "REFLECTION";
            case ThriftSDODataHandlerException.ErrorCodes.IO_ERROR:
                return "IO_ERROR";
            case ThriftSDODataHandlerException.ErrorCodes.JSON_PROCESSING_ERROR:
                return "JSON_PROCESSING";
            case ThriftSDODataHandlerException.ErrorCodes.SDO_PROCESSING_ERROR:
                return "SDO_PROCESSING";
            case ThriftSDODataHandlerException.ErrorCodes.THRIFT_PROCESSING_ERROR:
                return "THRIFT_PROCESSING";
            default:
                return "THRIFT_SDO_GENERIC";
        }
    }
    
    /**
     * Determines the root cause of the failure.
     *
     * @param failure the exception that occurred
     * @return the root cause description
     */
    private static String determineRootCause(Throwable failure) {
        if (failure == null) {
            return "Unknown root cause";
        }
        
        String message = failure.getMessage();
        String className = failure.getClass().getSimpleName();
        
        if (message != null) {
            if (message.contains("null") && message.contains("input")) {
                return "Null input value not properly handled";
            } else if (message.contains("type") && message.contains("conversion")) {
                return "Type conversion between incompatible types";
            } else if (message.contains("metadata")) {
                return "Thrift metadata reflection access failed";
            } else if (message.contains("JSON") && message.contains("parse")) {
                return "JSON parsing failed due to invalid format";
            } else if (message.contains("SDO") && message.contains("create")) {
                return "SDO DataObject creation failed";
            } else if (message.contains("protocol") && message.contains("mismatch")) {
                return "Thrift protocol configuration mismatch";
            }
        }
        
        // Check stack trace for clues
        StackTraceElement[] stackTrace = failure.getStackTrace();
        for (StackTraceElement element : stackTrace) {
            String methodName = element.getMethodName();
            if (methodName.contains("transform") || methodName.contains("convert")) {
                return "Transformation operation failed";
            } else if (methodName.contains("serialize") || methodName.contains("deserialize")) {
                return "Serialization/deserialization operation failed";
            } else if (methodName.contains("reflect")) {
                return "Reflection-based operation failed";
            }
        }
        
        return "Root cause: " + className + " - " + (message != null ? message : "No message");
    }
    
    /**
     * Generates suggested fixes based on failure category and root cause.
     *
     * @param category the failure category
     * @param rootCause the root cause
     * @return suggested fix
     */
    private static String generateSuggestedFix(String category, String rootCause) {
        switch (category) {
            case "NULL_POINTER":
                return "Add null checks before accessing object properties. Consider using Optional pattern or default values.";
            case "TYPE_CAST":
                return "Verify type compatibility before casting. Use instanceof checks or implement proper type conversion.";
            case "ILLEGAL_ARGUMENT":
                return "Validate input parameters before processing. Add parameter bounds checking.";
            case "TYPE_MAPPING":
                return "Check TypeMapper configuration. Ensure all Thrift types have corresponding SDO mappings.";
            case "SERIALIZATION":
                return "Verify Thrift protocol configuration matches serialization format. Check object field annotations.";
            case "DESERIALIZATION":
                return "Ensure input data format matches expected Thrift structure. Validate JSON schema.";
            case "CONFIGURATION":
                return "Review ThriftSDOConfiguration settings. Check binding context configuration.";
            case "VALIDATION":
                return "Implement proper input validation. Add pre-transformation validation steps.";
            case "NULL_INPUT":
                return "Configure null handling strategy properly. Add null input handling in transformation logic.";
            case "UNSUPPORTED":
                return "Check if the operation is supported. Consider implementing missing functionality.";
            case "REFLECTION":
                return "Verify Thrift class accessibility. Check generated code and reflection permissions.";
            case "IO_ERROR":
                return "Check file permissions and stream handling. Ensure proper resource cleanup.";
            case "JSON_PROCESSING":
                return "Validate JSON format. Check for malformed JSON or incompatible structure.";
            case "SDO_PROCESSING":
                return "Verify SDO model configuration. Check EClass and EAttribute definitions.";
            case "THRIFT_PROCESSING":
                return "Check Thrift object structure. Verify field metadata and annotations.";
            case "PROTOCOL_ERROR":
                return "Verify Thrift protocol configuration matches serialization format. Check binary/JSON compatibility.";
            case "ASSERTION_FAILED":
                return "Review test expectations. Check if test data matches expected transformation results.";
            default:
                return "Add comprehensive error handling and logging. Review transformation logic for edge cases.";
        }
    }
    
    /**
     * Determines the severity of the failure.
     *
     * @param category the failure category
     * @param failure the actual failure
     * @return severity level (CRITICAL, HIGH, MEDIUM, LOW)
     */
    private static String determineSeverity(String category, Throwable failure) {
        if (failure instanceof OutOfMemoryError || failure instanceof StackOverflowError) {
            return "CRITICAL";
        }
        
        switch (category) {
            case "NULL_POINTER":
            case "TYPE_CAST":
            case "CONFIGURATION":
                return "HIGH";
            case "TYPE_MAPPING":
            case "SERIALIZATION":
            case "DESERIALIZATION":
            case "VALIDATION":
                return "MEDIUM";
            case "IO_ERROR":
            case "JSON_PROCESSING":
            case "ASSERTION_FAILED":
                return "LOW";
            default:
                return "MEDIUM";
        }
    }
    
    /**
     * Generates a comprehensive failure report.
     *
     * @param analysisResults the analysis results
     * @return a formatted report string
     */
    public static String generateFailureReport(List<AnalysisResult> analysisResults) {
        if (analysisResults == null || analysisResults.isEmpty()) {
            return "No failures to report.";
        }
        
        StringBuilder report = new StringBuilder();
        report.append("=== TEST FAILURE ANALYSIS REPORT ===\n\n");
        
        // Summary statistics
        Map<String, Integer> categoryCounts = new HashMap<>();
        Map<String, Integer> severityCounts = new HashMap<>();
        int totalFailures = 0;
        
        for (AnalysisResult result : analysisResults) {
            categoryCounts.put(result.getFailureCategory(), 
                categoryCounts.getOrDefault(result.getFailureCategory(), 0) + result.getAffectedTests().size());
            severityCounts.put(result.getSeverity(), 
                severityCounts.getOrDefault(result.getSeverity(), 0) + result.getAffectedTests().size());
            totalFailures += result.getAffectedTests().size();
        }
        
        report.append("SUMMARY:\n");
        report.append("Total Failed Tests: ").append(totalFailures).append("\n");
        report.append("Failure Categories:\n");
        for (Map.Entry<String, Integer> entry : categoryCounts.entrySet()) {
            report.append("  - ").append(entry.getKey()).append(": ").append(entry.getValue()).append(" tests\n");
        }
        report.append("Severity Breakdown:\n");
        for (Map.Entry<String, Integer> entry : severityCounts.entrySet()) {
            report.append("  - ").append(entry.getKey()).append(": ").append(entry.getValue()).append(" tests\n");
        }
        report.append("\n");
        
        // Detailed analysis
        report.append("DETAILED ANALYSIS:\n");
        for (AnalysisResult result : analysisResults) {
            report.append("Category: ").append(result.getFailureCategory()).append("\n");
            report.append("Severity: ").append(result.getSeverity()).append("\n");
            report.append("Root Cause: ").append(result.getRootCause()).append("\n");
            report.append("Suggested Fix: ").append(result.getSuggestedFix()).append("\n");
            report.append("Affected Tests: ").append(String.join(", ", result.getAffectedTests())).append("\n");
            report.append("---\n");
        }
        
        // Recommendations
        report.append("RECOMMENDATIONS:\n");
        report.append("1. Address CRITICAL and HIGH severity issues first\n");
        report.append("2. Implement proper error handling and validation\n");
        report.append("3. Add comprehensive unit test coverage for edge cases\n");
        report.append("4. Consider adding integration tests for end-to-end scenarios\n");
        report.append("5. Review configuration and setup issues\n");
        
        return report.toString();
    }
    
    /**
     * Gets common failure patterns and their solutions.
     *
     * @return a map of common patterns to their solutions
     */
    public static Map<String, String> getCommonFailurePatterns() {
        Map<String, String> patterns = new HashMap<>();
        
        patterns.put("Thrift metadata reflection failures",
            "Ensure generated Thrift classes are on classpath and have proper public field access. Check that metaDataMap field is accessible.");
        
        patterns.put("JSON parsing errors",
            "Validate JSON structure matches Thrift schema. Use JSON schema validation if needed.");
        
        patterns.put("SDO DataObject creation failures",
            "Verify EClass definitions are correct. Check that required attributes are properly configured.");
        
        patterns.put("Type conversion failures",
            "Review TypeMapper configuration. Ensure all primitive types have proper mappings.");
        
        patterns.put("Configuration mismatches",
            "Check ThriftSDOConfiguration settings. Verify binding context is properly set.");
        
        patterns.put("Null value handling",
            "Configure null handling strategy appropriately. Add null checks in transformation logic.");
        
        patterns.put("Collection transformation issues",
            "Verify collection type mappings. Check for type safety in collection element transformations.");
        
        return patterns;
    }
}