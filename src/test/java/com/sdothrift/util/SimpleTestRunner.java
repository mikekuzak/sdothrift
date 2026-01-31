package com.sdothrift.util;

// import com.sdothrift.transformer.ThriftToSDOTransformer;
// import com.sdothrift.transformer.SDOToThriftTransformer;
// import com.sdothrift.config.ThriftSDOConfiguration;
// import com.sdothrift.util.TestDataGenerator;
// import com.sdothrift.util.TestFailureAnalyzer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
// import java.lang.reflect.Method;
// import java.util.ArrayList;
// import java.util.List;

/**
 * Simple test runner for executing tests when Maven is not available.
 * Provides basic test execution and failure analysis capabilities.
 */
public class SimpleTestRunner {
    
    /**
     * Runs a specific test method and provides analysis of any failures.
     *
     * @param testClass the test class
     * @param testName the name of the test method to run
     * @return true if test passed, false otherwise
     */
    public static boolean runSingleTest(Class<?> testClass, String testName) {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        
        try {
            // Capture output for analysis
            ByteArrayOutputStream outCapture = new ByteArrayOutputStream();
            ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
            System.setOut(new PrintStream(outCapture));
            System.setErr(new PrintStream(errCapture));
            
            System.out.println("Running test: " + testClass.getSimpleName() + "." + testName);
            System.out.println(createRepeatedString("=", 50));
            
            // Create test instance
            Object testInstance = testClass.getDeclaredConstructor().newInstance();
            
            // Run the test method
            Method testMethod = testClass.getMethod(testName);
            testMethod.invoke(testInstance);
            
            System.out.println("✅ Test " + testName + " PASSED");
            return true;
            
        } catch (Exception e) {
            System.err.println("X Test " + testName + " FAILED");
            System.err.println("Exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            
            // Analyze the failure
            List<TestFailureAnalyzer.AnalysisResult> analysisResults = new ArrayList<>();
            analysisResults.add(TestFailureAnalyzer.analyzeFailure(e, testName));
            
            // Generate failure report
            String report = TestFailureAnalyzer.generateFailureReport(analysisResults);
            System.err.println("\n" + report);
            
            return false;
            
        } finally {
            // Restore original output streams
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }
    
    /**
     * Runs all test methods in a class.
     *
     * @param testClass the test class
     * @return number of tests passed
     */
    public static int runAllTests(Class<?> testClass) {
        java.lang.reflect.Method[] methods = testClass.getDeclaredMethods();
        int passedTests = 0;
        int totalTests = 0;
        
        List<TestFailureAnalyzer.AnalysisResult> allFailures = new ArrayList<>();
        
        for (Method method : methods) {
            if (method.getName().startsWith("test") || method.getName().startsWith("should")) {
                totalTests++;
                try {
                    // Create test instance
                    Object testInstance = testClass.getDeclaredConstructor().newInstance();
                    method.invoke(testInstance);
                    System.out.println("+ " + testClass.getSimpleName() + "." + method.getName() + " PASSED");
                    passedTests++;
                } catch (Exception e) {
                    System.err.println("X " + testClass.getSimpleName() + "." + method.getName() + " FAILED");
                    System.err.println("Exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                    
                    allFailures.add(TestFailureAnalyzer.analyzeFailure(e, method.getName()));
                }
            }
        }
        
        // Summary
        System.out.println("\n" + createRepeatedString("=", 50));
        System.out.println("TEST SUMMARY");
        System.out.println("Total Tests: " + totalTests);
        System.out.println("Passed Tests: " + passedTests);
        System.out.println("Failed Tests: " + (totalTests - passedTests));
        System.out.println("Success Rate: " + 
                     String.format("%.2f%%", (totalTests > 0 ? (double) passedTests / totalTests * 100 : 0)));
        
        if (!allFailures.isEmpty()) {
            System.out.println("\n🎉 ALL TESTS PASSED!");
        } else {
            System.err.println("\nX FAILURE ANALYSIS REPORT");
            String report = TestFailureAnalyzer.generateFailureReport(allFailures);
            System.err.println(report);
        }
        
        System.out.println("\n" + createRepeatedString("=", 50));
        
        return passedTests;
    }
    
    /**
     * Runs performance tests and measures execution time.
     *
     * @param testClass the test class
     * @param testName the name of the performance test method
     * @param iterations number of iterations to run
     * @return average execution time in milliseconds
     */
    public static long runPerformanceTest(Class<?> testClass, String testName, int iterations) {
        System.out.println("Running performance test: " + testClass.getSimpleName() + "." + testName);
        System.out.println("Iterations: " + iterations);
        
        try {
            // Create test instance
            Object testInstance = testClass.getDeclaredConstructor().newInstance();
            Method testMethod = testClass.getMethod(testName);
            
            // Warm up
            testMethod.invoke(testInstance);
            
            // Measure performance
            long totalTime = 0;
            for (int i = 0; i < iterations; i++) {
                long startTime = System.nanoTime();
                testMethod.invoke(testInstance);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            
            long averageTime = totalTime / iterations / 1_000_000; // Convert to milliseconds
            
            System.out.println("✅ Performance test " + testName + " COMPLETED");
            System.out.println("Average execution time: " + averageTime + " ms");
            System.out.println("Total execution time: " + (totalTime / 1_000_000) + " ms");
            
            return averageTime;
            
        } catch (Exception e) {
            System.err.println("X Performance test " + testName + " FAILED");
            System.err.println("Exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return -1;
        }
    }
    
    /**
     * Main method for running tests from command line.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        System.out.println("🧪 SDO Thrift Data Handler Test Runner");
        System.out.println("Usage:");
        System.out.println("  java com.sdothrift.util.SimpleTestRunner <TestClassName> [testMethod]");
        System.out.println("  java com.sdothrift.util.SimpleTestRunner <TestClassName> all");
        System.out.println("\nExamples:");
        System.out.println("  java com.sdothrift.util.SimpleTestRunner TypeMapperTest shouldMapThriftBaseTypesToSDOTypes");
        System.out.println("  java com.sdothrift.util.SimpleTestRunner ThriftToSDOTransformerTest all");
        System.out.println("  java com.sdothrift.util.SimpleTestRunner SDOToThriftTransformerTest all");
        System.out.println("  java com.sdothrift.util.SimpleTestRunner ThriftSDODataHandlerTest shouldTransformThriftObjectToSDO");
        
        if (args.length < 2) {
            System.err.println("Error: Insufficient arguments");
            return;
        }
        
        String testClassName = args[0];
        String testName = args[1];
        
        if ("all".equalsIgnoreCase(testName)) {
            try {
                Class<?> testClass = Class.forName("com.sdothrift.transformer." + testClassName);
                int passedTests = runAllTests(testClass);
                System.exit(passedTests == getExpectedTestCount(testClass) ? 0 : 1);
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Test class not found: " + testClassName);
                System.exit(1);
            }
        } else {
            try {
                Class<?> testClass = Class.forName("com.sdothrift.transformer." + testClassName);
                boolean passed = runSingleTest(testClass, testName);
                System.exit(passed ? 0 : 1);
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Test class not found: " + testClassName);
                System.exit(1);
            }
        }
    }
    
    /**
     * Gets the expected number of tests for a test class.
     *
     * @param testClass the test class
     * @return expected test count
     */
    private static int getExpectedTestCount(Class<?> testClass) {
        String className = testClass.getSimpleName();
        
        switch (className) {
            case "TypeMapperTest":
                return 18; // Count of @Test methods
            case "ThriftToSDOTransformerTest":
                return 12;
            case "SDOToThriftTransformerTest":
                return 12;
            case "ThriftSDODataHandlerTest":
                return 16;
            default:
                return 0;
        }
    }
}