package com.sdothrift.util;

/**
 * Java 8 compatibility utilities for handling common operations.
 * Provides methods that may not be available in older Java versions.
 */
public class Java8CompatibilityUtils {
    
    /**
     * Creates a repeated string - Java 8 compatible version of String.repeat.
     *
     * @param str the string to repeat
     * @param count the number of times to repeat
     * @return the repeated string
     */
    public static String createRepeatedString(String str, int count) {
        if (str == null || count <= 0) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}