package com.sdothrift.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.sdo.EDataObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for generating test data for unit tests.
 * Provides sample Thrift objects, SDO DataObjects, and JSON representations.
 */
public class TestDataGenerator {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Sample Thrift struct for testing.
     */
    public static class TestThriftStruct implements TBase<TestThriftStruct, TestThriftStruct._Fields> {
        
        public enum _Fields implements org.apache.thrift.TFieldIdEnum {
            ID((short)1, "id"),
            NAME((short)2, "name"),
            ACTIVE((short)3, "active"),
            SCORE((short)4, "score"),
            TAGS((short)5, "tags"),
            PROPERTIES((short)6, "properties"),
            NESTED((short)7, "nested");
            
            private static final Map<String, _Fields> byName = new HashMap<>();
            
            static {
                for (_Fields field : _Fields.values()) {
                    byName.put(field.getFieldName(), field);
                }
            }
            
            private final short _thriftId;
            private final String _fieldName;
            
            _Fields(short thriftId, String fieldName) {
                _thriftId = thriftId;
                _fieldName = fieldName;
            }
            
            @Override
            public short getThriftFieldId() {
                return _thriftId;
            }
            
            @Override
            public String getFieldName() {
                return _fieldName;
            }
            
            public static _Fields findByThriftId(int fieldId) {
                switch(fieldId) {
                    case 1: return ID;
                    case 2: return NAME;
                    case 3: return ACTIVE;
                    case 4: return SCORE;
                    case 5: return TAGS;
                    case 6: return PROPERTIES;
                    case 7: return NESTED;
                    default: return null;
                }
            }
            
            public static _Fields findByName(String name) {
                return byName.get(name);
            }
        }
        
        private static final Map<_Fields, FieldMetaData> metaDataMap;
        
        static {
            Map<_Fields, FieldMetaData> tmpMap = new HashMap<>();
            
            FieldMetaData idMetaData = new FieldMetaData(
                "id", 
                TType.I32, 
                1, 
                FieldValueMetaData.I32, 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData nameMetaData = new FieldMetaData(
                "name", 
                TType.STRING, 
                2, 
                FieldValueMetaData.STRING, 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData activeMetaData = new FieldMetaData(
                "active", 
                TType.BOOL, 
                3, 
                FieldValueMetaData.BOOL, 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData scoreMetaData = new FieldMetaData(
                "score", 
                TType.DOUBLE, 
                4, 
                FieldValueMetaData.DOUBLE, 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData tagsMetaData = new FieldMetaData(
                "tags", 
                TType.LIST, 
                5, 
                new FieldValueMetaData(TType.STRING), 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData propertiesMetaData = new FieldMetaData(
                "properties", 
                TType.MAP, 
                6, 
                new FieldValueMetaData(TType.STRING), 
                new FieldValueMetaData(TType.STRING), 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData nestedMetaData = new FieldMetaData(
                "nested", 
                TType.STRUCT, 
                7, 
                new StructMetaData(TType.STRUCT, TestNestedStruct.class), 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            tmpMap.put(_Fields.ID, idMetaData);
            tmpMap.put(_Fields.NAME, nameMetaData);
            tmpMap.put(_Fields.ACTIVE, activeMetaData);
            tmpMap.put(_Fields.SCORE, scoreMetaData);
            tmpMap.put(_Fields.TAGS, tagsMetaData);
            tmpMap.put(_Fields.PROPERTIES, propertiesMetaData);
            tmpMap.put(_Fields.NESTED, nestedMetaData);
            
            metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        }
        
        public static final Map<_Fields, FieldMetaData> metaDataMap = metaDataMap;
        
        private int id;
        private String name;
        private boolean active;
        private double score;
        private List<String> tags;
        private Map<String, String> properties;
        private TestNestedStruct nested;
        
        public TestThriftStruct() {
            this.id = 0;
            this.name = "";
            this.active = false;
            this.score = 0.0;
            this.tags = new ArrayList<>();
            this.properties = new HashMap<>();
            this.nested = null;
        }
        
        public TestThriftStruct(int id, String name, boolean active, double score, 
                             List<String> tags, Map<String, String> properties, TestNestedStruct nested) {
            this.id = id;
            this.name = name;
            this.active = active;
            this.score = score;
            this.tags = tags != null ? new ArrayList<>(tags) : new ArrayList<>();
            this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
            this.nested = nested;
        }
        
        @Override
        public TestThriftStruct deepCopy() {
            return new TestThriftStruct(id, name, active, score, tags, properties, nested);
        }
        
        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public boolean isActive() { return active; }
        public void setActive(boolean active) { this.active = active; }
        
        public double getScore() { return score; }
        public void setScore(double score) { this.score = score; }
        
        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags != null ? new ArrayList<>(tags) : new ArrayList<>(); }
        
        public Map<String, String> getProperties() { return properties; }
        public void setProperties(Map<String, String> properties) { 
            this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>(); 
        }
        
        public TestNestedStruct getNested() { return nested; }
        public void setNested(TestNestedStruct nested) { this.nested = nested; }
        
        @Override
        public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
            // Simplified implementation for testing
        }
        
        @Override
        public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
            // Simplified implementation for testing
        }
        
        @Override
        public String toString() {
            return "TestThriftStruct{id=" + id + ", name='" + name + "', active=" + active + 
                   ", score=" + score + ", tags=" + tags + ", properties=" + properties + 
                   ", nested=" + nested + "}";
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestThriftStruct)) return false;
            TestThriftStruct that = (TestThriftStruct) o;
            return id == that.id &&
                   active == that.active &&
                   Double.compare(that.score, score) == 0 &&
                   java.util.Objects.equals(name, that.name) &&
                   java.util.Objects.equals(tags, that.tags) &&
                   java.util.Objects.equals(properties, that.properties) &&
                   java.util.Objects.equals(nested, that.nested);
        }
        
        @Override
        public int hashCode() {
            return java.util.Objects.hash(id, name, active, score, tags, properties, nested);
        }
    }
    
    /**
     * Sample nested Thrift struct for testing.
     */
    public static class TestNestedStruct implements TBase<TestNestedStruct, TestNestedStruct._Fields> {
        
        public enum _Fields implements org.apache.thrift.TFieldIdEnum {
            VALUE((short)1, "value"),
            DESCRIPTION((short)2, "description");
            
            private static final Map<String, _Fields> byName = new HashMap<>();
            
            static {
                for (_Fields field : _Fields.values()) {
                    byName.put(field.getFieldName(), field);
                }
            }
            
            private final short _thriftId;
            private final String _fieldName;
            
            _Fields(short thriftId, String fieldName) {
                _thriftId = thriftId;
                _fieldName = fieldName;
            }
            
            @Override
            public short getThriftFieldId() {
                return _thriftId;
            }
            
            @Override
            public String getFieldName() {
                return _fieldName;
            }
            
            public static _Fields findByThriftId(int fieldId) {
                switch(fieldId) {
                    case 1: return VALUE;
                    case 2: return DESCRIPTION;
                    default: return null;
                }
            }
            
            public static _Fields findByName(String name) {
                return byName.get(name);
            }
        }
        
        private static final Map<_Fields, FieldMetaData> metaDataMap;
        
        static {
            Map<_Fields, FieldMetaData> tmpMap = new HashMap<>();
            
            FieldMetaData valueMetaData = new FieldMetaData(
                "value", 
                TType.STRING, 
                1, 
                FieldValueMetaData.STRING, 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            FieldMetaData descriptionMetaData = new FieldMetaData(
                "description", 
                TType.STRING, 
                2, 
                FieldValueMetaData.STRING, 
                false, 
                false, 
                false, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null, 
                null
            );
            
            tmpMap.put(_Fields.VALUE, valueMetaData);
            tmpMap.put(_Fields.DESCRIPTION, descriptionMetaData);
            
            metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        }
        
        public static final Map<_Fields, FieldMetaData> metaDataMap = metaDataMap;
        
        private String value;
        private String description;
        
        public TestNestedStruct() {
            this.value = "";
            this.description = "";
        }
        
        public TestNestedStruct(String value, String description) {
            this.value = value;
            this.description = description;
        }
        
        @Override
        public TestNestedStruct deepCopy() {
            return new TestNestedStruct(value, description);
        }
        
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        @Override
        public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
            // Simplified implementation for testing
        }
        
        @Override
        public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
            // Simplified implementation for testing
        }
        
        @Override
        public String toString() {
            return "TestNestedStruct{value='" + value + "', description='" + description + "'}";
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestNestedStruct)) return false;
            TestNestedStruct that = (TestNestedStruct) o;
            return java.util.Objects.equals(value, that.value) &&
                   java.util.Objects.equals(description, that.description);
        }
        
        @Override
        public int hashCode() {
            return java.util.Objects.hash(value, description);
        }
    }
    
    /**
     * Creates a test Thrift struct with sample data.
     *
     * @return a sample TestThriftStruct
     */
    public static TestThriftStruct createTestThriftStruct() {
        List<String> tags = new ArrayList<>();
        tags.add("tag1");
        tags.add("tag2");
        tags.add("tag3");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");
        
        TestNestedStruct nested = new TestNestedStruct("nested_value", "nested_description");
        
        return new TestThriftStruct(
            123,
            "Test Structure",
            true,
            95.5,
            tags,
            properties,
            nested
        );
    }
    
    /**
     * Creates a test Thrift struct with null values.
     *
     * @return a TestThriftStruct with null fields
     */
    public static TestThriftStruct createNullThriftStruct() {
        return new TestThriftStruct();
    }
    
    /**
     * Creates a test Thrift struct with empty collections.
     *
     * @return a TestThriftStruct with empty collections
     */
    public static TestThriftStruct createEmptyThriftStruct() {
        return new TestThriftStruct(
            0,
            "",
            false,
            0.0,
            new ArrayList<>(),
            new HashMap<>(),
            null
        );
    }
    
    /**
     * Creates a sample JSON representation of a Thrift struct.
     *
     * @return a JSON string representing a test Thrift struct
     */
    public static String createTestThriftJson() {
        return "{\n" +
               "  \"id\": 123,\n" +
               "  \"name\": \"Test Structure\",\n" +
               "  \"active\": true,\n" +
               "  \"score\": 95.5,\n" +
               "  \"tags\": [\"tag1\", \"tag2\", \"tag3\"],\n" +
               "  \"properties\": {\n" +
               "    \"key1\": \"value1\",\n" +
               "    \"key2\": \"value2\"\n" +
               "  },\n" +
               "  \"nested\": {\n" +
               "    \"value\": \"nested_value\",\n" +
               "    \"description\": \"nested_description\"\n" +
               "  }\n" +
               "}";
    }
    
    /**
     * Creates a sample JSON with null values.
     *
     * @return a JSON string with null values
     */
    public static String createNullThriftJson() {
        return "{\n" +
               "  \"id\": null,\n" +
               "  \"name\": null,\n" +
               "  \"active\": null,\n" +
               "  \"score\": null,\n" +
               "  \"tags\": null,\n" +
               "  \"properties\": null,\n" +
               "  \"nested\": null\n" +
               "}";
    }
    
    /**
     * Creates a sample JSON with empty collections.
     *
     * @return a JSON string with empty collections
     */
    public static String createEmptyThriftJson() {
        return "{\n" +
               "  \"id\": 0,\n" +
               "  \"name\": \"\",\n" +
               "  \"active\": false,\n" +
               "  \"score\": 0.0,\n" +
               "  \"tags\": [],\n" +
               "  \"properties\": {},\n" +
               "  \"nested\": null\n" +
               "}";
    }
    
    /**
     * Creates edge case test data.
     *
     * @return an array of objects for edge case testing
     */
    public static Object[] createEdgeCaseData() {
        return new Object[]{
            null,
            "",
            0,
            false,
            new ArrayList<>(),
            new HashMap<>(),
            new HashSet<>(),
            "Special chars: !@#$%^&*()_+-={}[]|\\:;\"'<>,.?/",
            "Unicode: 你好世界 🌍",
            "Numbers: 1234567890",
            "Mixed: Test123!@#"
        };
    }
    
    /**
     * Creates test data for type mapping.
     *
     * @return a map of test values for different types
     */
    public static Map<String, Object> createTypeMappingTestData() {
        Map<String, Object> testData = new HashMap<>();
        testData.put("bool", true);
        testData.put("byte", (byte) 127);
        testData.put("i16", (short) 32767);
        testData.put("i32", 2147483647);
        testData.put("i64", 9223372036854775807L);
        testData.put("double", 3.14159265359);
        testData.put("string", "test string");
        testData.put("list", new ArrayList<>());
        testData.put("set", new HashSet<>());
        testData.put("map", new HashMap<>());
        return testData;
    }
    
    /**
     * Parses a JSON string to JsonNode.
     *
     * @param json the JSON string
     * @return the JsonNode
     * @throws Exception if parsing fails
     */
    public static JsonNode parseJson(String json) throws Exception {
        return objectMapper.readTree(json);
    }
}