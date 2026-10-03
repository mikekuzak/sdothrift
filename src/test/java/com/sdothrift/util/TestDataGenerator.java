package com.sdothrift.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

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
        
        public static final Map<_Fields, FieldMetaData> metaDataMap;
        
        static {
            Map<_Fields, FieldMetaData> tmpMap = new HashMap<>();
            
            FieldMetaData idMetaData = new FieldMetaData(
                "id", 
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.I32)
            );
            
            FieldMetaData nameMetaData = new FieldMetaData(
                "name", 
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.STRING)
            );
            
            FieldMetaData activeMetaData = new FieldMetaData(
                "active", 
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.BOOL)
            );
            
            FieldMetaData scoreMetaData = new FieldMetaData(
                "score", 
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.DOUBLE)
            );
            
            FieldMetaData tagsMetaData = new FieldMetaData(
                "tags", 
                org.apache.thrift.TFieldRequirementType.OPTIONAL,
                new ListMetaData(TType.LIST, new FieldValueMetaData(TType.STRING))
            );
            
            FieldMetaData propertiesMetaData = new FieldMetaData(
                "properties", 
                org.apache.thrift.TFieldRequirementType.OPTIONAL,
                new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.STRING))
            );
            
            FieldMetaData nestedMetaData = new FieldMetaData(
                "nested", 
                org.apache.thrift.TFieldRequirementType.OPTIONAL,
                new StructMetaData(TType.STRUCT, TestNestedStruct.class)
            );
            
            tmpMap.put(_Fields.ID, idMetaData);
            tmpMap.put(_Fields.NAME, nameMetaData);
            tmpMap.put(_Fields.ACTIVE, activeMetaData);
            tmpMap.put(_Fields.SCORE, scoreMetaData);
            tmpMap.put(_Fields.TAGS, tagsMetaData);
            tmpMap.put(_Fields.PROPERTIES, propertiesMetaData);
            tmpMap.put(_Fields.NESTED, nestedMetaData);
            
            metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
            FieldMetaData.addStructMetaDataMap(TestThriftStruct.class, metaDataMap);
        }
        
        private int id;
        private String name;
        private boolean active;
        private double score;
        private List<String> tags;
        private Map<String, String> properties;
        private TestNestedStruct nested;
        private boolean __isset_id;
        private boolean __isset_active;
        private boolean __isset_score;
        
        public TestThriftStruct() {
            this.id = 0;
            this.name = null;
            this.active = false;
            this.score = 0.0;
            this.tags = null;
            this.properties = null;
            this.nested = null;
        }
        
        public TestThriftStruct(int id, String name, boolean active, double score, 
                             List<String> tags, Map<String, String> properties, TestNestedStruct nested) {
            this.id = id;
            this.__isset_id = true;
            this.name = name;
            this.active = active;
            this.__isset_active = true;
            this.score = score;
            this.__isset_score = true;
            this.tags = tags != null ? new ArrayList<>(tags) : null;
            this.properties = properties != null ? new HashMap<>(properties) : null;
            this.nested = nested == null ? null : nested.deepCopy();
        }
        
        @Override
        public TestThriftStruct deepCopy() {
            TestThriftStruct copy = new TestThriftStruct();
            copy.id = id;
            copy.name = name;
            copy.active = active;
            copy.score = score;
            copy.tags = tags == null ? null : new ArrayList<>(tags);
            copy.properties = properties == null ? null : new HashMap<>(properties);
            copy.nested = nested == null ? null : nested.deepCopy();
            copy.__isset_id = __isset_id;
            copy.__isset_active = __isset_active;
            copy.__isset_score = __isset_score;
            return copy;
        }
        
        @Override
        public void clear() {
            this.id = 0;
            this.name = null;
            this.active = false;
            this.score = 0.0;
            this.tags = null;
            this.properties = null;
            this.nested = null;
            this.__isset_id = false;
            this.__isset_active = false;
            this.__isset_score = false;
        }
        
        @Override
        public _Fields fieldForId(int fieldId) {
            return _Fields.findByThriftId(fieldId);
        }
        
        @Override
        public boolean isSet(_Fields field) {
            if (field == null) return false;
            switch (field) {
                case ID: return __isset_id;
                case NAME: return name != null;
                case ACTIVE: return __isset_active;
                case SCORE: return __isset_score;
                case TAGS: return tags != null;
                case PROPERTIES: return properties != null;
                case NESTED: return nested != null;
                default: return false;
            }
        }
        
        @Override
        public Object getFieldValue(_Fields field) {
            switch (field) {
                case ID: return id;
                case NAME: return name;
                case ACTIVE: return active;
                case SCORE: return score;
                case TAGS: return tags;
                case PROPERTIES: return properties;
                case NESTED: return nested;
                default: return null;
            }
        }
        
        @Override
        public void setFieldValue(_Fields field, Object value) {
            switch (field) {
                case ID:
                    if (value == null) { id = 0; __isset_id = false; } else setId((Integer) value);
                    break;
                case NAME: setName((String) value); break;
                case ACTIVE:
                    if (value == null) { active = false; __isset_active = false; } else setActive((Boolean) value);
                    break;
                case SCORE:
                    if (value == null) { score = 0.0; __isset_score = false; } else setScore((Double) value);
                    break;
                case TAGS: setTags((List<String>) value); break;
                case PROPERTIES: setProperties((Map<String, String>) value); break;
                case NESTED: setNested((TestNestedStruct) value); break;
            }
        }
        
        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; this.__isset_id = true; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public boolean isActive() { return active; }
        public void setActive(boolean active) { this.active = active; this.__isset_active = true; }
        
        public double getScore() { return score; }
        public void setScore(double score) { this.score = score; this.__isset_score = true; }
        
        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags != null ? new ArrayList<>(tags) : null; }
        
        public Map<String, String> getProperties() { return properties; }
        public void setProperties(Map<String, String> properties) { 
            this.properties = properties != null ? new HashMap<>(properties) : null; 
        }
        
        public TestNestedStruct getNested() { return nested; }
        public void setNested(TestNestedStruct nested) { this.nested = nested; }
        
        @Override
        public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
            clear();
            iprot.readStructBegin();
            while (true) {
                TField field = iprot.readFieldBegin();
                if (field.type == TType.STOP) break;
                switch (field.id) {
                    case 1:
                        if (field.type == TType.I32) setId(iprot.readI32());
                        else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 2:
                        if (field.type == TType.STRING) setName(iprot.readString());
                        else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 3:
                        if (field.type == TType.BOOL) setActive(iprot.readBool());
                        else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 4:
                        if (field.type == TType.DOUBLE) setScore(iprot.readDouble());
                        else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 5:
                        if (field.type == TType.LIST) {
                            TList list = iprot.readListBegin();
                            List<String> readTags = new ArrayList<>(list.size);
                            for (int i = 0; i < list.size; i++) {
                                if (list.elemType == TType.STRING) readTags.add(iprot.readString());
                                else { TProtocolUtil.skip(iprot, list.elemType); readTags.add(null); }
                            }
                            iprot.readListEnd();
                            setTags(readTags);
                        } else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 6:
                        if (field.type == TType.MAP) {
                            TMap map = iprot.readMapBegin();
                            Map<String, String> readProperties = new HashMap<>();
                            for (int i = 0; i < map.size; i++) {
                                String key = null;
                                String value = null;
                                if (map.keyType == TType.STRING) key = iprot.readString();
                                else TProtocolUtil.skip(iprot, map.keyType);
                                if (map.valueType == TType.STRING) value = iprot.readString();
                                else TProtocolUtil.skip(iprot, map.valueType);
                                readProperties.put(key, value);
                            }
                            iprot.readMapEnd();
                            setProperties(readProperties);
                        } else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 7:
                        if (field.type == TType.STRUCT) {
                            TestNestedStruct readNested = new TestNestedStruct();
                            readNested.read(iprot);
                            setNested(readNested);
                        } else TProtocolUtil.skip(iprot, field.type);
                        break;
                    default:
                        TProtocolUtil.skip(iprot, field.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();
            if (!__isset_id || name == null || !__isset_active || !__isset_score) {
                throw new TProtocolException(TProtocolException.INVALID_DATA, "Required TestThriftStruct field missing");
            }
        }
        
        @Override
        public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
            oprot.writeStructBegin(new org.apache.thrift.protocol.TStruct("TestThriftStruct"));
            if (isSet(_Fields.ID)) { oprot.writeFieldBegin(new TField("id", TType.I32, (short) 1)); oprot.writeI32(id); oprot.writeFieldEnd(); }
            if (isSet(_Fields.NAME)) { oprot.writeFieldBegin(new TField("name", TType.STRING, (short) 2)); oprot.writeString(name); oprot.writeFieldEnd(); }
            if (isSet(_Fields.ACTIVE)) { oprot.writeFieldBegin(new TField("active", TType.BOOL, (short) 3)); oprot.writeBool(active); oprot.writeFieldEnd(); }
            if (isSet(_Fields.SCORE)) { oprot.writeFieldBegin(new TField("score", TType.DOUBLE, (short) 4)); oprot.writeDouble(score); oprot.writeFieldEnd(); }
            if (isSet(_Fields.TAGS)) {
                oprot.writeFieldBegin(new TField("tags", TType.LIST, (short) 5));
                oprot.writeListBegin(new TList(TType.STRING, tags.size()));
                for (String tag : tags) oprot.writeString(tag);
                oprot.writeListEnd(); oprot.writeFieldEnd();
            }
            if (isSet(_Fields.PROPERTIES)) {
                oprot.writeFieldBegin(new TField("properties", TType.MAP, (short) 6));
                oprot.writeMapBegin(new TMap(TType.STRING, TType.STRING, properties.size()));
                for (Map.Entry<String, String> entry : properties.entrySet()) { oprot.writeString(entry.getKey()); oprot.writeString(entry.getValue()); }
                oprot.writeMapEnd(); oprot.writeFieldEnd();
            }
            if (isSet(_Fields.NESTED)) { oprot.writeFieldBegin(new TField("nested", TType.STRUCT, (short) 7)); nested.write(oprot); oprot.writeFieldEnd(); }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
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
        
        @Override
        public int compareTo(TestThriftStruct other) {
            return this.name.compareTo(other.name);
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
        
        public static final Map<_Fields, FieldMetaData> metaDataMap;
        
        static {
            Map<_Fields, FieldMetaData> tmpMap = new HashMap<>();
            
            FieldMetaData valueMetaData = new FieldMetaData(
                "value", 
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.STRING)
            );
            
            FieldMetaData descriptionMetaData = new FieldMetaData(
                "description", 
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.STRING)
            );
            
            tmpMap.put(_Fields.VALUE, valueMetaData);
            tmpMap.put(_Fields.DESCRIPTION, descriptionMetaData);
            
            metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
            FieldMetaData.addStructMetaDataMap(TestNestedStruct.class, metaDataMap);
        }
        
        private String value;
        private String description;
        
        public TestNestedStruct() {
            this.value = null;
            this.description = null;
        }
        
        public TestNestedStruct(String value, String description) {
            this.value = value;
            this.description = description;
        }
        
        @Override
        public TestNestedStruct deepCopy() {
            return new TestNestedStruct(value, description);
        }
        
        @Override
        public void clear() {
            this.value = "";
            this.description = "";
        }
        
        @Override
        public _Fields fieldForId(int fieldId) {
            return _Fields.findByThriftId(fieldId);
        }
        
        @Override
        public boolean isSet(_Fields field) {
            if (field == null) return false;
            switch (field) {
                case VALUE: return value != null;
                case DESCRIPTION: return description != null;
                default: return false;
            }
        }
        
        @Override
        public Object getFieldValue(_Fields field) {
            switch (field) {
                case VALUE: return value;
                case DESCRIPTION: return description;
                default: return null;
            }
        }
        
        @Override
        public void setFieldValue(_Fields field, Object val) {
            switch (field) {
                case VALUE: setValue((String) val); break;
                case DESCRIPTION: setDescription((String) val); break;
            }
        }
        
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        @Override
        public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
            clear();
            iprot.readStructBegin();
            while (true) {
                TField field = iprot.readFieldBegin();
                if (field.type == TType.STOP) break;
                switch (field.id) {
                    case 1:
                        if (field.type == TType.STRING) setValue(iprot.readString());
                        else TProtocolUtil.skip(iprot, field.type);
                        break;
                    case 2:
                        if (field.type == TType.STRING) setDescription(iprot.readString());
                        else TProtocolUtil.skip(iprot, field.type);
                        break;
                    default:
                        TProtocolUtil.skip(iprot, field.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();
            if (value == null || description == null) {
                throw new TProtocolException(TProtocolException.INVALID_DATA, "Required TestNestedStruct field missing");
            }
        }
        
        @Override
        public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
            oprot.writeStructBegin(new org.apache.thrift.protocol.TStruct("TestNestedStruct"));
            if (isSet(_Fields.VALUE)) { oprot.writeFieldBegin(new TField("value", TType.STRING, (short) 1)); oprot.writeString(value); oprot.writeFieldEnd(); }
            if (isSet(_Fields.DESCRIPTION)) { oprot.writeFieldBegin(new TField("description", TType.STRING, (short) 2)); oprot.writeString(description); oprot.writeFieldEnd(); }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
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
        
        @Override
        public int compareTo(TestNestedStruct other) {
            return this.value.compareTo(other.value);
    }
    }

    /** Thrift enum used by tests for enum-to-integer mapping. */
    public enum Color implements org.apache.thrift.TEnum {
        RED(1),
        GREEN(2),
        BLUE(3);

        private final int value;

        Color(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        public static Color findByValue(int value) {
            for (Color color : values()) {
                if (color.value == value) return color;
            }
            return null;
        }
    }

    /** Thrift struct covering enum, binary, and UUID mappings. */
    public static class TestTypesStruct implements TBase<TestTypesStruct, TestTypesStruct._Fields> {
        public enum _Fields implements org.apache.thrift.TFieldIdEnum {
            ID((short) 1, "id"),
            COLOR((short) 2, "color"),
            DATA((short) 3, "data"),
            UUID((short) 4, "uuid");

            private final short thriftId;
            private final String fieldName;

            _Fields(short thriftId, String fieldName) {
                this.thriftId = thriftId;
                this.fieldName = fieldName;
            }

            @Override
            public short getThriftFieldId() {
                return thriftId;
            }

            @Override
            public String getFieldName() {
                return fieldName;
            }

            public static _Fields findByThriftId(int fieldId) {
                for (_Fields field : values()) {
                    if (field.thriftId == fieldId) return field;
                }
                return null;
            }

            public static _Fields findByName(String name) {
                for (_Fields field : values()) {
                    if (field.fieldName.equals(name)) return field;
                }
                return null;
            }
        }

        public static final Map<_Fields, FieldMetaData> metaDataMap;

        static {
            Map<_Fields, FieldMetaData> fields = new HashMap<>();
            fields.put(_Fields.ID, new FieldMetaData("id", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.I32)));
            fields.put(_Fields.COLOR, new FieldMetaData("color", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.EnumMetaData(TType.ENUM, Color.class)));
            fields.put(_Fields.DATA, new FieldMetaData("data", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.STRING, true)));
            fields.put(_Fields.UUID, new FieldMetaData("uuid", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new FieldValueMetaData(TType.UUID)));
            metaDataMap = java.util.Collections.unmodifiableMap(fields);
            FieldMetaData.addStructMetaDataMap(TestTypesStruct.class, metaDataMap);
        }

        private int id;
        private Color color;
        private byte[] data;
        private UUID uuid;
        private boolean issetId;

        public TestTypesStruct() { }

        public TestTypesStruct(int id, Color color, byte[] data, UUID uuid) {
            setId(id);
            setColor(color);
            setData(data);
            setUuid(uuid);
        }

        @Override
        public TestTypesStruct deepCopy() {
            return new TestTypesStruct(id, color, data, uuid);
        }

        @Override
        public void clear() {
            id = 0;
            color = null;
            data = null;
            uuid = null;
            issetId = false;
        }

        @Override
        public _Fields fieldForId(int fieldId) {
            return _Fields.findByThriftId(fieldId);
        }

        @Override
        public boolean isSet(_Fields field) {
            if (field == null) return false;
            switch (field) {
                case ID: return issetId;
                case COLOR: return color != null;
                case DATA: return data != null;
                case UUID: return uuid != null;
                default: return false;
            }
        }

        @Override
        public Object getFieldValue(_Fields field) {
            switch (field) {
                case ID: return id;
                case COLOR: return color;
                case DATA: return getData();
                case UUID: return uuid;
                default: return null;
            }
        }

        @Override
        public void setFieldValue(_Fields field, Object value) {
            switch (field) {
                case ID:
                    if (value == null) { id = 0; issetId = false; } else setId((Integer) value);
                    break;
                case COLOR: setColor((Color) value); break;
                case DATA: setData((byte[]) value); break;
                case UUID: setUuid((UUID) value); break;
                default: break;
            }
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; this.issetId = true; }

        public Color getColor() { return color; }
        public void setColor(Color color) { this.color = color; }

        public byte[] getData() { return data == null ? null : data.clone(); }
        public void setData(byte[] data) { this.data = data == null ? null : data.clone(); }

        public UUID getUuid() { return uuid; }
        public void setUuid(UUID uuid) { this.uuid = uuid; }

        @Override
        public void read(TProtocol protocol) throws TException {
            clear();
            protocol.readStructBegin();
            while (true) {
                TField field = protocol.readFieldBegin();
                if (field.type == TType.STOP) break;
                switch (field.id) {
                    case 1:
                        if (field.type == TType.I32) setId(protocol.readI32());
                        else TProtocolUtil.skip(protocol, field.type);
                        break;
                    case 2:
                        // Generated Thrift code writes enum fields with an I32 field descriptor;
                        // TType.ENUM (-1) is metadata-only and never appears on the wire.
                        if (field.type == TType.I32) setColor(Color.findByValue(protocol.readI32()));
                        else TProtocolUtil.skip(protocol, field.type);
                        break;
                    case 3:
                        if (field.type == TType.STRING) {
                            ByteBuffer bytes = protocol.readBinary();
                            byte[] value = new byte[bytes.remaining()];
                            bytes.get(value);
                            setData(value);
                        } else TProtocolUtil.skip(protocol, field.type);
                        break;
                    case 4:
                        if (field.type == TType.UUID) setUuid(protocol.readUuid());
                        else TProtocolUtil.skip(protocol, field.type);
                        break;
                    default:
                        TProtocolUtil.skip(protocol, field.type);
                }
                protocol.readFieldEnd();
            }
            protocol.readStructEnd();
            if (!issetId || color == null || data == null || uuid == null) {
                throw new TProtocolException(TProtocolException.INVALID_DATA, "Required TestTypesStruct field missing");
            }
        }

        @Override
        public void write(TProtocol protocol) throws TException {
            protocol.writeStructBegin(new org.apache.thrift.protocol.TStruct("TestTypesStruct"));
            if (isSet(_Fields.ID)) {
                protocol.writeFieldBegin(new TField("id", TType.I32, (short) 1));
                protocol.writeI32(id);
                protocol.writeFieldEnd();
            }
            if (isSet(_Fields.COLOR)) {
                protocol.writeFieldBegin(new TField("color", TType.I32, (short) 2));
                protocol.writeI32(color.getValue());
                protocol.writeFieldEnd();
            }
            if (isSet(_Fields.DATA)) {
                protocol.writeFieldBegin(new TField("data", TType.STRING, (short) 3));
                protocol.writeBinary(ByteBuffer.wrap(data));
                protocol.writeFieldEnd();
            }
            if (isSet(_Fields.UUID)) {
                protocol.writeFieldBegin(new TField("uuid", TType.UUID, (short) 4));
                protocol.writeUuid(uuid);
                protocol.writeFieldEnd();
            }
            protocol.writeFieldStop();
            protocol.writeStructEnd();
        }

        @Override
        public int compareTo(TestTypesStruct other) {
            int idComparison = Integer.compare(id, other.id);
            if (idComparison != 0) return idComparison;
            int colorComparison = Integer.compare(color == null ? 0 : color.getValue(),
                other.color == null ? 0 : other.color.getValue());
            if (colorComparison != 0) return colorComparison;
            int dataComparison = compareBytes(data, other.data);
            if (dataComparison != 0) return dataComparison;
            if (uuid == null) return other.uuid == null ? 0 : -1;
            return other.uuid == null ? 1 : uuid.compareTo(other.uuid);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof TestTypesStruct)) return false;
            TestTypesStruct that = (TestTypesStruct) other;
            return id == that.id && issetId == that.issetId && color == that.color &&
                Arrays.equals(data, that.data) && java.util.Objects.equals(uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            int result = java.util.Objects.hash(id, color, uuid, issetId);
            return 31 * result + Arrays.hashCode(data);
        }

        @Override
        public String toString() {
            return "TestTypesStruct{id=" + id + ", color=" + color + ", data=" + Arrays.toString(data) +
                ", uuid=" + uuid + "}";
        }

        private static int compareBytes(byte[] left, byte[] right) {
            if (left == right) return 0;
            if (left == null) return -1;
            if (right == null) return 1;
            int length = Math.min(left.length, right.length);
            for (int i = 0; i < length; i++) {
                int comparison = Integer.compare(left[i] & 0xff, right[i] & 0xff);
                if (comparison != 0) return comparison;
            }
            return Integer.compare(left.length, right.length);
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

    /** Creates a struct containing enum, binary, and UUID sample values. */
    public static TestTypesStruct createTestTypesStruct() {
        return new TestTypesStruct(7, Color.GREEN, "binary\u0000payload".getBytes(StandardCharsets.UTF_8),
            UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
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
