package com.sdothrift.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdothrift.config.ThriftSDOConfiguration;
import com.sdothrift.util.TestDataGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ThriftSerializerTest {
    private static final String UUID_TEXT = "123e4567-e89b-12d3-a456-426614174000";
    private static final byte[] BINARY_VALUE = "binary\u0000payload".getBytes(StandardCharsets.UTF_8);
    private static final String BASE64_VALUE = Base64.getEncoder().encodeToString(BINARY_VALUE);

    @Test
    @DisplayName("Should serialize enum, binary, and UUID to their JSON scalar forms")
    void shouldSerializeAndDeserializeEnumBinaryAndUuidAsJson() throws Exception {
        ThriftSerializer serializer = new ThriftSerializer(new ThriftSDOConfiguration());
        TestDataGenerator.TestTypesStruct source = TestDataGenerator.createTestTypesStruct();

        String json = serializer.serializeToString(source);
        JsonNode jsonNode = new ObjectMapper().readTree(json);
        assertThat(jsonNode.get("id").intValue()).isEqualTo(7);
        assertThat(jsonNode.get("color").isNumber()).isTrue();
        assertThat(jsonNode.get("color").intValue()).isEqualTo(2);
        assertThat(jsonNode.get("data").textValue()).isEqualTo(BASE64_VALUE);
        assertThat(jsonNode.get("uuid").textValue()).isEqualTo(UUID_TEXT);

        TestDataGenerator.TestTypesStruct result = serializer.deserializeFromString(
            json, TestDataGenerator.TestTypesStruct.class);
        assertThat(result).isEqualTo(source);
        assertThat(result.getId()).isEqualTo(7);
        assertThat(result.getColor()).isEqualTo(TestDataGenerator.Color.GREEN);
        assertThat(result.getData()).containsExactly(BINARY_VALUE);
        assertThat(result.getUuid()).isEqualTo(UUID.fromString(UUID_TEXT));
    }

    @ParameterizedTest(name = "{0} wire bytes round-trip")
    @ValueSource(strings = {"BINARY", "COMPACT", "TJSON"})
    @DisplayName("Should round-trip enum, binary, and UUID through Thrift wire protocols")
    void shouldRoundTripTypesOverWire(String protocolName) throws Exception {
        ThriftSDOConfiguration configuration = new ThriftSDOConfiguration();
        configuration.setThriftProtocol(protocolName.equals("TJSON")
            ? ThriftSDOConfiguration.ThriftProtocol.JSON
            : ThriftSDOConfiguration.ThriftProtocol.valueOf(protocolName));
        ThriftSerializer serializer = new ThriftSerializer(configuration);
        TestDataGenerator.TestTypesStruct source = TestDataGenerator.createTestTypesStruct();

        byte[] wireBytes = serializer.serializeToBytes(source);
        TestDataGenerator.TestTypesStruct result = serializer.deserializeFromBytes(
            wireBytes, TestDataGenerator.TestTypesStruct.class);

        assertThat(wireBytes).isNotEmpty();
        assertThat(result).isEqualTo(source);
        assertThat(result.getId()).isEqualTo(7);
        assertThat(result.getColor()).isEqualTo(TestDataGenerator.Color.GREEN);
        assertThat(result.getData()).containsExactly(BINARY_VALUE);
        assertThat(result.getUuid()).isEqualTo(UUID.fromString(UUID_TEXT));
    }
}
