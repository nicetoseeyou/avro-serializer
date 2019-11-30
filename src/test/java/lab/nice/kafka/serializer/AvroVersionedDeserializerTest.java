package lab.nice.kafka.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AvroVersionedDeserializerTest {

    private GenericRecord v1;
    private GenericRecord v2;

    private Deserializer<Object> deserializer;

    private String topic = "avro-topic";
    private String schemaStr_v1 = "{\"type\":\"record\",\"name\":\"Avro\",\"namespace\":\"lab.nice.avro\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"age\",\"type\":[\"null\",\"int\"]}]}";
    private String schemaStr_v2 = "{\"type\":\"record\",\"name\":\"Kafka\",\"namespace\":\"lab.nice.avro\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"age\",\"type\":[\"null\",\"int\"]},{\"name\":\"gender\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    private Schema schema_v1 = new Schema.Parser().parse(schemaStr_v1);
    private Schema schema_v2 = new Schema.Parser().parse(schemaStr_v2);
    private Map<String, Object> configs = new HashMap<>();

    @Before
    public void setUp() {
        this.deserializer = new AvroVersionedDeserializer();

        v1 = new GenericData.Record(schema_v1);
        v1.put("name", "Avro");
        v1.put("age", 18);

        v2 = new GenericData.Record(schema_v2);
        v2.put("name", "Kafka");
        v2.put("age", 18);
        v2.put("gender", "unknown");
    }

    @Test(expected = SerializationException.class)
    public void readerSchemaMustConfigured() {
        deserializer.configure(configs, false);
    }

    @Test(expected = SerializationException.class)
    public void readerSchemaMustConcrete() {
        configs.put(SederProperty.KEY_READER_SCHEMA, "");
        deserializer.configure(configs, false);
    }

    @Test
    public void deserializePrimitive() throws IOException {
        final Schema schema_primitive_long = Schema.create(Schema.Type.LONG);
        final byte[] bytes_long = toAvro(schema_primitive_long, 10L);
        configs.put(SederProperty.KEY_READER_SCHEMA, schema_primitive_long.toString());
        deserializer.configure(configs, false);
        final Object deser_long = deserializer.deserialize(topic, bytes_long);
        Assert.assertTrue("It should be a LONG type", deser_long instanceof Long);
        Assert.assertEquals("It should be 10", 10L, deser_long);
    }

    @Test
    public void deserializeRecord() throws IOException {
        final byte[] v1_bytes = toAvro(schema_v1, v1);
        configs.put(SederProperty.KEY_READER_SCHEMA, schemaStr_v1);
        deserializer.configure(configs, false);
        final Object deser_v1 = deserializer.deserialize(topic, v1_bytes);
        Assert.assertTrue("It should be a GenericRecord", deser_v1 instanceof GenericRecord);
        Assert.assertEquals("It should equals to v1", v1, deser_v1);
    }

    @Test
    public void deserializeVersionedRecords() throws IOException {
        final byte[] v1_bytes = toVersionedAvro(schema_v1, 1, v1);
        final byte[] v2_bytes = toVersionedAvro(schema_v2, 2, v2);
        configs.put(SederProperty.KEY_READER_SCHEMA, schemaStr_v1);
        configs.put("writer.schemas.1.version", "1");
        configs.put("writer.schemas.1.value", schemaStr_v1);
        configs.put("writer.schemas.2.version", "2");
        configs.put("writer.schemas.2.value", schemaStr_v2);
        deserializer.configure(configs, false);
        final Object deser_v1 = deserializer.deserialize(topic, v1_bytes);
        final Object deser_v2 = deserializer.deserialize(topic, v2_bytes);
        Assert.assertTrue("Deserialized v1 should be instance of GenericRecord", deser_v1 instanceof GenericRecord);
        Assert.assertTrue("Deserialized v2 should be instance of GenericRecord", deser_v2 instanceof GenericRecord);
        Assert.assertEquals("Deserialized v1 should equals to v1", v1, deser_v1);
        Assert.assertNull("Gender should not exists in deserialized v2", ((GenericRecord) deser_v2).getSchema().getField("gender"));
    }

    @After
    public void tearDown() {
        this.configs.clear();
        this.deserializer = null;
    }

    private byte[] toAvro(final Schema schema, final Object data) throws IOException {
        if (null == data) {
            return null;
        } else {
            try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                final DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
                writer.write(data, encoder);
                encoder.flush();
                return outputStream.toByteArray();
            }
        }
    }

    private byte[] toVersionedAvro(final Schema schema, final Integer version, final Object data) throws IOException {
        if (null == data) {
            return null;
        } else {
            try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                final DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
                encoder.writeInt(version);
                writer.write(data, encoder);
                encoder.flush();
                return outputStream.toByteArray();
            }
        }
    }
}
