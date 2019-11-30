package lab.nice.kafka.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Kafka Avro deserializer with versioned writer schemas.
 */
public class AvroVersionedDeserializer implements Deserializer<Object> {
    private Schema readerSchema;
    private Map<Integer, Schema> writerSchemas = new HashMap<>();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        if (configs.containsKey(SederProperty.KEY_READER_SCHEMA)) {
            final String readerSchemaStr = (String) configs.get(SederProperty.KEY_READER_SCHEMA);
            if (StringUtils.isBlank(readerSchemaStr)) {
                throw new SerializationException("Invalid reader schema: " + readerSchemaStr);
            } else {
                this.readerSchema = new Schema.Parser().parse(readerSchemaStr);
            }
        } else {
            throw new SerializationException("Reader schema not found. Please configure reader schema with property '"
                    + SederProperty.KEY_READER_SCHEMA + "'");
        }
        retrieveWriterSchemas(configs);
    }

    @Override
    public Object deserialize(final String topic, final byte[] data) {
        if (null == data) {
            return null;
        } else {
            final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            final boolean versioned = !this.writerSchemas.isEmpty();
            try {
                DatumReader<Object> reader;
                if (versioned) {
                    final Integer version = decoder.readInt();
                    final Schema writerSchema = this.writerSchemas.get(version);
                    if (null == writerSchema) {
                        throw new SerializationException("Invalid schema version: " + version);
                    } else {
                        reader = new SpecificDatumReader<>(writerSchema, this.readerSchema);
                    }
                } else {
                    reader = new GenericDatumReader<>(this.readerSchema);
                }
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new SerializationException("Failed to deserialize data", e);
            }
        }
    }

    @Override
    public Object deserialize(final String topic, final Headers headers, final byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public void close() {

    }

    /**
     * Retrieve writer schemas from incoming configurations.
     *
     * @param configs the incoming configurations
     */
    private void retrieveWriterSchemas(final Map<String, ?> configs) {
        this.writerSchemas.clear();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            final Matcher matcher = SederProperty.PATTERN_WRITER_SCHEMA_VERSION.matcher(entry.getKey());
            if (matcher.matches()) {
                final String versionStr = (String) entry.getValue();

                final String index = matcher.group(1);
                final String valueProp = String.format(SederProperty.FORMAT_WRITER_SCHEMA_VALUE, index);
                final String writerSchemaStr = (String) configs.get(valueProp);
                if (StringUtils.isNotBlank(writerSchemaStr)) {
                    final Integer version = Integer.parseInt(versionStr);
                    final Schema writerSchema = new Schema.Parser().parse(writerSchemaStr);
                    this.writerSchemas.put(version, writerSchema);
                }
            }
        }
    }
}
