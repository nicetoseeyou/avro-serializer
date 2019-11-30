# avro-serializer
Avro serializer and deserializer for Kafka
### Serializer
N/A
### Deserializer
1. AvroVersionedDeserializer
    - full class name: lab.nice.kafka.serializer.AvroVersionedDeserializer
    - usage:
        - single schema data  
        Configure the reader schema (in string) with property '_reader.schema_'  
        Example:  
        ```java
      configs.put("reader.schema", "{\"type\":\"long\"}");
      deserializer.configure(configs, false);
        ```   
        - multi schema data  
        Firstly configure the reader schema (in string) with property '_reader.schema_'.  
        Secondly configure the versioned writer schemas (in string). Configure the writer schema's version number with property '_writer.schemas.${index}.version_' and the writer schema with property '_writer.schemas.${index}.value_'.  
        Example:  
        ```java
      configs.put("reader.schema", "{\"namespace\":\"lab.nice.avro\",\"type\":\"record\",\"name\":\"Avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}");
      configs.put("writer.schemas.1.version", "1");
      configs.put("writer.schemas.1.value", "{\"namespace\":\"lab.nice.avro\",\"type\":\"record\",\"name\":\"Avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}");
      configs.put("writer.schemas.2.version", "2");
      configs.put("writer.schemas.2.value", "{\"namespace\":\"lab.nice.avro\",\"type\":\"record\",\"name\":\"Avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"gender\",\"type\":\"string\"}]}");
      deserializer.configure(configs, false);
        ```