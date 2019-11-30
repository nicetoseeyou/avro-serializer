package lab.nice.kafka.serializer;

import java.util.regex.Pattern;

public final class SederProperty {
    public static final String KEY_READER_SCHEMA = "reader.schema";

    public static final String FORMAT_WRITER_SCHEMA_VALUE = "writer.schemas.%s.value";
    public static final Pattern PATTERN_WRITER_SCHEMA_VERSION = Pattern.compile("writer\\.schemas\\.(\\d+)\\.version");

    private SederProperty() {
    }
}
