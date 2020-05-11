package tr.com.rcesur.kafka.serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by xrcesur on 20.2.2019.
 */
public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    private final static Logger LOGGER = LoggerFactory.getLogger(AvroDeserializer.class);
    protected final Class<T> targetType;

    public AvroDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // No-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            Schema schema = targetType.newInstance().getSchema();
            DatumReader<T> datumReader = new SpecificDatumReader<T>(schema);
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
            stream.close();
            T result = (T) datumReader.read(null, decoder);
            return result;
        } catch (InstantiationException | IllegalAccessException | IOException e) {
            LOGGER.error(String.valueOf(e));
            return null;
        }
    }
}
