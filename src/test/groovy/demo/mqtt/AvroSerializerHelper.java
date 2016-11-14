package demo.mqtt;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;

public class AvroSerializerHelper {

    public static <V> byte[] toBytes(V v,Class<V> cls) throws Exception{
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        SpecificDatumWriter<V> writer = new SpecificDatumWriter<V>(cls);
        BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        writer.write(v, binEncoder);
        binEncoder.flush();

        return bout.toByteArray();
    }

    public static <V> V fromBytes(byte[] bytes, Class<V> cls) throws Exception{
        SpecificDatumReader<V> reader = new SpecificDatumReader<V>(cls);

        BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
        V val = cls.newInstance();
        reader.read(val, binDecoder);

        return val;
    }
}