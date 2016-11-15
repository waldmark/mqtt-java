package demo.mqtt;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

public class AvroUtils {

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

        BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        V val = cls.newInstance();
        reader.read(val, binDecoder);

        return val;
    }

    public static byte[] fromJasonStringToAvro(String json, Schema avsc) throws Exception {
        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);
        Decoder decoder = DecoderFactory.get().jsonDecoder(avsc, din);

        DatumReader<Object> reader = new GenericDatumReader<>(avsc);
        Object datum = reader.read(null, decoder);

        GenericDatumWriter<Object> w = new GenericDatumWriter<>(avsc);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        w.write(datum, e);
        e.flush();

        return outputStream.toByteArray();
    }

    public static String fromAvroToJSONString(byte[] avroByteArray, Schema avsc) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroByteArray, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(avsc);
        GenericRecord result = reader.read(null, decoder);
        return result.toString();
    }

    public static byte[] fromJavaObjectToAvro(Object object, Class<Object> klass) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        ReflectDatumWriter<Object> w = new ReflectDatumWriter<>(klass);
        w.write(object, e);
        e.flush();
        return outputStream.toByteArray();
    }
}