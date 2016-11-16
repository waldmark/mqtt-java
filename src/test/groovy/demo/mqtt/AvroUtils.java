package demo.mqtt;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.*;

public class AvroUtils {

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

    public static <V> byte[] fromJavaObjectToAvro(V object, Class<V> klass) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        ReflectDatumWriter<V> w = new ReflectDatumWriter<>(klass);
        w.write(object, e);
        e.flush();
        return outputStream.toByteArray();
    }

    public static <V> V fromAvroToJavaObject(byte[] bytes, Class<V> klass) throws Exception{
        ReflectDatumReader<V> reader = new ReflectDatumReader<V>(klass);
        BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        V obj = klass.newInstance();
        reader.read(obj, binDecoder);
        return obj;
    }
}