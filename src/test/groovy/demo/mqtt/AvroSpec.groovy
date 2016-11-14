package demo.mqtt

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.*
import org.apache.avro.reflect.ReflectData
import org.apache.avro.reflect.ReflectDatumWriter
import spock.lang.Shared
import spock.lang.Specification

class AvroSpec extends Specification {

    @Shared
    Schema avsc
    @Shared
    JsonSlurper jsonSlurper


    def setupSpec() {
        avsc = ReflectData.get().getSchema(Alarm.class)
        jsonSlurper = new JsonSlurper()
    }

    /*
     *
     * Test converting JSON string into an Avro byte array
     */
    def "test encoding JSON string into Avro byte"() {
        given:
        Alarm myAlarm = new Alarm(name: "AL-999", code: 022, description: "test alarm 022")
        String json =  new JsonBuilder( myAlarm).toString()

        when: "the json is converted to Avro binary"
        byte[] avro = fromJasonStringToAvro(json, avsc)

        then: "the result is the correct byte array"
        avro.toString() == '[12, 65, 76, 45, 57, 57, 57, 36, 28, 116, 101, 115, 116, 32, 97, 108, 97, 114, 109, 32, 48, 50, 50]'
    }

    /*
     *
     * Test converting Avro byte array into JSON
     */
    def "encode Avro byteArray into JSON"() {
        given:
        def avroByteArray = [12, 65, 76, 45, 57, 57, 57, 36, 28, 116, 101, 115, 116, 32, 97, 108, 97, 114, 109, 32, 48, 50, 50] as byte[]

        when: "the Avro binary is converted back to a JSON string"
        String avroJSON = fromAvroToJSONString(avroByteArray, avsc)
        def jsonObject = jsonSlurper.parseText(avroJSON.toString())

        then: "the JSON string can be slurped and the correct values are returned"
        jsonObject
        'AL-999' == jsonObject.name
        022 == jsonObject.code
        'test alarm 022' == jsonObject.description
    }

    /*
     *
     * Test round trip conversion
     */
    def "test round trip: converting JSON to Avro to JSON"() {
        given: "a JSON string"
        Alarm myAlarm = new Alarm(name: "AL-999", code: 022, description: "test alarm 022")
        String json =  new JsonBuilder( myAlarm).toString()

        when: "the JSON is converted to Avro binary"
        byte[] avro = fromJasonStringToAvro(json, avsc)

        then: "the result is the correct byte array"
        avro.toString() == '[12, 65, 76, 45, 57, 57, 57, 36, 28, 116, 101, 115, 116, 32, 97, 108, 97, 114, 109, 32, 48, 50, 50]'

        when: "the Avro binary is converted back to a JSON string"
        String avroJSON = fromAvroToJSONString(avro, avsc)
        def jsonObject = jsonSlurper.parseText(avroJSON.toString())

        then: "the JSON string can be slurped and the correct values are returned"
        jsonObject
        'AL-999' == jsonObject.name
        022 == jsonObject.code
        'test alarm 022' == jsonObject.description
    }


    /*
     * Test converting Java object into Avro byte array
     */
    def "convert Java object to Avro"() {
        given:
        Alarm alarm2 = new Alarm(name: "AL-222", code: 044, description: "test alarm 044")

        when:
        byte[] avroByteArray = fromJavaObjectToAvro(alarm2, Alarm.class)

        then:
        avroByteArray.toString() == '[12, 65, 76, 45, 50, 50, 50, 72, 28, 116, 101, 115, 116, 32, 97, 108, 97, 114, 109, 32, 48, 52, 52]'
    }

    static byte[] fromJasonStringToAvro(json, avsc) throws Exception {
        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);
        Decoder decoder = DecoderFactory.get().jsonDecoder(avsc, din);

        DatumReader<Object> reader = new GenericDatumReader<Object>(avsc);
        Object datum = reader.read(null, decoder);

        GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(avsc);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        w.write(datum, e);
        e.flush();

        return outputStream.toByteArray();
    }

    static String fromAvroToJSONString(avroByteArray, avsc) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroByteArray, null)
        DatumReader<?> reader = new GenericDatumReader<GenericRecord>(avsc)
        GenericRecord result = reader.read(null, decoder)
        result.toString()
    }

    static byte[] fromJavaObjectToAvro(object, klass) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
        ReflectDatumWriter w = new ReflectDatumWriter<?>(klass);
        w.write(object, e)
        e.flush()
        return outputStream.toByteArray();
    }

}
