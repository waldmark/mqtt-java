package demo.mqtt

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import spock.lang.Shared
import spock.lang.Specification

import static demo.mqtt.AvroUtils.*

class AvroSpec extends Specification {

    @Shared
    Schema avsc

    def setupSpec() {
        avsc = ReflectData.get().getSchema(Alarm.class)
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
        def jsonSlurper = new JsonSlurper()
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
        def jsonSlurper = new JsonSlurper()
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
        byte[] avroByteArray = fromJavaObjectToAvro(alarm2, alarm2.class)

        then:
        avroByteArray.toString() == '[12, 65, 76, 45, 50, 50, 50, 72, 28, 116, 101, 115, 116, 32, 97, 108, 97, 114, 109, 32, 48, 52, 52]'
    }

    /*
  * Test converting Avro byte array into Java object
  */
    def "convert Avro to Java object"() {
        given:
        Alarm expectedAlarm = new Alarm(name: "AL-222", code: 044, description: "test alarm 044")
        byte[] providedAvroBytes = [12, 65, 76, 45, 50, 50, 50, 72, 28, 116, 101, 115, 116, 32, 97, 108, 97, 114, 109, 32, 48, 52, 52]

        when:
        Alarm result = fromAvroToJavaObject(providedAvroBytes, expectedAlarm.class)

        then:
        result == expectedAlarm
    }
}
