package demo.mqtt

import groovy.transform.Canonical
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttMessage

@Canonical
class GPublisher {

    String broker
    String topic
    String publisherId
    private MqttClient publisher

//    public GPublisher(String broker, String id, String topic) throws MqttException {
//        publisher = new MqttClient(broker, id);
//        this.topic = topic;
//    }
//
    GPublisher build() {
        publisher = new MqttClient(broker, publisherId)
        this
    }

    public void publishMessage() {
        try {
            MqttMessage message = new MqttMessage()
            Date now = new Date()
            String text = "Published @ " + now.toString()
            message.payload = text.bytes
            publisher.connect()
            publisher.publish(topic, message)
            publisher.disconnect()
        } catch (MqttException e) {
            e.printStackTrace()
        }
    }
//
//    @Override
//    public void run() {
//        while(true) {
//            try {
//                println Thread.name
//                Thread.sleep(1000)
//                publish();
//            } catch (InterruptedException e) {
//                //
//            }
//        }
//    }

}
