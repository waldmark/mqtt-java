package demo.mqtt

import groovy.transform.Canonical
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage

@Canonical
class GSubscriber implements MqttCallback {

    String broker
    String subscriberId
    String topic

    GSubscriber build() {
        MqttClient subscriber = new MqttClient(broker, subscriberId)
        subscriber.setCallback(this)
        subscriber.connect()
        subscriber.subscribe(topic)
        this
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println(cause.getMessage())
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("FROM TOPIC: " + topic + " MESSAGE: " + message)
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        try {
            System.out.println(token.getMessage().toString())
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Override
//    public void run() {
//        while(true) {
//            try {
//                println Thread.name
//                Thread.sleep(100)
//            } catch (InterruptedException e) {
//                //
//            }
//        }
//
//    }
//    @Override
//    void run() {
//        println 'sub start'
//    }
}
