package demo.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Date;

public class Publisher extends Thread {

    MqttClient publisher;
    String topic;

    public Publisher(String broker, String id, String topic) throws MqttException {
        publisher = new MqttClient(broker, id);
        this.topic = topic;
    }

    public void publish() {
        try {
            MqttMessage message = new MqttMessage();
            Date now = new Date();
            String text = "Published @ " + now.toString();
            message.setPayload(text.getBytes());
            publisher.connect();
            publisher.publish(topic, message);
            publisher.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(1000);
                publish();
            } catch (InterruptedException e) {
                //
            }
        }
    }
}
