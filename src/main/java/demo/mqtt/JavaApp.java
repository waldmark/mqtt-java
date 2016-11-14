package demo.mqtt;

import org.eclipse.paho.client.mqttv3.MqttException;

public class JavaApp {

    public static void main(String[] args) {
        try {
            Thread subscriber = new Subscriber("tcp://localhost:1883", "s1", "data");
            Thread publisher = new Publisher("tcp://localhost:1883", "p1", "data");

            subscriber.start();
            publisher.start();
            subscriber.join();

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
