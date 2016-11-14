package demo.mqtt;

import org.eclipse.paho.client.mqttv3.*;


public class Subscriber extends Thread implements MqttCallback {

    public Subscriber(String broker, String id, String topic) throws MqttException {
        MqttClient subscriber = new MqttClient(broker, id);
        subscriber.setCallback(this);
        subscriber.connect();
        subscriber.subscribe(topic);
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println(cause.getMessage());
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("FROM TOPIC: " + topic + " MESSAGE: " + message);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        try {
            System.out.println(token.getMessage().toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                //
            }
        }

    }
}
