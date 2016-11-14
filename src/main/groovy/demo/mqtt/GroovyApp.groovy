package demo.mqtt

import org.eclipse.paho.client.mqttv3.MqttException

class GroovyApp {

    @SuppressWarnings("GroovyInfiniteLoopStatement")
    public static void main(String[] args) {
        try {
            Thread.start {
                new GSubscriber(broker: "tcp://localhost:1883", subscriberId: "s1", topic: "data").build()
                while(true) {
                    sleep(1000);
                }
            }

            Thread.start {
                GPublisher pub  = new GPublisher(broker: "tcp://localhost:1883", publisherId:  "p1", topic: "data").build()
                while(true) {
                    pub.publishMessage()
                    sleep(500);
                }
            }
        } catch (MqttException | InterruptedException e) {
            e.printStackTrace()
        }

        // run for one minute
        new Timer().schedule({
            System.exit(0) // terminate
        } as TimerTask, 60 * 1000, 60000) //magic numbers are initial-delay & repeat-interval
    }
}
