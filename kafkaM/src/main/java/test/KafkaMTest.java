package test;

import config.KafkaProperties;
import core.KafkaManager;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 17:50 2019/10/9
 */
public class KafkaMTest {

    public static void main(String[] args) {
        KafkaProperties kafkaProperties = buildProperties();
        KafkaManager manager = new KafkaManager(kafkaProperties);
        for (int i = 0; i < 10; i++) {
            manager.sendMessage("test666", "kafkaM test NO." + i);
        }
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static KafkaProperties buildProperties() {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers("192.168.186.129:9093,192.168.186.129:9094,192.168.186.129:9095");
        return properties;
    }
}
