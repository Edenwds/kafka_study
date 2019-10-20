package test;

import config.KafkaProperties;
import core.KafkaManager;
import core.MessageContext;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 11:55 2019/10/10
 */
public class KafkaMTest2 {

    public static void main(String[] args) {
        KafkaProperties kafkaProperties = buildProperties();
        KafkaManager manager = new KafkaManager(kafkaProperties);
        manager.subscribe((MessageContext<String> msg) -> System.out.println(msg.getMessage()), "1236", "test666");
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        manager.unSubscribe("test666", "1236");
        try {
            Thread.sleep(10 * 1000);
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
