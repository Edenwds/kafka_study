package core;

import java.util.List;
import java.util.Map;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 18:07 2019/10/8
 */
public interface IKafkaManager {

    int sendMessage(String topic, String message);

    int sendMessage(String topic, List<String> messages);

    int sendMessage(String topic, String key, String message);

    int sendMessage(String topic, Map<String, String> messages);

    int sendMessage(String topic, Integer partition, List<String> messages);

    int sendMessage(String topic, Integer partition, Map<String, String> messages);

    <T> int sendMessageObject(String topic, T message);

    <T> int sendMessageObject(String topic, List<T> messages);

    <T> int sendMessageObject(String topic, Integer partition, List<T> messages);

    <T> int sendMessageObject(String topic, String key, T message);

    <T> int sendMessageObject(String topic, Integer partition, Map<String, T> messages);

    <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic);

    <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic, String clientId);

    <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic, String clientId, Integer concurrency);

    <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic, String clientId, Integer concurrency, Integer partitionCount);

    int unSubscribe(String topic);

    int unSubscribe(String topic, String groupId);

    int unSubscribe(String topic, String groupId, String clientId);
}
