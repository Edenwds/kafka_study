package core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 18:24 2019/10/8
 */
public class MessagedoHandle<T> implements Runnable {

    private MessageHandler<T> handler;
    private ConsumerRecords<String, T> records;

    public MessagedoHandle(MessageHandler<T> handler, ConsumerRecords<String, T> records) {
        this.handler = handler;
        this.records = records;
    }

    @Override
    public void run() {
        for (ConsumerRecord<String, T> record : records) {
            MessageContext<T> context = parseContext(record);
            handler.handle(context);
        }
    }

    private MessageContext<T> parseContext(ConsumerRecord<String,T> record) {
        MessageContext context = new MessageContext();
        context.setMessage(record.value());
        context.setTopic(record.topic());
        context.setPartition(record.partition());
        context.setOffset(record.offset());
        return context;
    }
}
