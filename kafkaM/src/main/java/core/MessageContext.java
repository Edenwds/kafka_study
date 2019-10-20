package core;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 18:14 2019/10/8
 */
public class MessageContext <T> {

    private String topic;
    private int partition;
    private long offset;
    private T message;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
