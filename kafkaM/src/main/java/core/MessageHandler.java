package core;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 18:09 2019/10/8
 */
public interface MessageHandler<T> {
    void handle(MessageContext<T> messageContext);
}
