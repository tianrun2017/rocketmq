package org.apache.rocketmq.example.toby;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费者
 *
 * @author fanqinhai
 * @since 2020/5/4 10:36 下午
 */

public class ConcurrentlyConsumer2 {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("groupTest");

        // Specify name server addresses.
        consumer.setNamesrvAddr("localhost:9876");
        // Subscribe one more more topics to consume.
        consumer.subscribe("topicTest6", "*");

        //Concurrently多线程(无序消费)
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("%s,msg:%s,queueId:%s %n", Thread.currentThread().getName(), new String(msg.getBody()), msg.getQueueId());
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //Launch the consumer instance.
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}

