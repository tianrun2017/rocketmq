package org.apache.rocketmq.example.toby;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 生产者
 *
 * @author fanqinhai
 * @since 2020/5/4 10:37 下午
 */

public class ConcurrentlyProducer2 {

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("groupTest");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();

        Message msg = new Message("topicTest6", "tagTest", ("testMessage1").getBytes());
        SendResult sendResult = producer.send(msg);
        System.out.printf("%s%n", sendResult);


        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}

