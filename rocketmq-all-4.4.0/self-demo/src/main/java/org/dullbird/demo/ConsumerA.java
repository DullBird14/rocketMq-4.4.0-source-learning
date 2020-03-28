package org.dullbird.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static main.java.org.dullbird.demo.Constants.MY_GROUP;
import static main.java.org.dullbird.demo.Constants.TOPIC;

/**
 * @author dullBird
 * @version 1.0.0
 * @createTime 2020年03月22日 15:30:00
 */
public class ConsumerA {
    private static AtomicInteger count = new AtomicInteger(1);
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(MY_GROUP);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe(TOPIC, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                int increment = count.getAndIncrement();
                System.out.printf("===================第%d次消息接收==========================\n", increment);
                for (MessageExt msg : msgs) {
                    byte[] body = msg.getBody();
                    System.out.printf("收到的消息id=%s, 消息的内容是%s\n", msg.getMsgId(), new String(body));
                }
                System.out.printf("===================第%d次消息接收结束==========================\n", increment);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

            }
        });
        consumer.start();
        System.out.printf("===================consumer start==========================\n");

//
//        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(MY_GROUP + 2);
//        consumer2.setNamesrvAddr("127.0.0.1:9876");
//        consumer2.subscribe(TOPIC, "*");
//        consumer2.registerMessageListener(new MessageListenerConcurrently() {
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                int increment = count.getAndIncrement();
//                System.out.printf("===================第%d次消息接收==========================\n", increment);
//                for (MessageExt msg : msgs) {
//                    byte[] body = msg.getBody();
//                    System.out.printf("收到的消息id=%s, 消息的内容是%s\n", msg.getMsgId(), new String(body));
//                }
//                System.out.printf("===================第%d次消息接收结束==========================\n", increment);
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//
//            }
//        });
//        consumer2.start();
    }
}
