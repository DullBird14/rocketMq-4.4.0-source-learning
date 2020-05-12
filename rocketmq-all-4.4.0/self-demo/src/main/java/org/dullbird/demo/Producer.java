package org.dullbird.demo;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

import static org.dullbird.demo.Constants.MY_GROUP;
import static org.dullbird.demo.Constants.TOPIC;

/**
 * @author dullBird
 * @version 1.0.0
 * @createTime 2020年03月19日 21:12:00
 */
public class Producer {
    public static void main(String[] args) throws MQClientException {
//        noOrderMessage();
        orderedMessage();
    }
    private static void orderedMessage() throws MQClientException {
        // 设置生产组的名字
        DefaultMQProducer producer = new DefaultMQProducer(MY_GROUP);
        // 设置nameSrv的ip
        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
//        producer.setSendMsgTimeout(1000000000);
        // 启动生产者
        producer.start();
        try {
            for (int i = 0; true; i++) {
                Message message = new Message(TOPIC, (i + ":message").getBytes());
                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
//                        Integer number = (Integer)arg;
//                        System.out.println("====================i:  " + number);
//                        System.out.println("====================message:  " + new String(msg.getBody()));
//                        int index = number % mqs.size();
//                        return mqs.get(index);
                        return mqs.get(0);
                    }
                }, i);
                System.out.println("===========success========");
                Thread.sleep(3000);
            }

//            // 发送消息
//            Message message = new Message(TOPIC, "hahahah".getBytes());
//            producer.send(message, new MessageQueueSelector() {
//                @Override
//                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
//                    return null;
//                }
//            });
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private static void noOrderMessage() throws MQClientException {
        // 设置生产组的名字
        DefaultMQProducer producer = new DefaultMQProducer(MY_GROUP);
        // 设置nameSrv的ip
        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        producer.setSendMsgTimeout(1000000000);
        // 启动生产者
        producer.start();
        try {
            // 发送消息
            Message message = new Message("noSuchTopic8", "hahahah".getBytes());
//            Message message = new Message(TOPIC, "hahahah".getBytes());
            producer.send(message);
            System.out.println("===========success========");
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
