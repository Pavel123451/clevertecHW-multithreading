package ru.clevertec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import ru.clevertec.consumer.Consumer;
import ru.clevertec.manager.TopicManager;
import ru.clevertec.producer.Producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicManagerTest {
    private TopicManager topicManager;

    @BeforeEach
    public void setUp() {
        topicManager = new TopicManager();
    }

    @RepeatedTest(1000)
    public void testPublishAndConsumeMessage() throws InterruptedException {
        topicManager.createTopic("testTopic");

        CountDownLatch latch = new CountDownLatch(1);

        Consumer consumer = new Consumer(topicManager, "testTopic", latch);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Producer producer = new Producer(topicManager, "testTopic", new String[]{"Hello World"});
        Thread producerThread = new Thread(producer);
        producerThread.start();

        producerThread.join();
        latch.await();

        consumerThread.join();

        assertEquals(1, consumer.getMessages().size());
        assertEquals("Hello World", consumer.getMessages().getFirst());
    }

    @RepeatedTest(1000)
    public void testMultipleConsumersReadingAllMessages() throws InterruptedException {
        topicManager.createTopic("testTopic");

        String[] messages = new String[]{"Message 1", "Message 2", "Message 3", "Message 4", "Message 5"};

        Producer producer = new Producer(topicManager, "testTopic", messages);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        CountDownLatch latch1 = new CountDownLatch(messages.length);
        CountDownLatch latch2 = new CountDownLatch(messages.length);
        CountDownLatch latch3 = new CountDownLatch(messages.length);

        Consumer consumer1 = new Consumer(topicManager, "testTopic", latch1);
        Consumer consumer2 = new Consumer(topicManager, "testTopic", latch2);
        Consumer consumer3 = new Consumer(topicManager, "testTopic", latch3);

        Thread consumerThread1 = new Thread(consumer1);
        Thread consumerThread2 = new Thread(consumer2);
        Thread consumerThread3 = new Thread(consumer3);

        consumerThread1.start();
        consumerThread2.start();
        consumerThread3.start();

        producerThread.join();

        latch1.await();
        latch2.await();
        latch3.await();

        consumerThread1.join();
        consumerThread2.join();
        consumerThread3.join();

        assertEquals(messages.length, consumer1.getMessages().size());
        assertEquals(messages.length, consumer2.getMessages().size());
        assertEquals(messages.length, consumer3.getMessages().size());

        for (String message : messages) {
            assertTrue(consumer1.getMessages().contains(message));
            assertTrue(consumer2.getMessages().contains(message));
            assertTrue(consumer3.getMessages().contains(message));
        }
    }

    @RepeatedTest(10000)
    public void testMultipleProducersAndConsumers() throws InterruptedException {
        String topic1 = "testTopic1";
        String topic2 = "testTopic2";

        String[] messagesProducer1T1 = new String[]{"Message1-T1", "Message2-T1", "Message3-T1", "Message4-T1"};
        String[] messagesProducer2T1 = new String[]{"Message5-T1", "Message6-T1", "Message7-T1", "Message4-T8"};
        String[] messagesProducer1T2 = new String[]{"Message1-T2", "Message2-T2", "Message3-T2", "Message4-T2"};
        String[] messagesProducer2T2 = new String[]{"Message5-T2", "Message6-T2", "Message7-T2", "Message4-T2"};

        topicManager.createTopic(topic1);
        topicManager.createTopic(topic2);

        Producer producer1 = new Producer(topicManager, topic1, messagesProducer1T1);
        Producer producer2 = new Producer(topicManager, topic1, messagesProducer2T1);
        Producer producer3 = new Producer(topicManager, topic2, messagesProducer1T2);
        Producer producer4 = new Producer(topicManager, topic2, messagesProducer2T2);

        Thread producerThread1 = new Thread(producer1);
        Thread producerThread2 = new Thread(producer2);
        Thread producerThread3 = new Thread(producer3);
        Thread producerThread4 = new Thread(producer4);

        CountDownLatch latchConsumer1 = new CountDownLatch(messagesProducer1T1.length
                + messagesProducer2T1.length);
        CountDownLatch latchConsumer2 = new CountDownLatch(messagesProducer1T1.length
                + messagesProducer2T1.length);
        CountDownLatch latchConsumer3 = new CountDownLatch(messagesProducer1T2.length
                + messagesProducer2T2.length);
        CountDownLatch latchConsumer4 = new CountDownLatch(messagesProducer1T2.length
                + messagesProducer2T2.length);

        Consumer consumer1 = new Consumer(topicManager, topic1, latchConsumer1);
        Consumer consumer2 = new Consumer(topicManager, topic1, latchConsumer2);
        Consumer consumer3 = new Consumer(topicManager, topic2, latchConsumer3);
        Consumer consumer4 = new Consumer(topicManager, topic2, latchConsumer4);

        Thread consumerThread1 = new Thread(consumer1);
        Thread consumerThread2 = new Thread(consumer2);
        Thread consumerThread3 = new Thread(consumer3);
        Thread consumerThread4 = new Thread(consumer4);

        producerThread1.start();
        producerThread2.start();
        producerThread3.start();
        producerThread4.start();

        consumerThread1.start();
        consumerThread2.start();
        consumerThread3.start();
        consumerThread4.start();

        producerThread1.join();
        producerThread2.join();
        producerThread3.join();
        producerThread4.join();

        latchConsumer1.await(5, TimeUnit.SECONDS);
        latchConsumer2.await(5, TimeUnit.SECONDS);
        latchConsumer3.await(5, TimeUnit.SECONDS);
        latchConsumer4.await(5, TimeUnit.SECONDS);

        assertEquals(messagesProducer1T1.length + messagesProducer2T1.length,
                consumer1.getMessages().size());
        assertEquals(messagesProducer1T1.length + messagesProducer2T1.length,
                consumer2.getMessages().size());
        assertEquals(messagesProducer1T2.length + messagesProducer2T2.length,
                consumer3.getMessages().size());
        assertEquals(messagesProducer1T2.length + messagesProducer2T2.length,
                consumer4.getMessages().size());

        for (String message : messagesProducer1T1) {
            assertTrue(consumer1.getMessages().contains(message));
            assertTrue(consumer2.getMessages().contains(message));
        }
        for (String message : messagesProducer2T1) {
            assertTrue(consumer1.getMessages().contains(message));
            assertTrue(consumer2.getMessages().contains(message));
        }
        for (String message : messagesProducer1T2) {
            assertTrue(consumer3.getMessages().contains(message));
            assertTrue(consumer4.getMessages().contains(message));
        }
        for (String message : messagesProducer2T2) {
            assertTrue(consumer3.getMessages().contains(message));
            assertTrue(consumer4.getMessages().contains(message));
        }
    }
}




