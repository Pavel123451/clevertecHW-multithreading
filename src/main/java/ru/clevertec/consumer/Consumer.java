package ru.clevertec.consumer;

import ru.clevertec.manager.TopicManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Consumer implements Runnable {
    private final TopicManager topicManager;
    private final String topicName;
    private final CountDownLatch latch;
    private int lastReadIndex = -1;
    private final List<String> messages = new ArrayList<>();

    public Consumer(TopicManager topicManager, String topicName, CountDownLatch latch) {
        this.topicManager = topicManager;
        this.topicName = topicName;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            while (latch.getCount() > 0) {
                String message = topicManager.consumeMessage(topicName, lastReadIndex);
                messages.add(message);
                lastReadIndex++;
                latch.countDown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public List<String> getMessages() {
        return messages;
    }
}
