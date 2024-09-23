package ru.clevertec.producer;

import ru.clevertec.manager.TopicManager;

public class Producer implements Runnable {
    private final TopicManager topicManager;
    private final String topicName;
    private final String[] messages;

    public Producer(TopicManager topicManager, String topicName, String[] messages) {
        this.topicManager = topicManager;
        this.topicName = topicName;
        this.messages = messages;
    }

    @Override
    public void run() {
        for (String message : messages) {
            topicManager.publishMessage(topicName, message);
        }
    }
}
