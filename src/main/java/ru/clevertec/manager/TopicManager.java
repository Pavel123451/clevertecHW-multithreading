package ru.clevertec.manager;

import ru.clevertec.model.Topic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicManager {
    private final Map<String, Topic> topics = new HashMap<>();
    private final Lock globalLock = new ReentrantLock();

    public void createTopic(String topicName) {
        globalLock.lock();
        try {
            topics.put(topicName, new Topic(topicName));
        } finally {
            globalLock.unlock();
        }
    }

    public void publishMessage(String topicName, String message) {
        Lock lock = topics.get(topicName).getLock();
        lock.lock();
        try {
            Topic topic = getTopicByName(topicName);
            topic.getMessages().add(message);
            topics.get(topicName).getCondition().signalAll();
        } finally {
            lock.unlock();
        }
    }

    public String consumeMessage(String topicName, int lastReadIndex) throws InterruptedException {
        Lock lock = topics.get(topicName).getLock();
        lock.lock();
        try {
            Topic topic = getTopicByName(topicName);
            Condition condition = topics.get(topicName).getCondition();
            while (lastReadIndex + 1 >= topic.getMessages().size()) {
                condition.await();
            }
            return topic.getMessages().get(++lastReadIndex);
        } finally {
            lock.unlock();
        }
    }

    private Topic getTopicByName(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        return topic;
    }
}


