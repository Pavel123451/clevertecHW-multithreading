package ru.clevertec.manager;

import ru.clevertec.model.Topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicManager {
    private final List<Topic> topics = new ArrayList<>();
    private final Map<String, Lock> topicLocks = new HashMap<>();
    private final Map<String, Condition> topicConditions = new HashMap<>();
    private final Lock globalLock = new ReentrantLock();

    public void createTopic(String topicName) {
        globalLock.lock();
        try {
            createTopicLock(topicName);
            Topic topic = new Topic(topicName);
            topics.add(topic);
        } finally {
            globalLock.unlock();
        }
    }

    public void publishMessage(String topicName, String message) {
        Lock lock = topicLocks.get(topicName);
        lock.lock();
        try {
            Topic topic = getTopicByName(topicName);
            topic.getMessages().add(message);
            topicConditions.get(topicName).signalAll();
        } finally {
            lock.unlock();
        }
    }

    public String consumeMessage(String topicName, int lastReadIndex) throws InterruptedException {
        Lock lock = topicLocks.get(topicName);
        lock.lock();
        try {
            Topic topic = getTopicByName(topicName);
            Condition condition = topicConditions.get(topicName);
            while (lastReadIndex + 1 >= topic.getMessages().size()) {
                condition.await();
            }
            return topic.getMessages().get(++lastReadIndex);
        } finally {
            lock.unlock();
        }
    }

    private void createTopicLock(String topicName) {
        if(topicLocks.containsKey(topicName)) {
            return;
        }
        Lock lock = new ReentrantLock();
        topicLocks.put(topicName, lock);
        topicConditions.put(topicName, lock.newCondition());
    }

    private Topic getTopicByName(String name) {
        for (Topic topic : topics) {
            if (topic.getName().equals(name)) {
                return topic;
            }
        }
        throw new IllegalArgumentException("Topic " + name + " not found");
    }
}


