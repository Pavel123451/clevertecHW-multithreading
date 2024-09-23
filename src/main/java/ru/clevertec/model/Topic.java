package ru.clevertec.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Topic {
    private final String name;
    private final List<String> messages = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public Topic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<String> getMessages() {
        return messages;
    }

    public Lock getLock() {
        return lock;
    }

    public Condition getCondition() {
        return condition;
    }
}
