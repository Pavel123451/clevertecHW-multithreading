package ru.clevertec.model;

import java.util.ArrayList;
import java.util.List;

public class Topic {
    private final String name;
    private final List<String> messages = new ArrayList<>();

    public Topic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<String> getMessages() {
        return messages;
    }
}
