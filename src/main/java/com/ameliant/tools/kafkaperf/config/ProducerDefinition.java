package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public class ProducerDefinition extends ConfigurableWithParent {

    private String topic;
    private long messagesToSend = 10000;
    private int messageSize = 1024;
    private boolean sendBlocking = false;

    @Override
    public String toString() {
        String mergedConfig = getMergedConfig().entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .reduce("", (joined, configEntry) ->
                        (joined.equals("")) ? configEntry : joined + ", " + configEntry);

        return "ProducerDefinition{" +
                "topic='" + topic + '\'' +
                ", messagesToSend=" + messagesToSend +
                ", messageSize=" + messageSize +
                ", sendBlocking=" + sendBlocking +
                ", mergedConfig={" + mergedConfig + "}" +
                '}';
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isSendBlocking() {
        return sendBlocking;
    }

    public void setSendBlocking(boolean sendBlocking) {
        this.sendBlocking = sendBlocking;
    }

    public long getMessagesToSend() {
        return messagesToSend;
    }

    public void setMessagesToSend(long messagesToSend) {
        this.messagesToSend = messagesToSend;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

}
