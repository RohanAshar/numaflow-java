package io.numaproj.numaflow.examples.flatmap;

import io.numaproj.numaflow.flatmapper.Datum;
import io.numaproj.numaflow.flatmapper.FlatMapper;
import io.numaproj.numaflow.flatmapper.Message;
import io.numaproj.numaflow.flatmapper.MessageList;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap)
 * example : if the input message is "dog,cat", it produces two output messages
 * "dog" and "cat"
 */

@Slf4j
public class FlatMapFunction extends FlatMapper {

    @Override
    public MessageList processMessage(String[] keys, Datum datum) {
        String msg = new String(datum.getValue());
        log.info("Got following message: {}", msg);
        String[] strs = msg.split(",");
        MessageList.MessageListBuilder listBuilder = MessageList.newBuilder();
        for (String str : strs) {
            listBuilder.addMessage(new Message(str.getBytes()));
        }
        return listBuilder.build();
    }
}
