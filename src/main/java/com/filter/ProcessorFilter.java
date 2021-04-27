package com.filter;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ProcessorFilter implements Processor<String, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        if (Integer.parseInt(value) > 10) {
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}