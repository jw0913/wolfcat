package com.canal.processor;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

import java.util.List;

public interface Processor {
    boolean process(List<Entry> var1);
}
