package com.canal.processor;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.canal.domain.TransactionBody;

import java.util.List;

public interface Processor {
    boolean process(List<Entry> entries) throws Exception;

    boolean processsTransactionBoty(List<TransactionBody> transactionBodies) throws Exception;
}
