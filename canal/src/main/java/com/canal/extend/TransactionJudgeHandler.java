package com.canal.extend;

import com.alibaba.otter.canal.protocol.CanalEntry;

public interface TransactionJudgeHandler {
   TransactionIdentify judge(CanalEntry.Entry entry);
    default boolean ignoreJudgeCanalEntry(){
        return false;
    }
    default boolean ignoreTransaction(){
        return false;
    }
    TransactionIdentify buildSpecialTransactionIdentify();
}
