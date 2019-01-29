package com.canal.domain;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.canal.extend.TransactionIdentify;

import java.util.LinkedList;
import java.util.List;

public class TransactionBody {
    private CanalEntry.TransactionBegin begin;
    private List<CanalEntry.RowChange> rowChanges = new LinkedList();
    private CanalEntry.TransactionEnd end;
    private TransactionIdentify type;

    public TransactionBody() {
    }
    public boolean full() {
        return this.begin != null && this.end != null;
    }

    public boolean ignore() {
        return false;
    }

    public CanalEntry.TransactionBegin getBegin() {
        return begin;
    }

    public void setBegin(CanalEntry.TransactionBegin begin) {
        this.begin = begin;
    }

    public List<CanalEntry.RowChange> getRowChanges() {
        return rowChanges;
    }

    public void setRowChanges(List<CanalEntry.RowChange> rowChanges) {
        this.rowChanges = rowChanges;
    }

    public CanalEntry.TransactionEnd getEnd() {
        return end;
    }

    public void setEnd(CanalEntry.TransactionEnd end) {
        this.end = end;
    }

    public TransactionIdentify getType() {
        return type;
    }

    public void setType(TransactionIdentify type) {
        this.type = type;
    }
}
