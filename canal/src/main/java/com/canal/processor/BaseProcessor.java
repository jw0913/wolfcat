package com.canal.processor;

import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.canal.domain.TransactionBody;
import com.canal.extend.TransactionIdentify;
import com.canal.extend.TransactionJudgeHandler;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class BaseProcessor implements Processor,InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(BaseProcessor.class);
    protected static final String SEP;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected static final ThreadLocal<SimpleDateFormat> format;
    protected static String row_format;
    protected static String transaction_format;
    private TransactionJudgeHandler transactionJudgeHandler = new TransactionJudgeHandler() {
        @Override
        public TransactionIdentify judge(Entry entry) {
            return new TransactionIdentify() {
                private ConcurrentSkipListSet ids = new ConcurrentSkipListSet();
                @Override
                public Object getType() {
                    return new Object();
                }

                @Override
                public Set<String> getBusniessIds() {
                    return this.ids;
                }
            };
        }

        @Override
        public TransactionIdentify buildSpecialTransactionIdentify() {
            return new TransactionIdentify(){
                private ConcurrentSkipListSet ids = new ConcurrentSkipListSet();
                @Override
                public Object getType() {
                    return new Object();
                }

                @Override
                public Set<String> getBusniessIds() {
                    return this.ids;
                }
            };
        }
    };
    static {
        SEP = SystemUtils.LINE_SEPARATOR;
        format = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
        };
        row_format = null;
        transaction_format = null;
        row_format = "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms";
        transaction_format = "============================> binlog[{}:{}] , executeTime : {} , delay : {}ms";
    }
    public BaseProcessor(){}

    @Override
    public boolean process(List<Entry> entries) throws Exception{
        List<TransactionBody> transactionBodies = new LinkedList<>();
        TransactionBody body = null;
        Iterator<Entry> iterator = entries.iterator();

        while (iterator.hasNext()){
            Entry entry = iterator.next();
            EntryType entryType = entry.getEntryType();
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;

            switch (entryType){
                case TRANSACTIONBEGIN:
                    if(!this.transactionJudgeHandler.ignoreTransaction()){
                        this.appendTransactionBody(transactionBodies,body);
                        body = new TransactionBody();
                        TransactionBegin begin = null;

                        try {
                            begin = TransactionBegin.parseFrom(entry.getStoreValue());
                            body.setBegin(begin);
                        } catch (InvalidProtocolBufferException var15) {
                            throw new RuntimeException("parse event has an error , data:" + entry.toString(), var15);
                        }

                        if (logger.isDebugEnabled()) {
                            logger.debug(transaction_format, new Object[]{entry.getHeader().getLogfileName(), String.valueOf(entry.getHeader().getLogfileOffset()), String.valueOf(((SimpleDateFormat)format.get()).format(new Date(entry.getHeader().getExecuteTime()))), String.valueOf(delayTime)});
                            logger.debug("TRANSACTION BEGIN ==========>  TransactionId : [{}], Thread id: [{}].", begin.getTransactionId(), begin.getThreadId());
                        }
                    }
                    break;
                case TRANSACTIONEND:
                    if (!this.transactionJudgeHandler.ignoreTransaction()) {
                        if (body == null) {
                            body = new TransactionBody();
                        }

                        TransactionEnd end = null;

                        try {
                            end = TransactionEnd.parseFrom(entry.getStoreValue());
                            body.setEnd(end);
                        } catch (InvalidProtocolBufferException var14) {
                            throw new RuntimeException("parse event has an error , data:" + entry.toString(), var14);
                        }

                        if (logger.isDebugEnabled()) {
                            logger.debug("TRANSACTION   END ============> TransactionId: [{}].", end.getTransactionId());
                            logger.debug(transaction_format, new Object[]{entry.getHeader().getLogfileName(), String.valueOf(entry.getHeader().getLogfileOffset()), String.valueOf(((SimpleDateFormat)format.get()).format(new Date(entry.getHeader().getExecuteTime()))), String.valueOf(delayTime)});
                        }
                    }
                    break;
                case ROWDATA:
                    if (body == null) {
                        body = new TransactionBody();
                    }

                    if (body.getType() == null) {
                        TransactionIdentify identify = this.transactionJudgeHandler.judge(entry);
                        if (identify != null) {
                            if (this.transactionJudgeHandler.ignoreJudgeCanalEntry()) {
                                body.setType(identify);
                            } else {
                                body.getRowChanges().add(this.parseEntry(entry));
                            }
                        } else {
                            body.getRowChanges().add(this.parseEntry(entry));
                        }
                    } else {
                        body.getRowChanges().add(this.parseEntry(entry));
                    }
                    break;
                default:
                    logger.error("unknown entryType : {}", entryType);
                    break;
            }
        }
        this.appendTransactionBody(transactionBodies, body);
        logger.error("transactionBodies size = " + transactionBodies.size());
        return this.processTransactionBody(transactionBodies);
    }

    private void appendTransactionBody(List<TransactionBody> transactionBodies, TransactionBody body) throws Exception {
        if (body != null) {
            if (!body.full() && !body.getRowChanges().isEmpty()) {
                throw new Exception("不完整的事务块");
            }

            body.setType(body.getType() == null ? this.transactionJudgeHandler.buildSpecialTransactionIdentify() : body.getType());
            transactionBodies.add(body);
        }

    }

    private RowChange parseEntry(Entry entry) {
        RowChange rowChange = null;

        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception var6) {
            throw new RuntimeException("parse event has an error , data:" + entry.toString(), var6);
        }

        if (logger.isDebugEnabled()) {
            EventType eventType = rowChange.getEventType();
            logger.debug("============================> SchemaName : [{}], TableName = [{}], EventType : [{}] ", new Object[]{entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType});
            Iterator var4 = rowChange.getRowDatasList().iterator();

            while(var4.hasNext()) {
                RowData rowData = (RowData)var4.next();
                logger.debug("============================> RowData");
                if (eventType == EventType.DELETE) {
                    logger.debug("============================> RowData Before");
                    this.printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    logger.debug("============================> RowData After");
                    this.printColumn(rowData.getAfterColumnsList());
                } else if (eventType == EventType.UPDATE) {
                    logger.debug("============================> RowData Before");
                    this.printColumn(rowData.getBeforeColumnsList());
                    logger.debug("============================> RowData After");
                    this.printColumn(rowData.getAfterColumnsList());
                } else if (eventType == EventType.QUERY) {
                    this.isQueryEventTypeOrDdl(rowChange);
                } else {
                    this.isQueryEventTypeOrDdl(rowChange);
                }
            }
        }

        return rowChange;
    }

    public boolean processTransactionBody(List<TransactionBody> transactionBodies) throws Exception {
        return true;
    }

    protected void printColumn(List<Column> columns) {
        if (logger.isDebugEnabled()) {
            Iterator var2 = columns.iterator();

            while(var2.hasNext()) {
                Column column = (Column)var2.next();
                this.printColumn(column);
            }
        }

    }

    protected void printColumn(Column column) {
        if (logger.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }

            builder.append("    getIsNull=" + column.getIsNull());
            builder.append("    hasIsNull=" + column.hasIsNull());
            builder.append("    hasValue=" + column.hasValue());
            logger.debug(builder.toString());
        }

    }

    protected boolean isQueryEventTypeOrDdl(RowChange rowChange) {
        boolean yes = false;
        EventType eventType = rowChange.getEventType();
        if (eventType == EventType.QUERY || rowChange.getIsDdl()) {
            logger.warn("eventType = {},getIsDdl = {}  sql ----> {}" + SEP, new Object[]{eventType, rowChange.getIsDdl(), rowChange.getSql()});
            yes = true;
        }

        return yes;
    }
    @Override
    public void afterPropertiesSet() throws Exception {
    }

    public void setTransactionJudgeHandler(TransactionJudgeHandler transactionJudgeHandler) {
        this.transactionJudgeHandler = transactionJudgeHandler;
    }
}
