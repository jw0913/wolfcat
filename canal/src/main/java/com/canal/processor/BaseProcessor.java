package com.canal.processor;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.canal.extend.TransactionIdentify;
import com.canal.extend.TransactionJudgeHandler;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;
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
        public TransactionIdentify judge(CanalEntry.Entry entry) {
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
    public boolean process(List<CanalEntry.Entry> entries) {
        return false;
    }
}
