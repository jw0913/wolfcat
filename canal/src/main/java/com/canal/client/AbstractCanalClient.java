package com.canal.client;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.canal.delay.DelayHandler;
import com.canal.delay.LogDelayHandler;
import com.canal.processor.Processor;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

class AbstractCanalClient
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    private static final String SEP = SystemUtils.LINE_SEPARATOR;
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final ThreadLocal<SimpleDateFormat> format = new ThreadLocal() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static String context_format = null;

    protected volatile boolean running = false;

    private Thread thread = null;
    protected volatile CanalConnector connector;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            AbstractCanalClient.logger.error("{} parse events has an error.", AbstractCanalClient.this.destination, e);
            try {
                TimeUnit.SECONDS.sleep(5L);
            } catch (InterruptedException e1) {
                AbstractCanalClient.logger.error("InterruptedException.", e1);
            }
            t.start();
        }
    };
    protected String destination;
    protected int batchSize = 5120;
    protected Processor processor;
    protected DelayHandler delayHandler = new LogDelayHandler();

    protected void start()
    {
        Assert.notNull(this.connector, "connector is null");
        this.thread = new Thread("CANAL-INNER-PROCESS-THREAD") {
            public void run() {
                AbstractCanalClient.this.process();
            }
        };
        this.running = true;
        this.thread.setUncaughtExceptionHandler(this.handler);
        this.thread.start();
    }

    void stop()
    {
        if (!this.running) {
            return;
        }
        this.running = false;
        if (this.thread != null) {
            try {
                this.thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
        while (this.running)
        {
            long dbBegin = 0L; long dbEnd = 2147483647L;
            long processBegin = 0L; long processEnd = 2147483647L;
            try {
                MDC.put("destination", this.destination);
                this.connector.connect();
                this.connector.subscribe();
                while (this.running) {
                    dbBegin = 0L;
                    dbEnd = 2147483647L;
                    processBegin = 0L;
                    processEnd = 2147483647L;

                    Message message = getWithoutAck();

                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if ((batchId == -1L) || (size == 0)) {
                        TimeUnit.SECONDS.sleep(1L);
                        ack(message);
                    }else {
                        printSummary(message, batchId, size);
                        processBegin = System.currentTimeMillis();
                        dbBegin = ((Entry)message.getEntries().get(0)).getHeader().getExecuteTime();
                        if (this.processor.process(message.getEntries())) {
                            ack(message);
                        }else {
                            logger.error("[{}] process fail!", this.destination);
                            rollback(batchId);
                        }
                        processEnd = System.currentTimeMillis();
                        dbEnd = ((Entry)message.getEntries().get(size - 1)).getHeader().getExecuteTime();
                        this.delayHandler.delay(this.destination, processEnd - dbBegin, dbEnd - dbBegin, processEnd - processBegin);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] process error!", this.destination, e);
                try
                {
                    this.delayHandler.delay(this.destination, processEnd - dbBegin, dbEnd - dbBegin, processEnd - processBegin);
                } catch (Throwable t) {
                    logger.error("delayHandler.delay error.", t);
                }
            } finally {
                try {
                    this.connector.disconnect();
                } catch (Exception e) {
                    logger.error("disconnect error.", e);
                }
                MDC.remove("destination");
            }
        }
    }

    Message getWithoutAck() throws Exception {
        Message message = null;
        try {
            message = this.connector.getWithoutAck(this.batchSize);
        }
        catch (Exception e)
        {
            throw e;
        } finally {
        }
        return message;
    }

    protected void ack(Message message) throws Exception
    {
        try {
            this.connector.ack(message.getId());
        }
        catch (Exception e)
        {
            throw e;
        }
        finally {

        }
    }

    void rollback(long batchId) throws Exception {
        try {
            this.connector.rollback(batchId);
        }catch (Exception e)
        {
            throw e;
        } finally {
        }
    }

    void printSummary(Message message, long batchId, int size) {
        if (logger.isInfoEnabled()) {
            long memsize = 0L;
            for (Entry entry : message.getEntries()) {
                memsize += entry.getHeader().getEventLength();
            }
            String startPosition = null;
            String endPosition = null;
            if (!CollectionUtils.isEmpty(message.getEntries())) {
                startPosition = buildPositionForDump((Entry)message.getEntries().get(0));
                endPosition = buildPositionForDump((Entry)message.getEntries().get(message.getEntries().size() - 1));
            }
            logger.info(context_format, new Object[] { this.destination,
                    Long.valueOf(batchId),
                    Integer.valueOf(size),
                    Long.valueOf(memsize),
                    ((SimpleDateFormat)format.get()).format(new Date()), startPosition, endPosition });
        }
    }

    private String buildPositionForDump(Entry entry)
    {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":" + entry
                .getHeader().getExecuteTime() + "(" + ((SimpleDateFormat)format.get()).format(date) + ") delay : " + (System.currentTimeMillis() - time) + "ms";
    }

    public void setRunning(boolean running)
    {
        this.running = running; }
    public boolean isRunning() { return this.running; }

    public void setConnector(CanalConnector connector) {
        this.connector = connector; }
    public CanalConnector getConnector() { return this.connector; }

    public void setHandler(Thread.UncaughtExceptionHandler handler) {
        this.handler = handler;
    }

    public void setDestination(String destination)
    {
        this.destination = destination; }
    public String getDestination() { return this.destination; }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    public void setProcessor(Processor processor) {
        this.processor = processor;
    }
    public void setDelayHandler(DelayHandler delayHandler) {
        this.delayHandler = delayHandler;
    }

    static
    {
        context_format = SEP + "****************************************************" + SEP;
        context_format = context_format + "* Destination : [{}] " + SEP;
        context_format = context_format + "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , AppTime : {}" + SEP;
        context_format = context_format + "* Start : [{}]" + SEP;
        context_format = context_format + "* End : [{}]" + SEP;
        context_format = context_format + "****************************************************" + SEP;
    }
}