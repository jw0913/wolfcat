package com.canal.client;


import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

class StreamCanalClient extends AbstractCanalClient implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(StreamCanalClient.class);
    private static final Logger monitorLogger = LoggerFactory.getLogger("MONITOR-LOGGER");
    private static final int monitorHz = 15;
    private Queue<Message> messagesQueue;
    private long noBinlogSleepMs = 100L;
    private long fillQueueSleepMs = 5L;

    StreamCanalClient() {
    }
    @Override
    protected void start() {
        Assert.notNull(this.connector, "connector is null");
        this.running = true;
        Thread offeThread = new Thread("CANAL-OFFER-THREAD(" + this.destination + ")") {
            @Override
            public void run() {
                StreamCanalClient.this.offer();
            }
        };
        offeThread.setUncaughtExceptionHandler(this.handler);
        offeThread.start();
        Thread pollThread = new Thread("CANAL-POLL-THREAD(" + this.destination + ")") {
            public void run() {
                StreamCanalClient.this.peekAndPoll();
            }
        };
        pollThread.setUncaughtExceptionHandler(this.handler);
        pollThread.start();
    }
    private void offer() {
        while (this.running){
            try {
                MDC.put("destination", this.destination);
                this.connector.connect();
                this.connector.subscribe();
                while ((this.running) && (this.connector.checkValid())) {
                    Message message = getWithoutAck();
                    int size = message.getEntries().size();
                    if (size == 0) {
                        TimeUnit.MILLISECONDS.sleep(this.noBinlogSleepMs);
                    }
                    else{
                        while (!this.messagesQueue.offer(message)){
                            TimeUnit.MILLISECONDS.sleep(this.fillQueueSleepMs);
                        }
                    }

                }
            } catch (Exception e) {
                logger.error("[{}] process error!", this.destination, e);
                if (!(e instanceof NullPointerException)){
                    logger.error("connector.disconnect error.", e);
                }

            } finally {
                try {
                    this.connector.disconnect();
                }
                catch (Exception e) {
                    if (!(e instanceof NullPointerException)) {
                        logger.error("connector.disconnect error.", e);
                    }
                }
                this.messagesQueue.clear();
                MDC.remove("destination");
            }
        }

    }

    private void peekAndPoll() {
        while(this.running) {
            long dbBegin = 0L;
            long dbEnd = 2147483647L;
            long processBegin = 0L;
            long processEnd = 2147483647L;

            try {
                MDC.put("destination", this.destination);

                while(this.running) {
                    Message message;
                    while((message = (Message)this.messagesQueue.peek()) == null) {
                        TimeUnit.MILLISECONDS.sleep(this.noBinlogSleepMs);
                    }

                    dbBegin = 0L;
                    dbEnd = 2147483647L;
                    processBegin = 0L;
                    processEnd = 2147483647L;
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (size == 0) {
                        this.ack(message);
                    } else {
                        this.printSummary(message, batchId, size);
                        processBegin = System.currentTimeMillis();
                        dbBegin = ((Entry)message.getEntries().get(0)).getHeader().getExecuteTime();
                        if (this.processor.process(message.getEntries())) {
                            this.ack(message);
                        } else {
                            logger.error("[{}] process fail!", this.destination);
                            this.rollback(batchId);
                        }

                        processEnd = System.currentTimeMillis();
                        dbEnd = ((Entry)message.getEntries().get(size - 1)).getHeader().getExecuteTime();
                        this.delayHandler.delay(this.destination, processEnd - dbBegin, dbEnd - dbBegin, processEnd - processBegin);
                    }
                }
            } catch (Exception var21) {
                logger.error("[{}] process error!", this.destination, var21);
                this.delayHandler.delay(this.destination, processEnd - dbBegin, dbEnd - dbBegin, processEnd - processBegin);
            } finally {
                try {
                    this.connector.disconnect();
                } catch (Exception var20) {
                    if (!(var20 instanceof NullPointerException)) {
                        logger.error("connector.disconnect error.", var20);
                    }
                }

                this.messagesQueue.clear();
                MDC.remove("destination");
            }
        }

    }
    @Override
    protected void ack(Message peekMessage) throws Exception {
        super.ack(peekMessage);
        Message pollMessage = (Message)this.messagesQueue.poll();
        if (!peekMessage.equals(pollMessage)) {
            String msg = String.format("peek not equals poll,peek = %s, poll =%s", peekMessage.getId(), pollMessage != null ? pollMessage.getId() : null);
            logger.error(msg);
            throw new Exception(msg);
        }
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        Timer timer = new Timer("CANAL-BUFFER-QUEUE-MONITOR(" + this.destination + ")");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                StreamCanalClient.monitorLogger.info("{} message buffer queue size:{}", StreamCanalClient.this.destination, StreamCanalClient.this.messagesQueue.size());
            }
        }, 0L, 15000L);
    }

    public void setMessagesQueue(Queue<Message> messagesQueue) {
        this.messagesQueue = messagesQueue;
    }

    public void setNoBinlogSleepMs(long noBinlogSleepMs) {
        this.noBinlogSleepMs = noBinlogSleepMs;
    }

    public void setFillQueueSleepMs(long fillQueueSleepMs) {
        this.fillQueueSleepMs = fillQueueSleepMs;
    }
}

