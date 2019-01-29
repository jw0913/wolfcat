package com.canal.delay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDelayHandler implements DelayHandler {
    private static final Logger logger = LoggerFactory.getLogger(LogDelayHandler.class);

    public LogDelayHandler() {
    }
    @Override
    public void delay(String destination, long realDelayMs, long dbDelayMs, long processDelayMs) {
        logger.warn("delay : [destination : ({}) , process delay :(realDelay: {}ms, dbDelay: {}ms, processDelay : {}ms, total : {}ms)]", new Object[]{destination, realDelayMs, dbDelayMs, processDelayMs, dbDelayMs + processDelayMs});
    }
}
