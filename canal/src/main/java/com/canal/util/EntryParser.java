package com.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.canal.constants.AppConstant;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EntryParser {
    public EntryParser() {
    }

    public static String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":" + entry.getHeader().getExecuteTime() + "(" + ((SimpleDateFormat)AppConstant.DATE_FORMAT.get()).format(date) + ") delay : " + (System.currentTimeMillis() - time) + "ms";
    }
}
