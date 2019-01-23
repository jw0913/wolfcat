package com.canal.constants;


import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringUtils;

public class AppConstant {
    private static final String DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };
    private static final String SEPARATE = ".";
    private static String appName = "gms.canal";
    private static String umpMonitorCanalClientGetWithOutAck = "gms.canal.client.getWithOutAck";
    private static String umpMonitorCanalClientAck = "gms.canal.client.ack";
    private static String umpMonitorCanalClientRollback = "gms.canal.client.rollback";
    private static String umpAlarmProcessError = "gms.canal";
    private static String umpAlarmProcessFail = "gms.canal";

    public AppConstant() {
    }

    public static String getUmpMonitorCanalClientGetWithOutAck(String destination) {
        return destination + "." + umpMonitorCanalClientGetWithOutAck;
    }

    public static String getUmpMonitorCanalClientAck(String destination) {
        return destination + "." + umpMonitorCanalClientAck;
    }

    public static String getUmpMonitorCanalClientRollback(String destination) {
        return destination + "." + umpMonitorCanalClientRollback;
    }

    public void setAppName(String appName) {
        if (StringUtils.isNotBlank(appName)) {
            appName = appName;
        }

    }

    public void setUmpAlarmProcessError(String umpAlarmProcessError) {
        if (StringUtils.isNotBlank(umpAlarmProcessError)) {
            umpAlarmProcessError = umpAlarmProcessError;
        }

    }

    public void setUmpAlarmProcessFail(String umpAlarmProcessFail) {
        if (StringUtils.isNotBlank(umpAlarmProcessFail)) {
            umpAlarmProcessFail = umpAlarmProcessFail;
        }

    }

    public static String getAppName() {
        return appName;
    }

    public static String getUmpAlarmProcessError() {
        return umpAlarmProcessError;
    }

    public static String getUmpAlarmProcessFail() {
        return umpAlarmProcessFail;
    }
}

