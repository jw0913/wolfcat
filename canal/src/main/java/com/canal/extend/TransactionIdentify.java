package com.canal.extend;

import java.util.Set;

public interface TransactionIdentify<T> {
    T getType();

    Set<String> getBusniessIds();
}
