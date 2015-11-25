package com.leansoft.bigqueue;

public class BigQueueException extends RuntimeException {

    private static final long serialVersionUID = -2688809848340212432L;

    public BigQueueException(final String msg) {
        super(msg);
    }

    public BigQueueException(final Exception e) {
        super(e);
    }
}
