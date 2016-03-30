package com.bluejeans.common.bigqueue;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CloseCommand {

    private final static Logger logger = LoggerFactory.getLogger(MappedPageFactory.class);

    public static void close(final Closeable closeable) {
        try {
            closeable.close();
        }
        catch (final IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
