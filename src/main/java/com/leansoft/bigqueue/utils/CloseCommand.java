package com.leansoft.bigqueue.utils;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.leansoft.bigqueue.page.MappedPageFactoryImpl;

public class CloseCommand {

    private final static Logger logger = Logger.getLogger(MappedPageFactoryImpl.class);

    public static void close(final Closeable closeable) {
        try {
            closeable.close();
        }
        catch (final IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
