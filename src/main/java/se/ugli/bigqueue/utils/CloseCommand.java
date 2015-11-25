package se.ugli.bigqueue.utils;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;

import se.ugli.bigqueue.page.MappedPageFactory;

public class CloseCommand {

    private final static Logger logger = Logger.getLogger(MappedPageFactory.class);

    public static void close(final Closeable closeable) {
        try {
            closeable.close();
        }
        catch (final IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
