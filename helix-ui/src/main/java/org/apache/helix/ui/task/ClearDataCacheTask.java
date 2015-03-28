package org.apache.helix.ui.task;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.apache.helix.ui.util.DataCache;

import java.io.PrintWriter;

public class ClearDataCacheTask extends Task {
    private final DataCache dataCache;

    public ClearDataCacheTask(DataCache dataCache) {
        super("clearDataCache");
        this.dataCache = dataCache;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
        printWriter.println("Clearing data caches ...");
        printWriter.flush();
        dataCache.invalidate();
        printWriter.println("Done!");
        printWriter.flush();
    }
}
