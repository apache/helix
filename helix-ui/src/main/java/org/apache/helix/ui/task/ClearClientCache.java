package org.apache.helix.ui.task;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.apache.helix.ui.util.ClientCache;

import java.io.PrintWriter;

public class ClearClientCache extends Task {
    private final ClientCache clientCache;

    public ClearClientCache(ClientCache clientCache) {
        super("clearClientCache");
        this.clientCache = clientCache;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception {
        printWriter.println("Clearing ZK connections ...");
        printWriter.flush();
        clientCache.invalidateAll();
        printWriter.println("Done!");
        printWriter.flush();
    }
}
