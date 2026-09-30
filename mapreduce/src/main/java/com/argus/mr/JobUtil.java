package com.argus.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

final class JobUtil {

    private JobUtil() {}

    /** Re-runs are the norm in a lab project, so replace old output unless -Dargus.overwrite=false. */
    static void prepareOutput(Configuration conf, Path out) throws IOException {
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out) && conf.getBoolean("argus.overwrite", true)) {
            fs.delete(out, true);
        }
    }

    static int usage(String name, String extra) {
        System.err.printf("usage: hadoop jar argus-mr.jar %s [-D key=value ...] <input> <output>%s%n", name, extra);
        return 2;
    }
}
