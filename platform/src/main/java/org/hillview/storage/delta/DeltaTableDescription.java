package org.hillview.storage.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import org.apache.hadoop.conf.Configuration;
import org.hillview.utils.HDFSUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains necessary information to access a delta table
 */
public class DeltaTableDescription implements Serializable {
    static final long serialVersionUID = 1;

    public String path;
    @Nullable
    public Long snapshotVersion;

    public List<String> getFiles() {
        Configuration conf = HDFSUtils.getDefaultHadoopConfiguration();

        DeltaLog log = DeltaLog.forTable(conf, path);
        Snapshot snapshot = null;
        if (snapshotVersion == null) {
            snapshot = log.snapshot();
        } else {
            snapshot = log.getSnapshotForVersionAsOf(snapshotVersion);
        }

        String sep = path.endsWith("/") ? "" : "/";
        return snapshot.getAllFiles()
                .stream()
                .map(addFile -> path + sep + addFile.getPath())
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "DeltaTableDescription{" +
                "path='" + path + '\'' +
                ", snapshotVersion=" + snapshotVersion +
                '}';
    }
}
