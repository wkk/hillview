package org.hillview.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSUtils {
     public static Configuration getDefaultHadoopConfiguration() {
        Configuration conf = new Configuration();
         // https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
         conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
         conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
         return conf;
    }

    public static boolean isEmptyDir(FileSystem fs, Path path) throws IOException {
         if (!fs.exists(path)) {
             return false;
         }

        FileStatus fileStatus = fs.getFileStatus(path);
         if (fileStatus.isDirectory()) {
             ContentSummary contentSummary = fs.getContentSummary(path);
             return contentSummary.getFileCount() + contentSummary.getDirectoryCount() == 0;
         } else {
             return false;
         }
    }
}
