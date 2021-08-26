package org.hillview.utils;

import org.apache.hadoop.conf.Configuration;

public class HDFSUtils {
     public static Configuration getDefaultHadoopConfiguration() {
        Configuration conf = new Configuration();
         // https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
         conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
         conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
         return conf;
    }
}
