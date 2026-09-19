package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Partition {

    // Fundamental units od data storage
    private final int id;
    private final Path baseDir;
    private int leader;
    private List<Integer> followers;
    private AtomicLong nextOffSet = new AtomicLong(0);
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private List<SegmentInfo> segments = new ArrayList<>();



    public Partition(int id, Path baseDir, int leader, List<Integer> followers){
        this.id = id;
        this.baseDir = baseDir;
        this.leader = leader;
        this.followers = followers;

    }

    public void initialize() throws IOException{

        File[] logFiles;
        Path directory;
        String name;
        int lastDot;
        String nameWithoutIndex;
        long parsedBaseOffset = 0;
        Path indexFiles;
        SegmentInfo segmentInfo;


        // Creates directory if it doesn't exist (it will not the first time baseDir will be called)

        Files.createDirectories(baseDir); // Get directory path
        System.out.println("Directories created or already exist");

        // Gets all .log files from directory
        logFiles = baseDir.toFile().listFiles(

                file -> file.getName().endsWith(".log")
        );

        if(logFiles != null){
            for(File f : logFiles){
                // get only file name
                name = f.getName();
                lastDot = name.lastIndexOf('.');
                if(lastDot > 0 && lastDot < name.length() - 1) {
                    nameWithoutIndex = name.substring(0, lastDot);
                } else {
                        nameWithoutIndex = name;
                }
                //Gets offset base from file name
                parsedBaseOffset = Long.parseLong(nameWithoutIndex);
                indexFiles = baseDir.resolve(nameWithoutIndex + ".index");
                segmentInfo = new SegmentInfo(parsedBaseOffset, f.toPath(), indexFiles);
                segments.add(segmentInfo);
            }
            segments.sort(Comparator.comparingLong(SegmentInfo::baseOffset));


        }
    }





}

