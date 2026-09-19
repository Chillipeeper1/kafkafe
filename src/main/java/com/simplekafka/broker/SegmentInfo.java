package com.simplekafka.broker;

import java.nio.file.Path;

// base offset + file paths
public record SegmentInfo(long baseOffset, Path dotLog, Path dotIndex) {}
