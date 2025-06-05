package org.gamma.metrics;

import java.sql.Timestamp;


public record FileInfo(String fileID, String dataSource, String fileName, long fileSize, Timestamp lastModTs,
                       long recCount, long failCount, String fileType, String task, Timestamp startTs, long duration,
                       Status status, String recordStart, String recordEnd, String message) {
}
