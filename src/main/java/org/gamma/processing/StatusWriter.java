package org.gamma.processing;

import org.gamma.metrics.FileInfo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Logger;

import static org.gamma.util.Utils.escapeCsvField;

public class StatusWriter {
    protected Logger logger;

    BufferedWriter statusWriter;
    BufferedWriter routeStatusWriter;

    public StatusWriter(Path statusPath, String statusTable, String routeTable, Logger logger) throws IOException {
        this.logger = logger;
        Files.createDirectories(statusPath);

        this.statusWriter = Files.newBufferedWriter(statusPath.resolve(statusTable), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        String s = "id, data_source, file_name, file_size, last_mod_ts, parse_count, fail_count, file_type, task, start_ts, duration, status, record_start, record_end, message\n";
        this.statusWriter.write(s);
        this.statusWriter.flush();

        this.routeStatusWriter = Files.newBufferedWriter(statusPath.resolve(routeTable), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        String routeHeader = "id, dataSource, srcFileID, srcFileName, destFileID, destFileName, count, message\n";
        this.routeStatusWriter.write(routeHeader);
        this.routeStatusWriter.flush();
    }


    public synchronized int[] insertFileStatus(List<FileInfo> batchStatus) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        for (FileInfo p : batchStatus) {
            String msg = escapeCsvField(p.fileID());
            String csv = msg + "," + p.dataSource() + "," + p.fileName() + "," + p.fileSize() + "," + p.lastModTs().toLocalDateTime().format(formatter)
                         + "," + p.recCount() + "," + p.failCount() + "," + p.fileType() + "," + p.task() + "," + p.startTs().toLocalDateTime().format(formatter)
                         + "," + p.duration() + "," + p.status() + "," + p.recordStart() + "," + p.recordEnd() + "," + escapeCsvField(p.message()) + "\n";
            this.statusWriter.write(csv);
        }

        this.statusWriter.flush();
        return new int[batchStatus.size()];
    }

//    public int[] insertRouteStatus(List<RouteInfo> routeStatus) throws Exception {
//        for (RouteInfo routeInf : routeStatus) {
//            String srcFileID = routeInf.srcFileID();
//            String crc = Utils.generateFileID(srcFileID + routeInf.destFileID());
//            String csv = crc + "," + routeInf.dataSource() + "," + routeInf.srcFileID() + "," + routeInf.srcFileName() + "," + routeInf.destFileID() + "," + routeInf.destFileName() + "," + routeInf.count() + "," + escapeCsvField(routeInf.msg()) + "\n";
//            this.routeStatusWriter.write(csv);
//        }
//
//        this.routeStatusWriter.flush();
//        return new int[routeStatus.size()];
//    }

    public void loadDataToDatabase(String mfi, String matrixTable) throws Exception {
    }


}
