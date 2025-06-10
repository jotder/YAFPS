package org.gamma.processing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MergedFileWriter implements AutoCloseable {
    private final BufferedWriter writer;
    private final Instant startTime;
    public Path mFlePath;
    public Map<String, Long> edgeStatus = new LinkedHashMap<>();
    //    String dataSource;
    String mergedFileName;
    private long recordCount = 0L;
    private Instant endTime;
    private boolean closed = false;

    public MergedFileWriter(Path mFlePath, List<String> headerFields, boolean keepHeader) throws Exception {
//        this.dataSource = dataSource;
        this.mFlePath = mFlePath;
        this.writer = Files.newBufferedWriter(mFlePath);
        if (keepHeader) {
            String s = String.join(",", headerFields) + "\n";
            this.writer.write(s);
        }
        this.startTime = Instant.now();
    }

    public Path getmFlePath() {
        return this.mFlePath;
    }

    public long getRecordCount() {
        return this.recordCount;
    }

    public boolean isClosed() {
        return !this.closed;
    }

    public void writeLine(String sFileName, String line) throws IOException {
        if (this.closed) {
            throw new IOException("Attempted to write to a closed writer for partition: " + this.mFlePath.toFile().getName());
        } else {
            this.writer.write(line + "\n");
            ++this.recordCount;
        }
        Long edge = edgeStatus.get(sFileName);
        if (edge != null)
            edge = edge + 1;
        else
            edge = 1L;
        this.edgeStatus.put(sFileName, edge);
    }


    public void close() throws IOException {
        if (!this.closed)
            this.writer.close();
        this.closed = true;
        this.endTime = Instant.now();
    }
}
