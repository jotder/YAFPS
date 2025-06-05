package org.gamma.processing;


import org.gamma.util.Utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MatrixBuilder implements AutoCloseable {
    private static ArrayList<String> dimFields;
    private static ArrayList<String> measureFields;
    BufferedWriter writer;
    boolean keepHeader;
    String fileName;
    String fileUrl;
    String id;
    String dataSource;
    Map<String, List<Number>> summaryMatrix = new ConcurrentHashMap<>();
    Map<String, Object> rec;

    public MatrixBuilder(String dim, String measures, String fileName, boolean keepHeader) throws IOException {
        dimFields = this.getListFromCSV(dim);
        measureFields = this.getListFromCSV(measures);
        this.keepHeader = keepHeader;
        this.fileName = fileName;
        this.id = Utils.getFileID(this.dataSource, fileName);
        Path filePath = Path.of(fileName);
        this.writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public static String getHeader() {
        String var10000 = String.join(",", dimFields);
        return var10000 + ",rec_count," + String.join(",", measureFields) + "\n";
    }

    static Double getNumber(String s) {
        if (s != null && !s.trim().isEmpty() && !s.equals("0.0")) {
            try {
                return Double.parseDouble(s);
            } catch (Exception e) {
                e.printStackTrace();
                return (double) 0.0F;
            }
        } else {
            return (double) 0.0F;
        }
    }



    public String asCSV() {
        StringBuilder csv = new StringBuilder();
        this.summaryMatrix.forEach((k, v) -> {
            String v1 = v.stream().map(String::valueOf).collect(Collectors.joining(","));
            csv.append(k).append(",").append(v1).append("\n");
        });
        return csv.toString();
    }

    public void populate(Map<String, Object> rec) throws Exception {
        this.rec = rec;
        List<String> dimValues = new ArrayList<>();
        dimFields.forEach((fn) -> {
            Object o = rec.get(fn);
            if (o != null) {
                dimValues.add(Utils.escapeCsvField(o.toString().trim()));
            } else {
                System.out.println("---- Issues ----");
            }

        });
        List<String> measureVals = new ArrayList<>();
        measureFields.forEach((fn) -> {
            Object o = rec.get(fn);
            if (o != null) {
                measureVals.add(o.toString().trim());
            } else {
                System.out.println("----Issues--");
            }

        });
        String keyAsCSV = String.join(",", dimValues);
        List<Number> counter = this.summaryMatrix.get(keyAsCSV);
        if (counter == null) {
            counter = new ArrayList<>(measureVals.size() + 1);
            counter.add(1L);

            for (String measureValue : measureVals) {
                counter.add(getNumber(measureValue));
            }

            this.summaryMatrix.put(keyAsCSV, counter);
        } else {
            if (!((String) measureVals.get(1)).equals(measureVals.get(3))) {
                System.out.println("matrix issue : inst_amount, inst_from_amount, inst_from_ifee, inst_to_amount,inst_to_ifee ->" + String.join(" ", measureVals));
            }

            ArrayList<Number> uCounter = getUpdatedCounter(counter, measureVals);
            this.summaryMatrix.put(keyAsCSV, uCounter);
        }

    }

    private static ArrayList<Number> getUpdatedCounter(List<Number> counter, List<String> measures) throws Exception {
        ArrayList<Number> uCounter = new ArrayList<>(measures.size() + 1);
        uCounter.add((Long) counter.getFirst() + 1L);

        for (int i = 0; i < measures.size(); ++i) {
            double x = (Double) counter.get(i + 1) + getNumber((String) measures.get(i));
            uCounter.add(x);
        }

        return uCounter;
    }

    public void writeMatrix() throws IOException {
        if (this.keepHeader) {
            this.writer.write(getHeader());
        }

        this.writer.write(this.asCSV());
    }

    public String getMatrixFile() {
        return this.fileName;
    }

    private ArrayList<String> getListFromCSV(String csv) {
        String[] x = csv.split(",");
        ArrayList<String> list = new ArrayList<>(x.length);
        Arrays.stream(x).forEach((e) -> list.add(e.trim()));
        return list;
    }

    public void validate() {
    }

    public void close() throws Exception {
        this.writer.flush();
        this.writer.close();
    }
}
