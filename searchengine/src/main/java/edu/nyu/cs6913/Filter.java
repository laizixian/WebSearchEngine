package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

import java.io.*;

public class Filter extends WarcWriterFactory {
    private WarcWriter _writer;
    public Filter(OutputStream outputStream) {
        _writer = getWriterCompressed(outputStream);
    }

    public void writeRecord (WarcRecord record, InputStream payloadStream) throws IOException {
        _writer.writeHeader(record);
        _writer.streamPayload(payloadStream);
    }

    public void closeWriter() {
        try {
            if (_writer != null) {
                _writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];
        double threshold = Double.parseDouble(args[2]);
        String home = System.getProperty("user.home");
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        String trueOutputPath = FilenameUtils.normalize(home + outputPath);
        File inputDir = new File(trueInputPath);
        FilenameFilter gzFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".gz");
            }
        };
        File[] inputFiles = inputDir.listFiles(gzFilter);
        assert inputFiles != null;
        System.out.println(trueOutputPath);
        for (File f : inputFiles) {
            String inputFilePath = trueInputPath + f.getName();
            String outputFilePath = trueOutputPath + "Filtered-" + f.getName();
            System.out.println("Filtering file" + f.getName());
            Parser parser = new Parser();
            File file = new File(FilenameUtils.normalize(outputFilePath));
            if (!file.exists()) {
                file.createNewFile();
            }
            OutputStream out = new FileOutputStream(file);
            Filter filter = new Filter(out);
            WarcReader testReader = parser.readWarc(FilenameUtils.normalize(inputFilePath));
            try {
                int totalRecord = 0;
                for (WarcRecord record : testReader) {
                    if (parser.checkValid(record)) {
                        InputStream payloadStream = record.getPayloadContent();
                        String payload = IOUtils.toString(payloadStream, "UTF-8");
                        if (parser.checkForASCII(payload, threshold)) {
                            InputStream tempStream = new ByteArrayInputStream(payload.getBytes());
                            filter.writeRecord(record, tempStream);
                            totalRecord++;
                            tempStream.close();
                        }
                        payloadStream.close();
                    }
                }
                System.out.println("Total English record:" + totalRecord);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                parser.closeInputStream();
                testReader.close();
                filter.closeWriter();
                out.close();
            }
        }
    }
}
