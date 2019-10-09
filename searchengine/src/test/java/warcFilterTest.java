import edu.nyu.cs6913.Filter;
import edu.nyu.cs6913.Parser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcRecord;

import java.io.*;

public class warcFilterTest {
    public static void main(String[] args) throws IOException {
        System.out.println("Testing writing filtered files");
        Parser parser = new Parser();
        String home = System.getProperty("user.home");
        File file = new File(FilenameUtils.normalize(home + "/Documents/WebSearchEngine/FilteredFiles/filtered00000.wet.gz"));
        if (!file.exists()) {
            file.createNewFile();
        }
        OutputStream out = new FileOutputStream(file);
        Filter filter = new Filter(out);
        WarcReader testReader = parser.readWarc(FilenameUtils.normalize(home + "/Documents/WebSearchEngine/CrawledFiles/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz"));
        try {
            int totalRecord = 0;
            for (WarcRecord record : testReader) {
                if (parser.checkValid(record)) {
                    InputStream payloadStream = record.getPayloadContent();
                    InputStream dummyStream = record.getPayload().getInputStreamComplete();
                    String payload = IOUtils.toString(payloadStream, "UTF-8");
                    if (parser.checkForASCII(payload, 0.98)) {
                        System.out.println(record.getHeader("Content-Length").value);
                        InputStream tempStream = new ByteArrayInputStream(payload.getBytes());
                        System.out.println(tempStream.available());
                        //System.out.println(payload);
                        filter.writeRecord(record, tempStream);
                        totalRecord++;
                    }
                }
            }

            System.out.println(totalRecord);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            parser.closeInputStream();
            testReader.close();
            filter.closeWriter();
        }
    }
}
