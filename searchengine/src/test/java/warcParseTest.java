import org.apache.commons.io.FilenameUtils;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcRecord;
import org.apache.commons.io.IOUtils;
import edu.nyu.cs6913.Parser;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class warcParseTest {
    public static void main(String[] args) {
        System.out.println("Testing reading a warc file ");
        Parser parser = new Parser();
        String home = System.getProperty("user.home");
        String path = FilenameUtils.normalize(home + "/Documents/WebSearchEngine/FilteredFiles/Filtered-CC-MAIN-20190915052433-20190915074433-00190.warc.wet.gz");
        System.out.println(path);
        WarcReader testReader = parser.readWarc(path);
        List<String> wordList = new ArrayList<>();
        try {
            int totalRecord = 0;
            for (WarcRecord record : testReader) {
                if (parser.checkValid(record)) {
                    InputStream payloadStream = record.getPayload().getInputStreamComplete();
                    String payload = IOUtils.toString(payloadStream, "UTF-8");
                    if (parser.checkForASCII(payload, 0.98)) {
                        HeaderLine filename = record.getHeader("WARC-Target-URI");
                        System.out.println(filename.value);
                        //System.out.println(payload);
                        //parser.getWordsList(payload, wordList);
                        //for (String s : wordList) {
                        //    System.out.println(s);
                        //}
                        totalRecord++;
                    }
                }
            }
            //System.out.println(wordList.size());
            System.out.println(totalRecord);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            testReader.close();
            parser.closeInputStream();
        }
    }
}
