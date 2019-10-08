import org.apache.commons.io.FilenameUtils;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.apache.commons.io.FilenameUtils.*;
import org.jwat.warc.WarcRecord;
import org.apache.commons.io.IOUtils;
import edu.nyu.cs6913.Parser;
import org.apache.commons.lang3.StringUtils;
import org.jwat.warc.WarcWriterCompressed;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class warcParseTest {
    public static void main(String[] args) {
        System.out.println("Testing reading a warc file ");
        Parser parser = new Parser();
        String home = System.getProperty("user.home");
        WarcReader testReader = parser.readWarc(home + "/Documents/WebSearchEngine/CrawledFiles/CC-MAIN-20190915052433-20190915074433-00000.warc.wet.gz");
        try {
            int totalRecord = 0;
            for (WarcRecord record : testReader) {
                if (parser.checkValid(record)) {
                    InputStream payloadStream = record.getPayload().getInputStreamComplete();
                    String payload = IOUtils.toString(payloadStream, "UTF-8");
                    if (parser.checkForASCII(payload, 0.98)) {
                        if (totalRecord < 1) {
                            HeaderLine filename = record.getHeader("WARC-Target-URI");
                            System.out.println(filename.value);
                            System.out.println(payload);
                            List<String> wordList = parser.getWordsList(payload);
                            //String[] wordList = StringUtils.split(payload, " +|.+|<+|>+|'+|\"+|(+|)+|-+");
                            //System.out.println(wordList[0]);
                            //for (int j = 0; j < wordList.length; j++) {
                            //    System.out.println("index " + j + " "+ wordList[j]);
                            //}
                            //System.out.println(Arrays.toString(wordList));
                            //System.out.println("String size = " + wordList.length);
                        }
                        totalRecord++;
                    }
                }
            }
            System.out.println(StringUtils.isAlphanumericSpace("Feltöltés"));
            System.out.println(totalRecord);
            /*WarcRecord record = testReader.getNextRecord();
            if (parser.checkValid(record)) {
                InputStream payloadStream = record.getPayload().getInputStreamComplete();
                String payload = IOUtils.toString(payloadStream, "US-ASCII");
                System.out.println(payload);
            }*/
            parser.closeInputStream();
            testReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
