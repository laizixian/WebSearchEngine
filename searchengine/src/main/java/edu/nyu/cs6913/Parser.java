package edu.nyu.cs6913;

import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Parser {

    InputStream _inputStream;

    public Parser() {
        _inputStream = null;
    }

    public boolean checkValid(WarcRecord record) {
        HeaderLine filename = record.getHeader("WARC-Target-URI");
        return filename != null && record.hasPayload();
    }

    public boolean checkForASCII (String Payload, double threshold) {
        int totalLength = Payload.length();
        int ASCIICount = 0;
        for (int i = 0; i < totalLength; i++) {
            if ((int) Payload.charAt(i) < 256) {
                ASCIICount++;
            }
        }
        double ASCIIPercentage = (double) ASCIICount / (double) totalLength;
        return ASCIIPercentage >= threshold;
    }

    public WarcReader readWarc(String fileName) {
        File inputFile = new File(fileName);
        try {
            _inputStream = new FileInputStream(inputFile);
            WarcReader warcReader = WarcReaderFactory.getReaderCompressed(_inputStream);
            return warcReader;
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> getWordsList(String payload) {
        List<String> wordList = new ArrayList<String>();
        int len = payload.length();
        int startIndex = Character.isLetterOrDigit(payload.charAt(0)) ? 0 : 1;
        for (int i = 1; i < len; i++) {
            if (!Character.isLetterOrDigit(payload.charAt(i))) {
                if (Character.isLetterOrDigit(payload.charAt(i - 1))) {
                    wordList.add(payload.substring(startIndex, i));
                }
                startIndex = i + 1;
            }
        }
        if (Character.isLetterOrDigit(payload.charAt(len - 1))) {
            wordList.add(payload.substring(startIndex, len));
        }
        for (String s : wordList) {
            System.out.println(s);
        }
        return wordList;
    }

    public void closeInputStream() throws IOException {
        _inputStream.close();
    }

}
