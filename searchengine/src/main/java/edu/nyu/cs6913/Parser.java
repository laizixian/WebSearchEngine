package edu.nyu.cs6913;

import org.jwat.common.HeaderLine;
import org.jwat.warc.*;

import java.io.*;
import java.util.List;

public class Parser {

    private InputStream _inputStream;
    private OutputStream _outputStream;

    public Parser() {
        _inputStream = null;
        _outputStream = null;
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
            return WarcReaderFactory.getReaderCompressed(_inputStream);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void getWordsList(String payload, List<String> wordList) {
        int len = payload.length();
        int startIndex = Character.isLetterOrDigit(payload.charAt(0)) ? 0 : 1;
        for (int i = 1; i < len; i++) {
            if (!Character.isLetterOrDigit(payload.charAt(i))) {
                if (Character.isLetterOrDigit(payload.charAt(i - 1))) {
                    wordList.add(payload.substring(startIndex, i).toLowerCase());
                }
                startIndex = i + 1;
            }
        }
        if (Character.isLetterOrDigit(payload.charAt(len - 1))) {
            wordList.add(payload.substring(startIndex, len));
        }
    }

    public void closeInputStream() {
        try {
            if (_inputStream != null) {
                _inputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
