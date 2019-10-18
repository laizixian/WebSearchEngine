package edu.nyu.cs6913;

import org.jwat.common.HeaderLine;
import org.jwat.warc.*;

import java.io.*;
import java.util.List;

public class Parser {

    private InputStream _inputStream;

    public Parser() {
        _inputStream = null;
    }

    /**
     * check if the record is valid
     * @param record a WarcRecord object
     * @return boolean
     */
    public boolean checkValid(WarcRecord record) {
        HeaderLine filename = record.getHeader("WARC-Target-URI");
        return filename != null && record.hasPayload();
    }

    /**
     * Check if the document is English document base on a threshold
     * @param Payload String
     * @param threshold double
     * @return boolean
     */
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

    /**
     * read the file as a WarcReader
     * @param fileName String of file name
     * @return WarcReader for the file
     */
    public WarcReader readWarc(String fileName) {
        File inputFile = new File(fileName);
        try {
            _inputStream = new FileInputStream(inputFile);
            return WarcReaderFactory.getReaderCompressed(_inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * check if the character is letter or digit
     * @param c character
     * @return boolean
     */
    private boolean isLetterOrDigit(char c) {
        return (c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9');
    }

    /**
     * parse words from the payload and add to word list
     * @param payload String
     * @param wordList List of String
     */
    void getWordsList(String payload, List<String> wordList) {
        int len = payload.length();
        int startIndex = isLetterOrDigit(payload.charAt(0)) ? 0 : 1;
        for (int i = 1; i < len; i++) {
            if (!isLetterOrDigit(payload.charAt(i))) {
                if (isLetterOrDigit(payload.charAt(i - 1))) {
                    String temp = payload.substring(startIndex, i).toLowerCase();
                    if (temp.length() < 30) {
                        wordList.add(temp);
                    }
                }
                startIndex = i + 1;
            }
        }
        if (isLetterOrDigit(payload.charAt(len - 1))) {
            String temp = payload.substring(startIndex, len).toLowerCase();
            if (temp.length() < 30) {
                wordList.add(temp);
            }
        }
    }

    /**
     * close the input stream used
     */
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
