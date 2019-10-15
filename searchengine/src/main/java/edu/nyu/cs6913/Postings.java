package edu.nyu.cs6913;

import org.apache.commons.io.IOUtils;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Postings {
    private long _maxBlockSizeBytes;
    private long _fileDocID;
    private HashMap<String, ArrayList<docIDtoFreq>> _MapWordToDocID;
    private HashMap<String, Long> _MapWordToFreq;
    private String _outputPath;
    private String _tempDocID;
    private String _tempLexicon;
    private String _tempInvertedIndex;
    private long _currBlockSizeBytes;
    private vByteCompression _encoder = new vByteCompression();

    private class docIDtoFreq {
        byte[] _docID;
        byte[] _freq;
        public docIDtoFreq(long docID, long freq) {
            _docID = _encoder.vByteEncode(docID);
            _freq = _encoder.vByteEncode(freq);
        }
    }

    public Postings(long maxBlockSizeMB, String outputPath) {
        _maxBlockSizeBytes = maxBlockSizeMB * 1024 * 1024;
        _currBlockSizeBytes = 0;
        _fileDocID = 0;
        _MapWordToDocID = new HashMap<>();
        _MapWordToFreq = new HashMap<>();
        _outputPath = outputPath;
        _tempDocID = _outputPath + "temp/DocID/";
        _tempLexicon = _outputPath + "temp/Lexicon/";
        _tempInvertedIndex = _outputPath + "temp/InvertedIndex/";
        new File(_tempDocID).mkdirs();
        new File(_tempLexicon).mkdirs();
        new File(_tempInvertedIndex).mkdirs();
    }

    public void addWordToFreq(List<String> wordList) {
        for (String w : wordList) {
            _MapWordToFreq.put(w, _MapWordToFreq.getOrDefault(w, (long) 0) + 1);
        }
    }

    public void addBlockSize(docIDtoFreq docItem) {
        _currBlockSizeBytes += docItem._docID.length + docItem._freq.length;
    }

    public void addToPosting() {
        for (Map.Entry<String, Long> wordToFreq : _MapWordToFreq.entrySet()) {
            ArrayList<docIDtoFreq> currDoc = _MapWordToDocID.getOrDefault(wordToFreq.getKey(), new ArrayList<>());
            docIDtoFreq docItem = new docIDtoFreq(_fileDocID, wordToFreq.getValue());
            addBlockSize(docItem);
            currDoc.add(docItem);
            _MapWordToDocID.put(wordToFreq.getKey(), currDoc);
        }
    }

    public void addToDocID(String filename) {
        File file = new File(_tempDocID + "DocIDs");
        try {
            file.createNewFile();
            FileWriter fr = new FileWriter(file, true);
            fr.write(filename);
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToInvertedIndex() {
        System.out.println("output sub block inverted index");
        ArrayList<String> sortedKeys = new ArrayList<>(_MapWordToDocID.keySet());
        Collections.sort(sortedKeys);
        System.out.println(sortedKeys.size());
        _MapWordToDocID.clear();
    }

    public void parseReader(WarcReader reader, int currFileNum, int totalFileNum) {
        Parser parser = new Parser();
        try {
            for (WarcRecord record : reader) {
                InputStream payloadStream = record.getPayload().getInputStreamComplete();
                String payload = IOUtils.toString(payloadStream, "UTF-8");
                HeaderLine filename = record.getHeader("WARC-Target-URI");
                addToDocID(filename.value + "\n");
                _fileDocID++;
                List<String> wordList = new ArrayList<>();
                parser.getWordsList(payload, wordList);
                addWordToFreq(wordList);
                addToPosting();
                _MapWordToFreq.clear();
            }
            if (_currBlockSizeBytes >= _maxBlockSizeBytes || currFileNum == totalFileNum - 1) {
                writeToInvertedIndex();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }

}
