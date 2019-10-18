package edu.nyu.cs6913;

import org.apache.commons.io.IOUtils;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcRecord;

import java.io.*;
import java.util.*;

/**
 * Posting class which creates sub inverted index files and lexicon files
 * base be the max allowed size
 */

class Postings {
    private long _maxBlockSizeBytes;
    private long _fileDocID;
    private int _tempFileID;
    private HashMap<String, ArrayList<docIDtoFreq>> _MapWordToDocID;
    private HashMap<String, Long> _MapWordToFreq;
    String _tempDocID;
    private String _tempLexicon;
    private String _tempInvertedIndex;
    private long _currBlockSizeBytes;
    private vByteCompression _encoder = new vByteCompression();

    private class docIDtoFreq {
        byte[] _docID;
        byte[] _freq;
        docIDtoFreq(long docID, long freq) {
            _docID = _encoder.vByteEncode(docID);
            _freq = _encoder.vByteEncode(freq);
        }
    }

    Postings(long maxBlockSizeMB, String outputPath) {
        _maxBlockSizeBytes = maxBlockSizeMB * 1024 * 1024;
        _currBlockSizeBytes = 0;
        _fileDocID = 0;
        _tempFileID = 0;
        _MapWordToDocID = new HashMap<>();
        _MapWordToFreq = new HashMap<>();
        _tempDocID = outputPath + "temp/DocID/";
        _tempLexicon = outputPath + "temp/Lexicon/";
        _tempInvertedIndex = outputPath + "temp/InvertedIndex/";
        new File(_tempDocID).mkdirs();
        new File(_tempLexicon).mkdirs();
        new File(_tempInvertedIndex).mkdirs();
    }

    /**
     * add word to the the frequency map for each document
     * @param wordList a word list for document
     */
    private void addWordToFreq(List<String> wordList) {
        for (String w : wordList) {
            _MapWordToFreq.put(w, _MapWordToFreq.getOrDefault(w, (long) 0) + 1);
        }
    }

    /**
     * increase the current block size base on the frequency length and document id length
     * @param docItem docID, freq
     */
    private void addBlockSize(docIDtoFreq docItem) {
        _currBlockSizeBytes += docItem._docID.length + docItem._freq.length;
    }

    /**
     * add to the posting for the current document
     */
    private void addToPosting() {
        for (Map.Entry<String, Long> wordToFreq : _MapWordToFreq.entrySet()) {
            ArrayList<docIDtoFreq> currDoc = _MapWordToDocID.getOrDefault(wordToFreq.getKey(), new ArrayList<>());
            docIDtoFreq docItem = new docIDtoFreq(_fileDocID, wordToFreq.getValue());
            addBlockSize(docItem);
            currDoc.add(docItem);
            _MapWordToDocID.put(wordToFreq.getKey(), currDoc);
        }
    }

    /**
     * write to the lexicon file
     * @param lexiconWriter a buffered writer for writing lexicon files
     * @param term the term id in ASCII form
     * @param indexStart the start byte of docID for that term
     * @param indexEnd the end byte of docID for that term
     * @param freqEnd the end byte of frequency for that term
     */
    private void appendToLexicon(BufferedWriter lexiconWriter, String term, long indexStart, long indexEnd, long freqEnd) {
        try{
            lexiconWriter.write(term + " " + indexStart + " " + indexEnd + " " + freqEnd + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * after the max block size has reached, this function write the sub block inverted
     * index and lexicon to files.
     */
    private void writeToInvertedIndex() {
        System.out.println("output sub block inverted index");
        File tempInvertedIndex = new File(_tempInvertedIndex + _tempFileID + ".tempIndex");
        File tempLexicon = new File(_tempLexicon + _tempFileID + ".tempLexicon");
        ArrayList<String> sortedKeys = new ArrayList<>(_MapWordToDocID.keySet());
        Collections.sort(sortedKeys);
        long indexStart = 0;
        long indexEnd = 0;
        long freqEnd = 0;
        try {
            tempInvertedIndex.createNewFile();
            tempLexicon.createNewFile();
            BufferedWriter lexiconWriter = new BufferedWriter(new FileWriter(tempLexicon));
            OutputStream invertedIndexWriter = new FileOutputStream(tempInvertedIndex);
            BufferedOutputStream bufferedInvertedIndexWriter = new BufferedOutputStream(invertedIndexWriter);
            for (String term : sortedKeys) {
                ArrayList<docIDtoFreq> curIndex = _MapWordToDocID.get(term);
                for (docIDtoFreq docIDAndFreq : curIndex) {
                    bufferedInvertedIndexWriter.write(docIDAndFreq._docID);
                    indexEnd += docIDAndFreq._docID.length;
                }
                freqEnd = indexEnd;
                for (docIDtoFreq docIDtoFreq : curIndex) {
                    bufferedInvertedIndexWriter.write(docIDtoFreq._freq);
                    freqEnd += docIDtoFreq._freq.length;
                }
                appendToLexicon(lexiconWriter, term, indexStart, indexEnd, freqEnd);
                indexStart = freqEnd;
                indexEnd = indexStart;
            }
            bufferedInvertedIndexWriter.close();
            invertedIndexWriter.close();
            lexiconWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        _tempFileID++;
        sortedKeys.clear();
        _MapWordToDocID.clear();
    }

    /**
     * This function takes a WarcReader object and parse it into sub block inverted index
     * @param reader a WarcReader object which contains documents
     * @param docIDBufferedWriter a buffered writer for writing docIDs
     * @param currFileNum the current file index
     * @param totalFileNum the number of total files that need process
     */
    void parseReader(WarcReader reader, BufferedWriter docIDBufferedWriter, int currFileNum, int totalFileNum) {
        Parser parser = new Parser();
        try {
            InputStream payloadStream = null;
            BufferedInputStream temp = null;
            for (WarcRecord record : reader) {
                payloadStream = record.getPayload().getInputStreamComplete();
                temp = new BufferedInputStream(payloadStream);
                String payload = IOUtils.toString(temp, "UTF-8");
                HeaderLine filename = record.getHeader("WARC-Target-URI");
                docIDBufferedWriter.write(filename.value + "\n");
                _fileDocID++;
                List<String> wordList = new ArrayList<>();
                parser.getWordsList(payload, wordList);
                addWordToFreq(wordList);
                addToPosting();
                _MapWordToFreq.clear();
            }
            if (temp != null) {
                temp.close();
            }
            if (payloadStream != null) {
                payloadStream.close();
            }
            System.out.println("Total pages: " + _fileDocID);
            if (_currBlockSizeBytes >= _maxBlockSizeBytes || currFileNum == totalFileNum - 1) {
                writeToInvertedIndex();
                _currBlockSizeBytes = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }

}
