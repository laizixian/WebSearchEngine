package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.*;

public class Merger {

    private String _inputPath;
    private Map<String, BufferedInputStream> _mapFileToInputStream;
    Merger(String inputPath) {
        _inputPath = inputPath;
        _mapFileToInputStream = new HashMap<>();
    }

    private class oneLexicon {
        String _termID;
        String _filename;
        long _startPos;
        long _freqPos;
        long _endPos;
        oneLexicon(String lexiconString, String filename) {
            String[] infoArray = lexiconString.split(" ");
            _filename = filename;
            _termID = infoArray[0];
            _startPos = Long.parseLong(infoArray[1]);
            _freqPos = Long.parseLong(infoArray[2]);
            _endPos = Long.parseLong(infoArray[3]);
        }
    }

    /**
     * write to the index file
     * @param indexFile the buffered output stream
     * @param docIdList a list of byte array
     * @param freqList a list of byte array
     * @throws IOException throws exception if can not write to the output stream
     */
    private void writeToIndexFile(BufferedOutputStream indexFile, List<byte[]> docIdList, List<byte[]> freqList) throws IOException {
        for (byte[] ids : docIdList) {
            indexFile.write(ids);
        }
        docIdList.clear();
        for (byte[] freq : freqList) {
            indexFile.write(freq);
        }
        freqList.clear();
    }

    /**
     * close the file input stream
     * @throws IOException throws exception if can not close the input stream
     */
    private void closeFileInputStream() throws IOException {
        for (Map.Entry<String, BufferedInputStream> entry : _mapFileToInputStream.entrySet()) {
            entry.getValue().close();
        }
        _mapFileToInputStream.clear();
    }

    /**
     * delete all the temp files
     */
    void deleteTempFiles() {
        String _invertedIndexFilePath = "temp/InvertedIndex";
        String invertedIndexPath = _inputPath + _invertedIndexFilePath;
        String _lexiconFilePath = "temp/Lexicon";
        String lexiconPath = _inputPath + _lexiconFilePath;
        File invertedIndexDir = new File(invertedIndexPath);
        File lexiconDir = new File(lexiconPath);
        FilenameFilter invertedIndexFilter = (dir, name) -> name.endsWith("tempIndex");
        FilenameFilter lexiconFilter = (dir, name) -> name.endsWith("tempLexicon");
        File[] invertedIndexFiles = invertedIndexDir.listFiles(invertedIndexFilter);
        File[] lexiconFiles = lexiconDir.listFiles(lexiconFilter);
        assert invertedIndexFiles != null;
        for (File f : invertedIndexFiles) {
            f.delete();
        }
        assert lexiconFiles != null;
        for (File f : lexiconFiles) {
            f.delete();
        }
    }

    /**
     * merge the temp files
     */
    void startMerge() {
        String _invertedIndexFilePath = "temp/InvertedIndex";
        String invertedIndexPath = _inputPath + _invertedIndexFilePath;
        String _lexiconFilePath = "temp/Lexicon";
        String lexiconPath = _inputPath + _lexiconFilePath;
        File invertedIndexDir = new File(invertedIndexPath);
        File lexiconDir = new File(lexiconPath);
        FilenameFilter invertedIndexFilter = (dir, name) -> name.endsWith("tempIndex");
        FilenameFilter lexiconFilter = (dir, name) -> name.endsWith("tempLexicon");
        File[] invertedIndexFiles = invertedIndexDir.listFiles(invertedIndexFilter);
        File[] lexiconFiles = lexiconDir.listFiles(lexiconFilter);
        assert lexiconFiles != null;
        ArrayList<oneLexicon> lexiconsList = new ArrayList<>();
        /*
        PriorityQueue<> fileReaderList = new PriorityQueue();

         */
        for (File f : lexiconFiles) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(f));
                String line = reader.readLine();
                while (line != null) {
                    lexiconsList.add(new oneLexicon(line, f.getName().split("\\.")[0]));
                    line = reader.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        lexiconsList.sort((o1, o2) -> o1._termID.compareTo(o2._termID) != 0 ? o1._termID.compareTo(o2._termID) : o1._filename.compareTo(o2._filename));
        assert invertedIndexFiles != null;
        for (File f : invertedIndexFiles) {
            try{
                _mapFileToInputStream.put(f.getName().split("\\.")[0], new BufferedInputStream(new FileInputStream(f)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        File finalIndex = new File(invertedIndexPath + "/InvertedIndex");
        File finalLexicon = new File(lexiconPath + "/Lexicon");
        try {
            BufferedOutputStream indexOutputStream = new BufferedOutputStream(new FileOutputStream(finalIndex));
            BufferedWriter lexiconWriter = new BufferedWriter(new FileWriter(finalLexicon));
            String lastTerm = null;
            ArrayList<byte[]> docIDList = new ArrayList<>();
            ArrayList<byte[]> freqList = new ArrayList<>();
            long newStart = 0;
            long newFreqStart = 0;
            long newEnd = 0;
            for (oneLexicon temp : lexiconsList) {
                if (lastTerm != null && lastTerm.compareTo(temp._termID) != 0) {
                    for (byte[] b : docIDList) {
                        newFreqStart += b.length;
                    }
                    newEnd = newFreqStart;
                    for (byte[] b : freqList) {
                        newEnd += b.length;
                    }
                    lexiconWriter.write(lastTerm + " " + newStart + " " + newFreqStart + " " + newEnd + "\n");
                    newStart = newEnd;
                    newFreqStart = newStart;
                    writeToIndexFile(indexOutputStream, docIDList, freqList);
                }
                int IDLength = (int)(temp._freqPos - temp._startPos);
                byte[] currID = new byte[IDLength];
                int FreqLength = (int)(temp._endPos - temp._freqPos);
                byte[] currFreq = new byte[FreqLength];
                _mapFileToInputStream.get(temp._filename).read(currID, 0, IDLength);
                _mapFileToInputStream.get(temp._filename).read(currFreq, 0, FreqLength);
                docIDList.add(currID);
                freqList.add(currFreq);
                lastTerm = temp._termID;
            }
            if (!docIDList.isEmpty()) {
                for (byte[] b : docIDList) {
                    newFreqStart += b.length;
                }
                newEnd = newFreqStart;
                for (byte[] b : freqList) {
                    newEnd += b.length;
                }
                lexiconWriter.write(lastTerm + " " + newStart + " " + newFreqStart + " " + newEnd + "\n");
                writeToIndexFile(indexOutputStream, docIDList, freqList);
            }
            indexOutputStream.close();
            lexiconWriter.close();
            closeFileInputStream();
            deleteTempFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String inputPath = "/Documents/WebSearchEngine/test/";
        String home = System.getProperty("user.home");
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        Merger merger = new Merger(trueInputPath);
        merger.startMerge();
    }
}
