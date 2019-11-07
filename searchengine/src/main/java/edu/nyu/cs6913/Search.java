package edu.nyu.cs6913;

import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

class Search {
    private RandomAccessFile _randomAccessIndex;
    private vByteCompression _decoder;
    Search(String indexPath) throws FileNotFoundException {
        _randomAccessIndex = new RandomAccessFile(indexPath + "InvertedIndex", "r");
        _decoder = new vByteCompression();
    }

    private List<Long> getDocIDList(long start, long end) throws IOException {
        _randomAccessIndex.seek(start);
        int length = (int) (end - start);
        byte[] encodedIDs = new byte[length];
        _randomAccessIndex.read(encodedIDs);
        return _decoder.vByteDecodeArray(encodedIDs);
    }

    private List<Long> getFreqList(long start, long end) throws IOException {
        int length = (int) (end - start);
        byte[] encodedFreq = new byte[length];
        _randomAccessIndex.read(encodedFreq);
        return _decoder.vByteDecodeArray(encodedFreq);
    }

    private Set<Long> intersection(List<List<Long>> lists) {
        Set<Long> result = Sets.newHashSet(lists.get(0));
        for (List<Long> numbers : lists) {
            result = Sets.intersection(result, Sets.newHashSet(numbers));
        }
        return result;
    }

    List<Result> conjunctiveSearch(String[] commands, Lexicon lexicon, DocID docID, long avgLength) throws IOException {
        final int length = commands.length;
        PriorityQueue<Result> top10 = new PriorityQueue<>(Comparator.comparingDouble(o -> o._BM25score));
        List<List<Long>> listOfIDList = new ArrayList<>();
        List<List<Long>> listOfFreqList = new ArrayList<>();
        ArrayList<String> containedTerm = new ArrayList<>();
        for (int i = 1; i < length; i++) {
            if (!lexicon.contains(commands[i])) {
                System.out.println(String.format("Could not find %s", commands[i]));
                return new LinkedList<>();
            }
            containedTerm.add(commands[i]);
            long[] info = lexicon.getInfo(commands[i]);
            List<Long> IDList = getDocIDList(info[0], info[1]);
            List<Long> FreqList = getFreqList(info[1], info[2]);
            listOfIDList.add(IDList);
            listOfFreqList.add(FreqList);
        }
        Set<Long> filteredResult = intersection(listOfIDList);
        for (long l : filteredResult) {
            int index = listOfIDList.get(0).indexOf(l);
            String[] docInfo = docID.get(l).split(" ");
            String url = docInfo[0];
            long docLength = Long.parseLong(docInfo[1]);
            Result result = new Result(l, docID.getSize(), listOfIDList.get(0).size(), listOfFreqList.get(0).get(index), docLength, avgLength, 1.2, 0.75, url, containedTerm);
            for (int i = 1; i < listOfIDList.size(); i++) {
                int nextIndex = listOfIDList.get(i).indexOf(l);
                Result nextResult = new Result(l, docID.getSize(), listOfIDList.get(i).size(), listOfFreqList.get(i).get(nextIndex), docLength, avgLength, 1.2, 0.75, url, containedTerm);
                result.add(nextResult);
            }
            if (top10.size() < 10) {
                top10.add(result);
            }
            else if (top10.peek()._BM25score < result._BM25score){
                top10.poll();
                top10.add(result);
            }
        }
        LinkedList<Result> finalResult = new LinkedList<>();
        while (!top10.isEmpty()) {
            finalResult.addFirst(top10.poll());
        }
        return finalResult;
    }

    List<Result> disjunctiveSearch(String[] commands, Lexicon lexicon, DocID docID, long avgLength) throws IOException {
        final int length = commands.length;
        PriorityQueue<Result> top10 = new PriorityQueue<>(Comparator.comparingDouble(o -> o._BM25score));
        for (int i = 1; i < length; i++) {
            if (lexicon.contains(commands[i])) {
                ArrayList<String> containedTerm = new ArrayList<>();
                containedTerm.add(commands[i]);
                long[] info = lexicon.getInfo(commands[i]);
                List<Long> IDList = getDocIDList(info[0], info[1]);
                List<Long> FreqList = getFreqList(info[1], info[2]);
                int ListSize = IDList.size();
                for (int j = 0; j < ListSize; j++) {
                    String[] docInfo = docID.get(IDList.get(j)).split(" ");
                    String url = docInfo[0];
                    long docLength = Long.parseLong(docInfo[1]);
                    Result currResult = new Result(IDList.get(j), docID.getSize(), ListSize, FreqList.get(j), docLength, avgLength, 1.2, 0.75, url, containedTerm);
                    if (top10.size() < 10) {
                        top10.add(currResult);
                    }
                    else if (top10.peek()._BM25score < currResult._BM25score){
                        top10.poll();
                        top10.add(currResult);
                    }
                }
            }
        }
        LinkedList<Result> finalResult = new LinkedList<>();
        while (!top10.isEmpty()) {
            finalResult.addFirst(top10.poll());
        }
        return finalResult;
    }

    void close() throws IOException {
        _randomAccessIndex.close();
    }
}
