package edu.nyu.cs6913;

import java.util.HashMap;
import java.util.Map;

class Lexicon {

    private Map<String, termInfo> _mapTermToInfo;

    Lexicon() {
        _mapTermToInfo = new HashMap<>();
    }

    void add(String lexiconLine) {
        String[] infoArray = lexiconLine.split(" ");
        _mapTermToInfo.put(infoArray[0], new termInfo(Long.parseLong(infoArray[1]), Long.parseLong(infoArray[2]), Long.parseLong(infoArray[3])));
    }

    boolean contains(String term) {
        return _mapTermToInfo.containsKey(term);
    }

    long[] getInfo(String term) {
        long[] info = new long[3];
        termInfo queryTerm = _mapTermToInfo.get(term);
        info[0] = queryTerm._startByte;
        info[1] = queryTerm._startFreq;
        info[2] = queryTerm._endByte;
        return info;
    }

    long getSize() {
        return _mapTermToInfo.size();
    }

    private class termInfo {
        long _startByte;
        long _startFreq;
        long _endByte;
        termInfo(long startByte, long startFreq, long endByte) {
            _startByte = startByte;
            _startFreq = startFreq;
            _endByte = endByte;
        }
    }
}
