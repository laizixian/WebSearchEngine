package edu.nyu.cs6913;

import java.util.ArrayList;
import java.util.Arrays;

public class Result {
    double _BM25score;
    private long _ft;
    private long _fdt;
    private long _d;
    private long _dAvg;
    private String _url;
    private ArrayList<String> _containedTerms;
    private long _docID;

    Result(long docID, long totalDoc, long ft, long fdt, long d, long dAvg, double k1, double b, String url, ArrayList<String> containedTerms) {
        _docID = docID;
        _ft = ft;
        _fdt = fdt;
        _d = d;
        _dAvg = dAvg;
        _url = url;
        _containedTerms = containedTerms;
        double K = k1 * ((1 - b) + (b * ((double)_d / (double)_dAvg)));
        _BM25score = Math.log10((totalDoc - _ft + 0.5d) / (_ft + 0.5d)) * (((k1 + 1d) * _fdt) / (K + _fdt));
    }

    long get_docID(){
        return _docID;
    }

    ArrayList<String> get_containedTerms(){
        return _containedTerms;
    }

    void add(Result target) {
        _ft += target._ft;
        _fdt += target._fdt;
        _BM25score += target._BM25score;
    }
    @Override
    public String toString() {
        return String.format("The url of the page is : %s\n" +
                             "The docID is : %d\n" +
                             "The BM25 score is : %f\n" +
                             "Words %s occurred in the document\n" +
                             "They occurred %d time\n" +
                             "The length of the document is %d\n" +
                             "The average length of all document is %d\n" +
                             "The number of documents contain terms is %d", _url, _docID, _BM25score, Arrays.toString(_containedTerms.toArray()), _fdt, _d, _dAvg, _ft);
    }

}
