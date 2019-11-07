package edu.nyu.cs6913;

import java.util.HashMap;
import java.util.Map;

class DocID {
    private Map<Long, String> _mapDocIDToDocName;
    DocID() {
        _mapDocIDToDocName = new HashMap<>();
    }

    void add(Long docID, String docName) {
        _mapDocIDToDocName.put(docID, docName);
    }
    String get (long id) {
        return _mapDocIDToDocName.get(id);
    }
    long getSize() {
        return _mapDocIDToDocName.size();
    }
}
