package edu.nyu.cs6913;

import java.util.HashMap;
import java.util.Map;

public class DocID {
    Map<Long, String> _mapDocIDToDocName;
    public DocID() {
        _mapDocIDToDocName = new HashMap<>();
    }

    public void add(Long DocID, String docName) {
        _mapDocIDToDocName.put(DocID, docName);
    }
}
