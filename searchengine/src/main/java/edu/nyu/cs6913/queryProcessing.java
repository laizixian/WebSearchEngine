package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class queryProcessing {
    private queryProcessing() {

    }

    private void loadLexicon(Lexicon lexicon, String filePath) throws IOException {
        BufferedReader bfd = new BufferedReader(new FileReader(filePath));
        String line = bfd.readLine();
        while (line != null) {
            lexicon.add(line);
            line = bfd.readLine();
        }
        bfd.close();
    }

    public static void main(String[] args) throws IOException {
        String home = System.getProperty("user.home");
        String inputPath = args[0];
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        String DocIDPath = trueInputPath + "DocID/";
        String indexPath = trueInputPath + "InvertedIndex/";
        String LexiconPath = trueInputPath + "Lexicon/";

        queryProcessing processor = new queryProcessing();

        System.out.println("Loading lexicon and DocID files");
        Lexicon lexicon = new Lexicon();
        processor.loadLexicon(lexicon, LexiconPath + "Lexicon");

        System.out.println("Loading DocIDs");

    }
}
