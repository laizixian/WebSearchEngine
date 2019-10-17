package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;
import org.jwat.warc.WarcReader;

import java.io.*;

public class createInvertedIndex {
    public static void main(String[] args) {
        //convert input into file path
        String inputPath = args[0];
        int RAM_Size = Integer.parseInt(args[1]);
        String home = System.getProperty("user.home");
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        File inputDir = new File(trueInputPath);
        FilenameFilter gzFilter = (dir, name) -> name.toLowerCase().endsWith(".gz");
        File[] inputFiles = inputDir.listFiles(gzFilter);
        Parser parser = new Parser();
        assert inputFiles != null;
        Postings subPostings = new Postings(RAM_Size, trueInputPath);
        int totalFiles = inputFiles.length;
        //create sub inverted indexes
        File DocIDFile = new File(subPostings._tempDocID + "DocIDs");
        try {
            DocIDFile.createNewFile();
            FileWriter docIDFileWriter = new FileWriter(DocIDFile);
            BufferedWriter docIDBufferedWriter = new BufferedWriter(docIDFileWriter);
            for (int i = 0; i < totalFiles; i++) {
                String inputFilePath = trueInputPath + inputFiles[i].getName();
                WarcReader warcReader = parser.readWarc(inputFilePath);
                System.out.println("Parsing: " + inputFilePath);
                subPostings.parseReader(warcReader, docIDBufferedWriter, i, totalFiles);
            }
            docIDBufferedWriter.close();
            docIDFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Start merging sub inverted indexes");
        Merger merger = new Merger(trueInputPath);
        merger.startMerge();
        System.out.println("Finished merging");
    }
}
