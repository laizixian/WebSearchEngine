package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;
import org.jwat.warc.WarcReader;

import java.io.File;
import java.io.FilenameFilter;

public class createInvertedIndex {
    public static void main(String[] args) {
        String inputPath = args[0];
        int RAM_Size = Integer.parseInt(args[1]);
        String home = System.getProperty("user.home");
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        File inputDir = new File(trueInputPath);
        FilenameFilter gzFilter = (dir, name) -> name.toLowerCase().endsWith(".gz");
        File[] inputFiles = inputDir.listFiles(gzFilter);
        Parser parser = new Parser();
        assert inputFiles != null;
        System.out.println(trueInputPath);
        Postings subPostings = new Postings(RAM_Size, trueInputPath);
        int totalFiles = inputFiles.length;
        for (int i = 0; i < totalFiles; i++) {
            String inputFilePath = trueInputPath + inputFiles[i].getName();
            WarcReader warcReader = parser.readWarc(inputFilePath);
            System.out.println(inputFilePath);
            subPostings.parseReader(warcReader, i, totalFiles);
        }
    }
}
