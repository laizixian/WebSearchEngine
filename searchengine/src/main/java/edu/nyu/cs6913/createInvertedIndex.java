package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;
import org.jwat.warc.WarcReader;

import java.io.File;
import java.io.FilenameFilter;

public class createInvertedIndex {
    public static void main(String[] args) {
        String inputPath = args[0];
        String home = System.getProperty("user.home");
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        File inputDir = new File(trueInputPath);
        FilenameFilter gzFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".gz");
            }
        };
        File[] inputFiles = inputDir.listFiles(gzFilter);
        Parser parser = new Parser();
        assert inputFiles != null;
        for (File f : inputFiles) {
            String inputFilePath = trueInputPath + f.getName();
            WarcReader warcReader = parser.readWarc(inputFilePath);
            System.out.println(inputFilePath);
            //TO-DO
            // pass the input file into Postings class to process
        }
    }
}
