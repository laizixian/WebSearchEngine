package edu.nyu.cs6913;

import org.apache.commons.io.FilenameUtils;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

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

    private void loadDocID(DocID docID, String filePath) throws IOException {
        BufferedReader bfd = new BufferedReader(new FileReader(filePath));
        long idNum = 1;
        String line = bfd.readLine();
        while (line != null) {
            docID.add(idNum, line);
            line = bfd.readLine();
            idNum++;
        }
        bfd.close();
    }

    private void generateSnippet(mongodbDriver mongodb, Result result) {
        System.out.println("Snippet of result: ");
        Document doc = mongodb.getWebsite(result.get_docID());
        String content = doc.get("content").toString().toLowerCase();
        String[] lines = content.split("\n");
        ArrayList<String> containedLines = new ArrayList<>();
        ArrayList<String> terms = result.get_containedTerms();
        String[] termArray = new String[terms.size()];
        Map<String, List<String>> termToLines = new HashMap<>();
        for (String line : lines) {
            for (String term : terms) {
                if (line.contains(term)) {
                    List<String> termList = termToLines.getOrDefault(term, new ArrayList<>());
                    termList.add(line);
                    termToLines.put(term, termList);
                }
            }
        }
        int maxLines = 8 / terms.size();
        Random rand = new Random();
        for (Map.Entry<String, List<String>> entry : termToLines.entrySet()) {
            List<String> termList = entry.getValue();
            maxLines = Math.min(maxLines, termList.size());
            for (int i = 0; i < maxLines; i++) {
                int randomIndex = rand.nextInt(termList.size());
                String randomLine = termList.get(randomIndex);
                System.out.println(randomLine);
                termList.remove(randomIndex);
            }
        }
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        String home = System.getProperty("user.home");
        String inputPath = args[0];
        String trueInputPath = FilenameUtils.normalize(home + inputPath);
        String DocIDPath = trueInputPath + "DocID/";
        String indexPath = trueInputPath + "InvertedIndex/";
        String LexiconPath = trueInputPath + "Lexicon/";

        queryProcessing processor = new queryProcessing();

        System.out.println("Loading Lexicon");
        Lexicon lexicon = new Lexicon();
        processor.loadLexicon(lexicon, LexiconPath + "Lexicon");
        System.out.println(lexicon.getSize());

        System.out.println("Loading DocIDs");
        DocID docID = new DocID();
        processor.loadDocID(docID, DocIDPath + "DocIDs");
        System.out.println(docID.getSize());
        Scanner scanner = new Scanner(System.in);

        mongodbDriver mongodb = new mongodbDriver("localhost", 32770);
        mongodb.setCollection("website", "info");
        Document info = mongodb.getDocLength();
        long avgLength = Long.parseLong((String) info.get("content")) / docID.getSize();
        mongodb.setCollection("website", "siteContent");
        while (true) {
            System.out.println("please input 1(conjunctive) or 2(disjunctive) + search terms or 3(exit)");
            String command = scanner.nextLine();
            if (command.equals("3")) {
                break;
            }
            String[] commands = command.split(" ");

            if (commands[0].equals("1") || commands[0].equals("2")) {
                Search search = new Search(indexPath);
                List<Result> finalResult;
                if (commands[0].equals("1")) {
                    finalResult = search.conjunctiveSearch(commands, lexicon, docID, avgLength);
                    for (Result r : finalResult) {
                        System.out.println(r);
                        processor.generateSnippet(mongodb, r);
                    }
                }
                else if (commands[0].equals("2")) {
                    finalResult = search.disjunctiveSearch(commands, lexicon, docID, avgLength);
                    for (Result r : finalResult) {
                        System.out.println(r);
                        processor.generateSnippet(mongodb, r);
                    }
                }
                search.close();
            }
        }


    }
}
