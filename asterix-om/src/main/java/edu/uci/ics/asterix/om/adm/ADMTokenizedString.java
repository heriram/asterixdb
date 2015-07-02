package edu.uci.ics.asterix.om.adm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.uci.ics.asterix.external.library.textanalysis.TextAnalyzer;

public class ADMTokenizedString {
    private final static TextAnalyzer ANALYZER = TextAnalyzer.INSTANCE;
    private Object tokenCollection = null;
    private String terms[] = null;

    public final static boolean ANALYSED = true;
    public final static boolean NOT_ANALYSED = !ANALYSED;
    public final static boolean ORDERED = false;
    public final static boolean UNORDERED = !ORDERED;

    public ADMTokenizedString(String txt) {
        this.terms = txt.split("\\s");
        this.tokenCollection = new HashSet<String>();
        ((HashSet<String>) this.tokenCollection).addAll(Arrays.asList(terms));
    }

    public ADMTokenizedString(boolean is_analyzed, String txt) {

        if (is_analyzed) {
            ANALYZER.analyze(txt);
            this.terms = ANALYZER.getAnalyzedTerms();
        } else {
            this.terms = txt.split("\\s");
        }
    }

    private Map<String, List<Integer>> saveTokenOcurrences() {
        Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();

        for (int i = 0; i < this.terms.length; i++) {
            List<Integer> occurences;
            if (map.containsKey(this.terms[i]))
                occurences = map.get(this.terms[i]);
            else
                occurences = new ArrayList<Integer>();

            occurences.add(i);
            map.put(this.terms[i], occurences);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public ADMArray getTokenizedString(boolean is_unordered) {
        if (is_unordered) {
            this.tokenCollection = new HashSet<String>();
            return new ADMUnorderedArray(this.terms);
        } else {
            return new ADMOrderedArray(this.terms);
        }

    }

    public ADMArray getTokenizedStringWithOccurrences() {
        Map<String, List<Integer>> map = saveTokenOcurrences();
        Iterator<Entry<String, List<Integer>>> it = map.entrySet().iterator();
        ADMArray adm_array = new ADMOrderedArray();
        while (it.hasNext()) {
            adm_array.put(it.next());
        }

        return adm_array;

    }

    public ADMArray getTokenizedString() {
        return getTokenizedString(UNORDERED);
    }

}
