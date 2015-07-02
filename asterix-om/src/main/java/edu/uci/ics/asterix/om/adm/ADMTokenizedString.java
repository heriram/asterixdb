package edu.uci.ics.asterix.om.adm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ADMTokenizedString {
    private Object tokenCollection = null;
    private String terms[] = null;

    public final static boolean ORDERED = false;
    public final static boolean UNORDERED = !ORDERED;

    public ADMTokenizedString(String txt) {
        this.terms = tokenize(txt);
        this.tokenCollection = new HashSet<String>();
        ((HashSet<String>) this.tokenCollection).addAll(Arrays.asList(terms));
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
    public AbstractADMArray getTokenizedString(boolean is_unordered) {
        if (is_unordered) {
            this.tokenCollection = new HashSet<String>();
            return new ADMUnorderedArray(this.terms);
        } else {
            return new ADMOrderedArray(this.terms);
        }

    }

    public AbstractADMArray getTokenizedStringWithOccurrences() {
        Map<String, List<Integer>> map = saveTokenOcurrences();
        Iterator<Entry<String, List<Integer>>> it = map.entrySet().iterator();
        AbstractADMArray adm_array = new ADMOrderedArray();
        while (it.hasNext()) {
            adm_array.put(it.next());
        }

        return adm_array;

    }

    public AbstractADMArray getTokenizedString() {
        return getTokenizedString(UNORDERED);
    }

    public String[] tokenize(String str) {
        char [] textCharArray = str.toCharArray();
        int len = textCharArray.length;
        String[] temp = new String[(len / 2) + 2];
        int wordCount = 0;
        char wordBuff[] = new char[len];
        int index = 0;

        for (int i = 0; i < len; i++) {
            char c = textCharArray[i];
            c = Character.toLowerCase(c);
            switch (c) {
                case '\'': // Remove "'s"
                    if (i==(len-1))
                        break;

                    char next_c =  textCharArray[i+1];
                    if (next_c == 's') {
                        i++;
                    } else if (next_c == 't') { // keep 't forms for now
                        wordBuff[index] = c;
                        index++;
                        wordBuff[index] = next_c;
                        index++;
                        i++;
                    }
                    break;
                case ' ':
                case '\n':
                case '\t':
                case '\r':
                    if (index > 0) {
                        String word = new String(wordBuff, 0, index);
                        index = 0;
                        temp[wordCount] = word;
                        wordCount++;

                    }
                    break;

                case '&':
                case '@':
                case '-':
                case '_':
                    wordBuff[index] = c;
                    index++;

                    break;
                default:
                    if ((c >= toDigit(0) && c <= toDigit(9)) || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                            || (c >= 0x00C0 && c <= 0x00F6) || (c >= 0x00F8 && c <= 0x02AF)) {
                        wordBuff[index] = c;
                        index++;
                    }
            }

        }

        String lastToken = new String(wordBuff, 0, index).trim();
        if (!lastToken.isEmpty()) {
            temp[wordCount] = lastToken;
            wordCount++;
        }
        String result[] = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);

        return result;
    }

    public static char toDigit(int n) {
        return (char) ('0' + n);
    }

}
