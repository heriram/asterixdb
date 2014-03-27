/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.tokenorder;

import java.util.Collection;
import java.util.HashMap;
import java.util.TreeSet;

public class TokenRankFrequency implements TokenRank {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final HashMap<String, Integer> ranksMap = new HashMap<String, Integer>();
    private int crtRank = 0;

    public int add(String token) {
        int prevRank = crtRank;
        ranksMap.put(token, prevRank);
        crtRank++;
        return prevRank;
    }

    public Integer getRank(String token) {
        return ranksMap.get(token);
    }

    public Collection<Integer> getTokenRanks(Iterable<String> tokens) {
        TreeSet<Integer> ranksCol = new TreeSet<Integer>();
        for (String token : tokens) {
            Integer rank = getRank(token);
            if (rank != null) {
                ranksCol.add(rank);
            }
        }
        return ranksCol;
    }

    @Override
    public String toString() {
        return "[" + crtRank + ",\n " + ranksMap + "\n]";
    }
}
