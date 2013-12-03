/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.api.http.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.bootstrap.FeedLifecycleListener;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityDetails;

public class FeedDataProviderServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

        String feedName = request.getParameter("feed");
        String datasetName = request.getParameter("dataset");
        String dataverseName = request.getParameter("dataverse");

        String report = getFeedReport(feedName, datasetName, dataverseName);
        System.out.println(" REPORT " + report);
        long timestamp = System.currentTimeMillis();
        JSONObject obj = null;
        if (report != null) {
            try {
                obj = new JSONObject();
                obj.put("type", "report");
                obj.put("time", timestamp);
                obj.put("value", report);
            } catch (JSONException jsoe) {
                throw new IOException(jsoe);
            }
        } else {
            obj = verifyIfFeedIsAlive(dataverseName, feedName, datasetName);
        }

        PrintWriter out = response.getWriter();
        out.println(obj.toString());
    }

    private String getFeedReport(String feedName, String datasetName, String dataverseName) {
        FeedConnectionId feedId = new FeedConnectionId(dataverseName, feedName, datasetName);
        LinkedBlockingQueue<String> queue = FeedLifecycleListener.INSTANCE.getFeedReportQueue(feedId);
        String report = null;
        try {
            report = queue.poll(25, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return report;
    }

    private JSONObject verifyIfFeedIsAlive(String dataverseName, String feedName, String datasetName) {
        JSONObject obj = new JSONObject();
        try {
            MetadataTransactionContext ctx = MetadataManager.INSTANCE.beginTransaction();
            List<FeedActivity> activities = MetadataManager.INSTANCE.getFeedActivity(ctx, new FeedId(dataverseName,
                    feedName));
            FeedActivity activity = null;
            if (activities != null && !activities.isEmpty()) {
                for (FeedActivity fa : activities) {
                    if (fa.getDatasetName().equals(datasetName)) {
                        activity = fa;
                        break;
                    }
                }
            }
            if (activity != null) {
                Map<String, String> activityDetails = activity.getFeedActivityDetails();
                String ingestLocations = activityDetails.get(FeedActivityDetails.COLLECT_LOCATIONS);
                String computeLocations = activityDetails.get(FeedActivityDetails.COMPUTE_LOCATIONS);
                String storageLocations = activityDetails.get(FeedActivityDetails.STORAGE_LOCATIONS);
                obj.put("status", "active");
                obj.put("type", "reload");
                obj.put("ingestLocations", ingestLocations);
                obj.put("computeLocations", computeLocations);
                obj.put("storageLocations", storageLocations);
                System.out.println(" RE LOADING " + " ingestion at " + ingestLocations + " compute at "
                        + computeLocations + " storage at " + storageLocations);
            } else {
                obj.put("status", "ended");
            }
        } catch (Exception e) {
            // ignore
        }
        return obj;

    }
}
