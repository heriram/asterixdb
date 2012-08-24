package edu.uci.ics.asterix.api.http.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class APIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String query = request.getParameter("query");
        String printExprParam = request.getParameter("print-expr-tree");
        String printRewrittenExprParam = request.getParameter("print-rewritten-expr-tree");
        String printLogicalPlanParam = request.getParameter("print-logical-plan");
        String printOptimizedLogicalPlanParam = request.getParameter("print-optimized-logical-plan");
        String printJob = request.getParameter("print-job");
        String strIP = request.getParameter("hyracks-ip");
        String strPort = request.getParameter("hyracks-port");
        String strDisplayResult = request.getParameter("display-result");
        int port = Integer.parseInt(strPort);
        PrintWriter out = response.getWriter();
        response.setContentType("text/html");
        out.println("<H1>Input statements:</H1>");
        printInHtml(out, query);
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                if (hcc == null) {
                    hcc = new HyracksConnection(strIP, port);
                    context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
                }
            }
            AQLParser parser = new AQLParser(new StringReader(query));
            Query q = (Query) parser.Statement();
            SessionConfig pc = new SessionConfig(port, true, isSet(printExprParam), isSet(printRewrittenExprParam),
                    isSet(printLogicalPlanParam), isSet(printOptimizedLogicalPlanParam), false, isSet(printJob));
            pc.setGenerateJobSpec(true);

            MetadataManager.INSTANCE.init();
            String dataverseName = null;

            if (q != null) {
                dataverseName = postDmlStatement(hcc, q, out, pc);
            }

            if (q.isDummyQuery()) {
                return;
            }

            Pair<AqlCompiledMetadataDeclarations, JobSpecification> metadataAndSpec = APIFramework.compileQuery(
                    dataverseName, q, parser.getVarCounter(), null, null, pc, out, DisplayFormat.HTML, null);
            JobSpecification spec = metadataAndSpec.second;
            GlobalConfig.ASTERIX_LOGGER.info(spec.toJSON().toString(1));
            AqlCompiledMetadataDeclarations metadata = metadataAndSpec.first;
            long startTime = System.currentTimeMillis();
            APIFramework.executeJobArray(hcc, new JobSpecification[] { spec }, out, DisplayFormat.HTML);
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.00;
            out.println("<H1>Result:</H1>");

            out.println("<PRE>");
            out.println(metadata.getOutputFile().getNodeName() + ":"
                    + metadata.getOutputFile().getLocalFile().getFile().getPath());
            out.println("Duration: " + duration);
            out.println("</PRE>");

            if (isSet(strDisplayResult)) {
                out.println("<PRE>");
                displayFile(metadata.getOutputFile().getLocalFile().getFile(), out);
                out.println("</PRE>");
            }
        } catch (ParseException pe) {
        	String message = pe.getMessage();
        	message = message.replace("<", "&lt");
        	message = message.replace(">", "&gt");
        	out.println("SyntaxError:" + message);
        	int pos = message.indexOf("line");
        	if (pos > 0) {
        		int columnPos = message.indexOf(",", pos + 1 + "line".length());
        		int lineNo = Integer.parseInt(message.substring(pos + "line".length() + 1, columnPos));
        		String line = query.split("\n")[lineNo - 1];            
        		out.println("==> " + line);
        	}
        } catch (Exception e) {
            out.println(e.getMessage());
            e.printStackTrace(out);
       }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter out = response.getWriter();
        response.setContentType("text/html");
        final String form = "<form method=\"post\">"
                + "<center><textarea cols=\"80\" rows=\"25\" name=\"query\" ></textarea><br/>"
                + "IP Address: <input type = \"text\" name = \"hyracks-ip\" size=\"15\" maxlength=\"35\" value=\"localhost\" /><br/>"
                + "Port: <input type = \"text\" name = \"hyracks-port\" size=\"5\" maxlength=\"5\" value=\"1098\" /><br/>"
                + "<input type = \"checkbox\" name = \"print-expr-tree\" value=\"true\" />print parsed expressions<P>"
                + "<input type = \"checkbox\" name = \"print-rewritten-expr-tree\" value=\"true\" />print rewritten expressions<P>"
                + "<input type = \"checkbox\" name = \"print-logical-plan\" value=\"true\" checked/>print logical plan<P>"
                + "<input type = \"checkbox\" name = \"print-optimized-logical-plan\" value=\"true\" checked/>print optimized logical plan<P>"
                + "<input type = \"checkbox\" name = \"print-job\" value=\"true\" checked/>print Hyracks job<P>"
                + "<input type = \"checkbox\" name = \"display-result\" value=\"true\" checked/>display NFS file<P>"
                // +
                // "<input type = \"checkbox\" name = \"serialize-as-xml\" value=\"true\">serialize as XML<P>"
                // +
                // "<input type = \"checkbox\" name = \"show-tuples\" value=\"true\">show the entire tuples<P>"
                + "<input type=\"submit\"/>" + "</center>" + "</form>";
        out.println(form);
    }

    private String postDmlStatement(IHyracksClientConnection hcc, Query dummyQ, PrintWriter out, SessionConfig pc)
            throws Exception {

        String dataverseName = APIFramework.compileDdlStatements(hcc, dummyQ, out, pc, DisplayFormat.TEXT);
        Job[] dmlJobSpecs = APIFramework.compileDmlStatements(dataverseName, dummyQ, out, pc, DisplayFormat.HTML);

        long startTime = System.currentTimeMillis();
        APIFramework.executeJobArray(hcc, dmlJobSpecs, out, DisplayFormat.HTML);
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.00;
        out.println("<PRE>Duration of all jobs: " + duration + "</PRE>");
        return dataverseName;
    }

    private static boolean isSet(String requestParameter) {
        return (requestParameter != null && requestParameter.equals("true"));
    }

    private static void printInHtml(PrintWriter out, String s) {
        out.println("<PRE>");
        out.println(s);
        out.println("</PRE>");
    }

    private void displayFile(File localFile, PrintWriter out) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(localFile));
        String inputLine = reader.readLine();
        int i = 0;
        while (inputLine != null) {
            out.println(inputLine);
            inputLine = reader.readLine();
            i++;
            if (i > 500) {
                out.println("...");
                out.println("SKIPPING THE REST OF THE RESULTS");
                break;
            }
        }
        reader.close();
    }
}
