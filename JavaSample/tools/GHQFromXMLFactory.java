package com.amazon.dw.grasshopper.tools;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.amazon.dw.grasshopper.compilerservice.CompilerException;
import com.amazon.dw.grasshopper.compilerservice.ghqfactory.GHQFactory;
import com.amazon.dw.grasshopper.metadata.MetaDataStore;
import com.amazon.dw.grasshopper.model.GHQuery.AggregateFunctions;
import com.amazon.dw.grasshopper.model.GHQuery.FinalizedReportNode;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.Function;
import com.amazon.dw.grasshopper.model.GHQuery.GHQuery;
import com.amazon.dw.grasshopper.model.GHQuery.QueryAttribute;
import com.amazon.dw.grasshopper.model.GHQuery.QueryConstant;
import com.amazon.dw.grasshopper.model.GHQuery.QueryInternalNode;
import com.amazon.dw.grasshopper.model.GHQuery.QueryTreeNode;
import com.amazon.dw.grasshopper.model.GHQuery.OrderingDetails;

import java.util.Arrays;

@Deprecated
public class GHQFromXMLFactory extends GHQFactory {

    private static final String COL_ID_TO_ATTR_ID_MAPPING_FILENAME = "metadata-content/dssAttributeMap.xml";
    private static final String SOURCE = "dssui.xml";
    private static final String GROUP_TO_COL_MAPPING_FILENAME = "group_to_col_name.gh";
    private static final String GROUP_TO_GROUP_MAPPING_FILENAME = "group_to_group.gh";

    HashMap<String, Long> colIdToAttrMapping;
    HashMap<String, List<String>> groupToColMapping;
    HashMap<String, List<String>> groupToGroupMapping;
    HashMap<String, String> altsMapping;
    HashMap<String, String> reverseAltsMapping;
    HashMap<String, String> aggrColUse;

    public GHQFromXMLFactory(MetaDataStore metadata) {
        super(metadata);
        generateGroupedMapping();
        colIdToAttrMapping = retrieveMapping();
        groupToColMapping = loadMappings(GROUP_TO_COL_MAPPING_FILENAME);
        groupToGroupMapping = loadMappings(GROUP_TO_GROUP_MAPPING_FILENAME);
        altsMapping = generateAltsMapping();
        reverseAltsMapping = generateReverseAltsMapping();
        aggrColUse = findCustomCols();
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, List<String>> loadMappings(String filename) {

        HashMap<String, List<String>> toReturn = null;

        try {
            FileInputStream fileIn = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            toReturn = (HashMap<String, List<String>>) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return toReturn;
    }
    
    public List<FinalizedReportNode> handleSpecialCase(Element root, String template){
        String id = (root.getAttributeValue("ID") != null ? 
                root.getAttributeValue("ID") : 
                    root.getAttributeValue("COL_ID"));
        List<FinalizedReportNode> generatedRoots = new ArrayList<FinalizedReportNode>();
        
        
        //SUM(cp.CONTRIBUTION_PROFIT_AMT)/SUM(cp.QUANTITY_SHIPPED)
        if(id.equals("cp_per_unit")){
            QueryTreeNode rootProfitAmt = generateAttribute("cp_contribution_profit_amt", false, template);
            FinalizedReportNode conProfitAmt = new FinalizedReportNode(rootProfitAmt, AggregateFunctions.SUM);
            generatedRoots.add(conProfitAmt);
            
            QueryTreeNode rootQuantShip = generateAttribute("cp_quantity_shipped", false, template);
            FinalizedReportNode conQuantShip = new FinalizedReportNode(rootQuantShip,AggregateFunctions.SUM);
            generatedRoots.add(conQuantShip);
        }
        
        return generatedRoots;
    }
    
    public QueryTreeNode handleSpecialCase(String exp, String template){
        
        
        //sign(trunc(sysdate) - rday.calendar_day ) = 1)  and ( trunc(sysdate) -  rday.calendar_day
        if(exp.equals("sign(trunc(sysdate) - rday.calendar_day ) = 1)  and ( trunc(sysdate) -  rday.calendar_day")){
            
            QueryTreeNode rday = generateAttribute("rday.calendar_day", true, template);
            QueryTreeNode sysdate = new QueryConstant("sysdate");
            QueryTreeNode one = new QueryConstant("1");
            
            QueryTreeNode trunc = new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {sysdate}); 
            QueryTreeNode minus = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {trunc, rday});
            QueryTreeNode sign = new QueryInternalNode(queryFunction(DSSUI_OP.SIGN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {minus});
            QueryTreeNode eq1 = new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {sign, one});
            QueryTreeNode and = new QueryInternalNode(queryFunction(DSSUI_OP.SIGN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {eq1, minus});
            
            return and;
        }
        
        if(exp.equals("mp_asin_street_day_ca  > trunc(sysdate)")){
            
            QueryTreeNode streetDay = generateAttribute("mp_asin_street_day_ca", true, template);
            QueryTreeNode sysdate = new QueryConstant("sysdate");

            QueryTreeNode trunc = new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {sysdate}); 
            QueryTreeNode greater = new QueryInternalNode(queryFunction(DSSUI_OP.GREATER_THAN.getFunctionID(), DBType.SQL.getId()),
                                                            new QueryTreeNode[] {streetDay, trunc});   
            return greater;
        }
        
        if(exp.equals("dacp.competitor_price+dacp.competitor_shipping-dacp.rebate_amount")){
            
            QueryTreeNode comp_price = generateAttribute("dacp.competitor_price", true, template);
            QueryTreeNode comp_shipping = generateAttribute("dacp.competitor_shipping", true, template);
            QueryTreeNode reb_amt = generateAttribute("dacp.rebate_amount", true, template);
            
            QueryTreeNode minus = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                    new QueryTreeNode[] {comp_shipping, reb_amt});
            
            QueryTreeNode plus = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()),
                    new QueryTreeNode[] {comp_price, minus});
                    
            return plus;
        }
        
        return null;
    }
    
    public boolean notSpecialCase(Element root){
        String id = (root.getAttributeValue("ID") != null ? 
                root.getAttributeValue("ID") : 
                    root.getAttributeValue("COL_ID"));
        if(id.equals("cp_per_unit")){
            return false;
        }
 
        return true;
    }
    
    public boolean isSpecialCase(String exp){
        
        if(exp.equals("sign(trunc(sysdate) - rday.calendar_day ) = 1)  and ( trunc(sysdate) -  rday.calendar_day"))
            return true;
        if(exp.equals("mp_asin_street_day_ca  > trunc(sysdate)"))
            return true;
        if(exp.equals("dacp.competitor_price+dacp.competitor_shipping-dacp.rebate_amount"))
            return true;
        
        return false;
    }

    @SuppressWarnings("unchecked")
    public GHQuery generateQuery(Element queryRoot, long queryType) {
        GHQuery newQuery = new GHQuery();
        Collection<FinalizedReportNode> reportedRoots = new ArrayList<FinalizedReportNode>();

        if (queryRoot.getChildren("COLUMN").isEmpty()
                && queryRoot.getChildren("RESTRICTION").isEmpty()
                && queryRoot.getChildren("AND").isEmpty()
                && queryRoot.getChildren("OR").isEmpty()) {
            throw new RuntimeException("SQL only query.");
        }

        String template = queryRoot.getAttributeValue("DESC");

        // Generate ReportedRoots
        List<Element> reportElements = queryRoot.getChildren("COLUMN");

        for (Element root : reportElements) {
            if(notSpecialCase(root)){
                reportedRoots.add(generateRoot(root, template));
            }
            else{
                reportedRoots.addAll(handleSpecialCase(root, template));
            }
        }

        // add to new Query
        newQuery.setReportRoots(reportedRoots);
        newQuery.setConditionQueryTree(generateInternalNode(queryRoot, template));
        return newQuery;
    }

    public FinalizedReportNode generateRoot(Element reportedRoot,
            String template) {
        AggregateFunctions agg = null;
        String id = (reportedRoot.getAttributeValue("ID") != null ? 
                reportedRoot.getAttributeValue("ID") : 
                    reportedRoot.getAttributeValue("COL_ID"));
        id = replaceIdIfNeeded(id);
        if (aggrColUse.containsKey(id)) {
            List<String> pieces = generateDissectedString(aggrColUse.get(id));

            if (pieces.size() == 2) { // should have a funct(operand)
                id = pieces.get(1); // the operand
                agg = findIfAggregate(pieces.get(0), pieces.get(1));
                if (agg != null) {
                    id = id.replace(agg.getDescription(), ""); // remove
                    // distinct, so
                    // generateAttribute
                    // doesn't have
                    // to worry about
                    // it.
                    id = id.replace("COUNT (DISTINCT", "");
                }
            }
        }
        
        QueryTreeNode attribute = generateAttribute(id, false, template);
        FinalizedReportNode newReportNode;
        // Generates a root with ordering and no synonym (cannot be set in
        // DSSUI)
        if (reportedRoot.getAttribute("ORDER") != null) {
            int orderVal = 0;
            try{
                orderVal = Integer.parseInt(reportedRoot
                    .getAttributeValue("ORDER")); // values will be 1-n, but
            }
            catch(NumberFormatException ne){
                throw new RuntimeException("Order is broken, apparently");
            }
            // sign flips
            boolean asc = (orderVal > 0 ? true : false); // -n for desc, n for
            // asc
            OrderingDetails order = new OrderingDetails(asc, Math.abs(orderVal));

            if (agg != null)
                newReportNode = new FinalizedReportNode(attribute, null, order,agg);
            else
                newReportNode = new FinalizedReportNode(attribute, null, order, AggregateFunctions.NONE);
        } else
            if (agg != null)
                newReportNode = new FinalizedReportNode(attribute, null, null, agg);
            else
                newReportNode = new FinalizedReportNode(attribute, null,null, AggregateFunctions.NONE );

        return newReportNode;
    }

    @SuppressWarnings("unchecked")
    public QueryInternalNode generateInternalNode(Element queryRoot,
            String template) {

        Element andRoot = queryRoot.getChild("AND");
        Element orRoot = queryRoot.getChild("OR");
        List<QueryInternalNode> nestedBooleans = new ArrayList<QueryInternalNode>();

        if (andRoot != null && orRoot != null) {
            throw new RuntimeException(
            "Both an AND and OR are present in this query.");
        } else if (andRoot != null) {
            if (andRoot.getChild("AND") != null) {
                nestedBooleans.add(generateInternalNode(andRoot, template));
            }
            if (andRoot.getChild("OR") != null) {
                nestedBooleans.add(generateInternalNode(andRoot, template));
            }
            QueryInternalNode toReturn = processBooleanList(
                    queryRoot.getChild("AND").getChildren("RESTRICTION"),
                    queryFunction("AND"), template);
            Collection<QueryTreeNode> children = toReturn.getChildren();
            children.addAll(nestedBooleans);
            toReturn.setChildren(children);
            return toReturn;
        } else if (orRoot != null) {
            if (orRoot.getChild("AND") != null) {
                nestedBooleans.add(generateInternalNode(orRoot, template));
            }
            if (orRoot.getChild("OR") != null) {
                nestedBooleans.add(generateInternalNode(orRoot, template));
            }
            QueryInternalNode toReturn = processBooleanList(
                    queryRoot.getChild("OR").getChildren("RESTRICTION"),
                    queryFunction("OR"), template);
            Collection<QueryTreeNode> children = toReturn.getChildren();
            children.addAll(nestedBooleans);
            toReturn.setChildren(children);
            return toReturn;
        } else {// no filters, this is represented in the GHQ by a null for the
            // condition tree, so unchecked return
            throw new RuntimeException("No Filters present.");
        }
    }

    // processes a list of restrictions, builds the tree.
    public QueryInternalNode processBooleanList(List<Element> boolList,
            Function funct, String template) {

        if (boolList.size() > 1) {
            Collection<QueryTreeNode> processedNodes = new ArrayList<QueryTreeNode>(
                    boolList.size());
            for (Element e : boolList) {
                processedNodes.add(processFilter(e, template));
            }
            return new QueryInternalNode(funct, processedNodes);
        } else if (boolList.size() == 1) {
            return processFilter(boolList.get(0), template);
        } else {
            throw new CompilerException(
            "Attempting to create a QueryInternalNode with no Filter instances.");
        }
    }

    public QueryInternalNode processFilter(Element column, String template) {
        String id = (column.getAttributeValue("ID") != null ? column
                .getAttributeValue("ID") : column.getAttributeValue("COL_ID"));
        
        if(id == null)
            throw new RuntimeException("Includes segment in query.");
        QueryTreeNode attr = generateAttribute(id, true, template);
        Function funct;

        if (column.getAttributeValue("OP") != null)
            funct = queryFunction(column.getAttributeValue("OP"));
        else
            funct = queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId());

        if (column.getAttribute("VALUES") != null) {
            Collection<QueryTreeNode> nodeList = new ArrayList<QueryTreeNode>();
            nodeList.add(attr);
            nodeList.addAll(generateConstants(column));

            return new QueryInternalNode(funct, nodeList);
        } else {
            return new QueryInternalNode(funct, new QueryTreeNode[] { attr,
                    new QueryConstant(column.getAttributeValue("DESC")) });
        }
    }

    public String cleanIdString(String id) {

        if (id == null)
            throw new RuntimeException("Null id: ");

        id = id.trim();

        if (id.equals(""))
            throw new RuntimeException("Empty attribute");

        id = id.toLowerCase();

        return id;
    }

    public String replaceIdIfNeeded(String id) {

        if (reverseAltsMapping.containsKey(id))
            id = reverseAltsMapping.get(id);

        if (altsMapping.containsKey(id))
            id = altsMapping.get(id);

        if (colIdToAttrMapping.containsKey(id.replace(".", "_")))
            id = id.replace(".", "_");

        return id;

    }

    public QueryTreeNode generateAttribute(String id, boolean isFilter,
            String template) {

      //Handle sysdate 
        if(isConstant(id) || id.toUpperCase().equals("SYSDATE"))
            return new QueryConstant(id);
        
        id = cleanIdString(id);
        id = replaceIdIfNeeded(id);
         
        if (colIdToAttrMapping.containsKey(id)) {
            long attrId = colIdToAttrMapping.get(id);
            if (queryAttribute(attrId) == null)
                throw new RuntimeException("Attribute ID: " + attrId
                        + " in mapping but not in metadata");
            return new QueryAttribute(queryAttribute(attrId));
        } else if (groupToColMapping.containsKey(id)) {
            int counter = 0;
            List<String> groupList = groupToColMapping.get(id);

            while (counter < groupList.size()
                    && !colIdToAttrMapping.containsKey(groupList.get(counter))) {
                counter++;
            }
            if (counter < groupList.size()) {
                long attrId = colIdToAttrMapping.get(groupList.get(counter));
                return new QueryAttribute(queryAttribute(attrId));
            } else {
                throw new RuntimeException(
                        "No Attribute Matches this Column ID: " + id);
            }
        } else if (aggrColUse.containsKey(id)) {
            return interpretCompoundCols(aggrColUse.get(id), isFilter, template);
        } else if (groupToGroupMapping.containsKey(id)) {
            while (!groupToColMapping.containsKey(id)) {
                id = groupToGroupMapping.get(id).get(0);
            }
            if (colIdToAttrMapping
                    .containsKey(groupToColMapping.get(id).get(0))) {
                long attrId = colIdToAttrMapping.get(id);
                return new QueryAttribute(queryAttribute(attrId));
            } else {
                throw new RuntimeException(
                        "No Attribute Matches this Column ID: " + id);
            }
        } else if (id.equals("date")) {
            throw new RuntimeException("Date attribute");

        } else {

            QueryAttribute date = new QueryAttribute(queryAttribute(14)); // TODO:should
            // not
            // be
            // hardcoded.

            if (isFilter && id != null) {

                QueryInternalNode rundate = new QueryInternalNode(
                        queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()),
                        new ArrayList<QueryTreeNode>());

                // If relative date is yesterday
                if (id.contains("yesterday")) {
                    // GHQ to create:
                    // GHQ={"children":[{"attributeId":14},{"children":[],"functionId":31}],"functionId":3}
                    // Equivalent SQL: SQL=(d_ord.ACTIVITY_DAY = to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ))

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, rundate });
                }
                // if relative date is last n days, n=7
                else if ((id.contains("last_") && id.contains("_days"))
                        || (id.contains("previous_") && id.contains("_days"))) {
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[],"functionId":31},{"constant":"6"}],"functionId":12},{"children":[],"functionId":31}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN (to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ) - 6) AND to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ))

                    String constant = id.split("_")[1];
                    QueryConstant n = new QueryConstant(constant);
                    QueryTreeNode interm = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, n });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, interm, rundate });

                }
                // if relative date is last calendar week
                else if (id.contains("last_") && id.contains("week")) {
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'D'"}],"functionId":33},{"constant":"7"}],"functionId":12},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'D'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN (TRUNC(to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'D') - 7) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'D') - 1))

                    QueryConstant D = new QueryConstant("'D'");
                    QueryConstant seven = new QueryConstant("7");
                    QueryConstant one = new QueryConstant("1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, D });

                    QueryInternalNode minus7 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, seven });

                    QueryInternalNode minus1 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, one });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minus7, minus1 });

                }
                // if relative date is last calendar week
                else if (id.contains("_weeks") && id.contains("last_")) {
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'D'"}],"functionId":33},{"constant":"7"}],"functionId":12},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'D'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN (TRUNC(to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'D') - 7) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'D') - 1))

                    String constant = id.split("_")[1];
                    QueryConstant n = new QueryConstant(
                            (Integer.parseInt(constant) * 7) + "");

                    QueryConstant D = new QueryConstant("'D'");
                    QueryConstant one = new QueryConstant("1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, D });

                    QueryInternalNode minusN = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, n });

                    QueryInternalNode minus1 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, one });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minusN, minus1 });

                }
                // runs between beginning of this week unitl now.
                else if (id.contains("this_week_to_date")) {// TODO: Ensure
                    // accuracy
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[],"functionId":31},{"constant":"6"}],"functionId":12},{"children":[],"functionId":31}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN (to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ) - 6) AND to_date (
                    // '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ))

                    QueryConstant D = new QueryConstant("'D'");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, D });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, trunc, rundate });

                }
                // if relative date is last calendar month
                else if (id.contains("last_month")) {
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'MM'"}],"functionId":33},{"constant":"-1"}],"functionId":34},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'MM'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN ADD_MONTHS(TRUNC(to_date
                    // ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'MM'), -1) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'MM') - 1))

                    QueryConstant MM = new QueryConstant("'MM'");
                    QueryConstant negOne = new QueryConstant("-1");
                    QueryConstant one = new QueryConstant("1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, MM });

                    QueryInternalNode minusMonth = new QueryInternalNode(
                            queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, negOne });

                    QueryInternalNode minus1 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, one });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minusMonth, minus1 });

                }
                // runs between beginning of this month and now.
                else if (id.contains("this_month_to_date")) {//      
                    // accuracy
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'MM'"}],"functionId":33},{"constant":"-1"}],"functionId":34},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'MM'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN ADD_MONTHS(TRUNC(to_date
                    // ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'MM'), -1) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'MM') - 1))

                    QueryConstant MM = new QueryConstant("'MM'");
                    QueryConstant negOne = new QueryConstant("-1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, MM });

                    QueryInternalNode minusMonth = new QueryInternalNode(
                            queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, negOne });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minusMonth, rundate });

                }
                /*// if relative date is last calendar week
                else if (id.contains("_months") && id.contains("last_")) {
                    
                    String constant = id.split("_")[1];
                    QueryConstant n = new QueryConstant("-" +
                            (Integer.parseInt(constant)) + "");

                    QueryConstant MM = new QueryConstant("'MM'");
                    QueryConstant one = new QueryConstant("1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID()),
                            new QueryTreeNode[] { rundate, MM });

                    QueryInternalNode minusN = new QueryInternalNode(
                            queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID()),
                            new QueryTreeNode[] { trunc, n });

                    QueryInternalNode minus1 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID()),
                            new QueryTreeNode[] { trunc, one });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID()),
                            new QueryTreeNode[] { date, minusN, minus1 });

                }*/
                // if relative date is last calendar quarter
                else if (id.contains("last_quarter")) {
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'Q'"}],"functionId":33},{"constant":"-3"}],"functionId":34},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'Q'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN ADD_MONTHS(TRUNC(to_date
                    // ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'Q'), -3) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'Q') - 1))

                    QueryConstant Q = new QueryConstant("'Q'");
                    QueryConstant negThree = new QueryConstant("-3");
                    QueryConstant one = new QueryConstant("1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, Q });

                    QueryInternalNode minusMonth = new QueryInternalNode(
                            queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, negThree });

                    QueryInternalNode minus1 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, one });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minusMonth, minus1 });

                }
                // if relative date is last calendar year
                else if (id.contains("last_year")) {
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'YYYY'"}],"functionId":33},{"constant":"-12"}],"functionId":34},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'YYYY'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN ADD_MONTHS(TRUNC(to_date
                    // ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'YYYY'), -12) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'YYYY') - 1))

                    QueryConstant YYYY = new QueryConstant("'YYYY'");
                    QueryConstant negTwelve = new QueryConstant("-12");
                    QueryConstant one = new QueryConstant("1");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, YYYY });

                    QueryInternalNode minusMonth = new QueryInternalNode(
                            queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, negTwelve });

                    QueryInternalNode minus1 = new QueryInternalNode(
                            queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, one });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minusMonth, minus1 });
                } else if (id.contains("this_year_to_date")) {// TODO: Ensure
                    // accuracy
                    // GHQ={"children":[{"attributeId":14},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'YYYY'"}],"functionId":33},{"constant":"-12"}],"functionId":34},{"children":[{"children":[{"children":[],"functionId":31},{"constant":"'YYYY'"}],"functionId":33},{"constant":"1"}],"functionId":12}],"functionId":9}
                    // SQL=(d_ord.ACTIVITY_DAY BETWEEN ADD_MONTHS(TRUNC(to_date
                    // ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ), 'YYYY'), -12) AND
                    // (TRUNC(to_date ( '{RUN_DATE_YYYYMMDD}', 'YYYYMMDD' ),
                    // 'YYYY') - 1))

                    QueryConstant YYYY = new QueryConstant("'YYYY'");
                    QueryConstant negTwelve = new QueryConstant("-12");

                    QueryInternalNode trunc = new QueryInternalNode(
                            queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { rundate, YYYY });

                    QueryInternalNode minusMonth = new QueryInternalNode(
                            queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { trunc, negTwelve });

                    return new QueryInternalNode(
                            queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()),
                            new QueryTreeNode[] { date, minusMonth, rundate });
                }
            } else {
                if (id != null) {
                    String[] tableNames = new String[] { "mp_asin_", "wma_",
                    "vsc_" };

                    for (String s : tableNames) {
                        if (id.contains(s)) {
                            id = id.replace(s, "");

                            if (colIdToAttrMapping.containsKey(id)) {
                                long attrId = colIdToAttrMapping.get(id);
                                return new QueryAttribute(
                                        queryAttribute(attrId));
                            }
                        }
                    }

                    String[] tableAppends = new String[] { "mp_asin_", "_ca",
                            "mp_asin_", "" };
                    for (int i = 0; i < tableAppends.length - 1; i = i + 2) {
                        String test_id = tableAppends[i] + id
                        + tableAppends[i + 1];

                        if (colIdToAttrMapping.containsKey(test_id)) {
                            long attrId = colIdToAttrMapping.get(id);
                            if (queryAttribute(attrId) == null)
                                throw new RuntimeException("Attribute ID: "
                                        + attrId
                                        + " in mapping but not in metadata");

                            return new QueryAttribute(queryAttribute(attrId));
                        }
                    }
                }

            }
        }

        // Could be a constant:
        
        

        throw new RuntimeException("Unknown Attribute " + "<" + id + ">");
    }

    public Collection<QueryTreeNode> generateConstants(Element column) {
        Collection<QueryTreeNode> constantList = new ArrayList<QueryTreeNode>();
        String[] values = column.getAttributeValue("VALUES").split(",");
        for (String value : values) {
            constantList.add(new QueryConstant(value));
        }
        return constantList;
    }

    public AggregateFunctions findIfAggregate(String funct, String operand) {

        for (AggregateFunctions agg : AggregateFunctions.values()) {
            if (funct.toUpperCase().contains(agg.getDescription())) {

                if (operand.toUpperCase().contains("DISTINCT "))
                    return AggregateFunctions.COUNT_DISTINCT;
                else
                    return agg;
            }
        }
        return null;
    }

    public String hangingParens(String arg) {

        if (arg.contains("(") && !arg.contains(")"))
            arg = arg.replace("(", "");

        else if (arg.contains(")") && !arg.contains("("))
           arg =  arg.replace(")", "");

        return arg;
    }

    public List<String> generateDissectedString(String exp) {

        List<String> toReturn = new ArrayList<String>();

        exp = exp.trim(); // avoid trailing information exception if it's just
        // trailing whitespace.
        
        if(isConstant(exp)){
            toReturn.add(exp);
            return toReturn;
        }
        
        if(exp.equals("(to_char(date,'YYYY') || '-' || 'Q' || to_char(date,'Q'))")){
            toReturn.add("to_char(date,'YYYY') ||");
            toReturn.add("'-' || ('Q' || to_char(date,'Q'))");
            return toReturn;
        }
        else if(exp.equals(" '-' ||( 'Q' || to_char(date,'Q'))")){
            toReturn.add("'-' ||");
            toReturn.add("'Q' || to_char(date,'Q')");
        }
        else if(exp.equals("'Q' || to_char(date,'Q')")){
            toReturn.add("'Q' ||");
            toReturn.add("to_char(date,'Q')");
        }
        
        //Handles the CASE, well, case. 
        if ((exp.contains("case") || exp.contains("CASE")) &&
                (exp.contains("when") || exp.contains("WHEN")) &&
                (exp.contains("then") || exp.contains("THEN")) &&
                (exp.contains("end") || (exp.contains("END")))) {
            // short circuit this for now.
            toReturn.add("CASE");
            
            String [] whens = null;
            
            if(exp.contains("when")){
                whens = exp.split("when");
            }
            else if(exp.contains("WHEN")){
                whens = exp.split("WHEN");
            }
            else
                whens = new String[0];
            
            for(int i=1; i < whens.length; i++){
                String [] thens = null;
                if(whens[i].contains("THEN")){
                    thens = whens[i].split("THEN");
                }
                else if(whens[i].contains("then")){
                    thens = whens[i].split("then");          
                }
                else
                    thens = new String[0];
                
                for(String s: thens){
                    if(s.contains("END"))
                        s = s.replace("END", "");
                    if(s.contains("end"))
                        s = s.replace("end", "");
                    
                    if(s.contains("ELSE")){
                        for(String newS: s.split("ELSE"))
                            toReturn.add(newS);
                    }
                    else if(s.contains("else")){
                        for(String newS: s.split("else"))
                            toReturn.add(newS);
                    }
                    else{
                        toReturn.add(s);
                    }
                }
            }   
            return toReturn;          
        }

        if(exp.contains(" IN ") || exp.contains(" in ")){
            String [] terms = exp.replace(" in ", " IN ").split(" IN ");
            toReturn.add("IN");
            toReturn.add(terms[0]); 
            String [] args = terms[1].replace("(", "").replace(")", "").split(",");
            for(String arg: args)
                toReturn.add(arg);
            return toReturn;
        }
        
        if(exp.contains(" AND ") || exp.contains(" and ")){
            if(exp.contains(" BETWEEN ") || exp.contains(" between ")){
                String betweenRem = exp.replace(" between ", " BETWEEN ");
                String [] terms = betweenRem.split("BETWEEN");
                toReturn.add("BETWEEN");
                toReturn.add(terms[0]);
                String [] andClauses = terms[1].replace(" and ", " AND ").split(" AND ");
                for(String and: andClauses)
                    toReturn.add(and);
            }
            else{//not a between
                String [] terms = exp.replace(" and ", " AND ").split(" AND ");
                toReturn.add(terms[0]); 
                toReturn.add("AND"); 
                toReturn.add(terms[1]);
            }
            return toReturn;
        }

        if (exp.contains("(") && exp.contains(")")) {

            // break off the first and last parens, process the word that comes
            // before that, act accordingly.
            String function = exp.substring(0, exp.indexOf("("));

           if (exp.lastIndexOf("(") < exp.indexOf(")")
                    && !containsFunction(function)) {

                String operand = exp.substring(exp.indexOf("(") + 1,
                        exp.lastIndexOf(")"));

                if (function.length() > 1) {// Handles exp wrapped in parens

                    if (operand.toUpperCase().contains("DISTINCT ")) {
                        function = "COUNT_DISTINCT";
                        operand = operand.replace("distinct ", "");
                        operand = operand.replace("DISTINCT ", "");
                    }
                    toReturn.addAll(generateDissectedString(function));

                    if (operand.contains(",")) {
                        if (!operand.contains("(") && !operand.contains(")")) {
                            String[] operands = operand.split(",");

                            for (int i = 0; i < operands.length; i++){
                                    toReturn.add(operands[i]);
                            }
                            return toReturn;
                        } else {
                            int openParensSeen = 0;
                            int lastComma = -1;
                            for (int i = 0; i < operand.length(); i++) {
                                // iterate through string &
                                // generate substrings manually
                                char currChar = operand.charAt(i);

                                if (currChar == '(')
                                    openParensSeen++;

                                if (currChar == ')')
                                    openParensSeen--;

                                if (currChar == ',' && openParensSeen == 0) {
                                    // adds entire value between outermost list
                                    // of commas
                                    toReturn.add(operand.substring(
                                            lastComma + 1, i));
                                    lastComma = i;
                                }

                            }
                            toReturn.add(operand.substring(lastComma + 1,
                                    operand.length() - 1));
                            return toReturn;
                        }

                    }
                }
                if(toReturn.isEmpty())
                    toReturn.addAll(generateDissectedString(operand));
                else
                    toReturn.add(operand);
                
                return toReturn;
            } else {
                                
                if(exp.indexOf("(") == 0 && exp.lastIndexOf(")") == exp.length() -1){
                    exp = exp.substring(exp.indexOf("(") + 1,
                            exp.lastIndexOf(")"));
                }
                    
                String[] arthFuncts = new String[] { "*", "+", "-", "/", "IN", "<", ">", "||", "AND", "="};
                for (String funct : arthFuncts) {
                    if (exp.contains(funct)) {

                        int index = exp.indexOf(funct);
                        String firstArg = null;
                        String secondArg = null;
                        String operator = null;
                        
                        if(funct.equals("IN") || funct.equals("||")){
                            toReturn.add(hangingParens(exp.substring(0, index)));
                            toReturn.add(exp.substring(index, index + 2));
                            toReturn.add(hangingParens(exp.substring(index + 2)));

                            return toReturn;
                        }
                        else if(funct.equals("AND")){
                            firstArg = hangingParens(exp.substring(0, index));
                            secondArg = hangingParens(exp
                                .substring(index + 3));
                            operator = exp.substring(index, index + 3);
                        }
                        else{
                            firstArg = hangingParens(exp.substring(0, index));
                            secondArg = hangingParens(exp
                                .substring(index + 1));
                            operator = "" + exp.charAt(index);  
                        }

                        toReturn.add(firstArg);
                        toReturn.add(operator);
                        toReturn.add(secondArg);

                        return toReturn;
                    }

                }

                toReturn.add(exp);
                return toReturn;
            }
        }

        if(exp.contains(",")){
            for(String e: exp.split(",")){
                toReturn.add(e);
            }
            return toReturn;
        }
        
        String[] arthFuncts = new String[] { "-", "+", "*", "/", "<", ">", "="};
        for (String funct : arthFuncts) {
            if (exp.contains(funct)) {

                int index = exp.indexOf(funct);
                String firstArg = hangingParens(exp.substring(0, index));
                String secondArg = hangingParens(exp.substring(index + 1));

                toReturn.add(firstArg);
                toReturn.add("" + exp.charAt(index));
                toReturn.add(secondArg);

                return toReturn;
            }
        }

        toReturn.add(exp);
        return toReturn;
    }

    public boolean isConstant(String test) {
        
       String test2 = test.trim();
        
        if(test.equals(""))
            throw new RuntimeException("Nothing in there: " + test + ", see?");
        
        if(test2.charAt(0) == '\'' && test2.charAt(test2.length()-1) == '\'')
            return true;
        try {
            Integer.parseInt(test2);
            return true;
        } catch (NumberFormatException ne) {
            if(test2.contains("'"))
                return true;
            return false;
        }
    }

    // processes those columns with functions in their expr tags
    public QueryTreeNode interpretCompoundCols(String exp, boolean isFilter,
            String template) {

        if (isSpecialCase(exp)){
            return handleSpecialCase(exp, template);
        }
        
        List<String> stringPieces = generateDissectedString(exp);

        // As aggregate functions should have been handled in the generation of
        // FinalizedReportNode,
        // and all evidence of them stripped before this is reached, we can
        // ignore this case for the moment
        if (stringPieces.size() == 2) { // so a function(operand) case
            String givenFunction = stringPieces.get(0).toUpperCase();
            String[] givenOperands = stringPieces.get(1).split(","); // if no
            // ,s, then
            // will
            // just be
            // a length
            // 1 array

            // find out which function
            for (Function funct : getAllFunctions()) {
                if (funct.getDescription().contains(givenFunction)) {
                    ArrayList<QueryTreeNode> operandsList = new ArrayList<QueryTreeNode>();

                    for (String op : givenOperands) {

                        if (containsFunction(op)) {
                            operandsList.add(interpretCompoundCols(op,
                                    isFilter, template));
                        } else {

                            if (isConstant(op)) {
                                operandsList.add(new QueryConstant(op));
                            } else if (isFilter)
                                operandsList.add(generateAttribute(op.trim(),
                                        true, template));
                            else
                                operandsList.add(generateAttribute(op.trim(),
                                        false, template));
                        }
                    }
                    QueryTreeNode[] opsList = new QueryTreeNode[operandsList
                                                                .size()];
                    for (int i = 0; i < operandsList.size(); i++) {
                        opsList[i] = operandsList.get(i);
                    }

                    return new QueryInternalNode(funct, opsList);
                }
            }
            throw new RuntimeException(
                    "Function not found in function(operand) case: "
                    + givenFunction);
        } else if (stringPieces.size() == 3) { // so a term * term case (where *
            // can be replaced with +,-,/
            QueryTreeNode term1, term2 = null;

            for (Function testF : getAllFunctions()) {
                String functString = testF.getFunctionString();
                if (functString != null
                        && functString.contains(stringPieces.get(1))) {

                    if (containsFunction(stringPieces.get(0))) {
                        term1 = interpretCompoundCols(stringPieces.get(0),
                                isFilter, template);
                    } else {
                        term1 = generateAttribute(stringPieces.get(0),
                                isFilter, template);
                    }

                    if (containsFunction(stringPieces.get(2))) {
                        term2 = interpretCompoundCols(stringPieces.get(2),
                                isFilter, template);
                    } else {
                        term2 = generateAttribute(stringPieces.get(2),
                                isFilter, template);
                    }

                    return new QueryInternalNode(testF, new QueryTreeNode[] {
                            term1, term2 });
                }
            } // see if it's of the form fxn(op1, op2)
            for (Function testF : getAllFunctions()) {
                String functString = testF.getFunctionString();
                String testS = stringPieces.get(0).toUpperCase();
                if ((functString != null
                        && functString.contains(testS)) ||
                        testF.getDescription().contains(testS)) {

                    if (containsFunction(stringPieces.get(1))) {
                        term1 = interpretCompoundCols(stringPieces.get(1),
                                isFilter, template);
                    } else {
                        term1 = generateAttribute(stringPieces.get(1),
                                isFilter, template);
                    }

                    if (containsFunction(stringPieces.get(2))) {
                        term2 = interpretCompoundCols(stringPieces.get(2),
                                isFilter, template);
                    } else {
                        term2 = generateAttribute(stringPieces.get(2),
                                isFilter, template);
                    }

                    return new QueryInternalNode(testF, new QueryTreeNode[] {
                            term1, term2 });
                }
            }
            throw new RuntimeException("No function split found: " + exp + " "
                    + stringPieces.get(2));
        } else if (stringPieces.size() == 1) {
            return generateAttribute(stringPieces.get(0), isFilter, template);
            
        } else {// x arguments
            String givenFunction = stringPieces.get(0).toUpperCase()
            .replace("'", "");

            // find out which function
            for (Function funct : getAllFunctions()) {
                if (funct.getDescription().contains(givenFunction)) {
                    List<QueryTreeNode> operandsList = new ArrayList<QueryTreeNode>();
                    
                    for (String op : stringPieces) {

                        if (op.toUpperCase().replace("'", "")
                                .equals(givenFunction))
                            continue;

                        if (containsFunction(op)) {
                            operandsList.add(interpretCompoundCols(op,
                                    isFilter, template));
                        } else {
                            if (isFilter)
                                operandsList.add(generateAttribute(op.trim(),
                                        true, template));
                            else
                                operandsList.add(generateAttribute(op.trim(),
                                        false, template));
                        }
                    }
                    QueryTreeNode[] opsList = new QueryTreeNode[operandsList
                                                                .size()];
                    for (int i = 0; i < operandsList.size(); i++) {
                        opsList[i] = operandsList.get(i);
                    }

                    return new QueryInternalNode(funct,opsList);
                }
            }
            
            throw new RuntimeException(
                    "Function not found in function(operand, operand) case: "
                    + givenFunction);
        }
    }

    public boolean containsFunction(String exp) {

        try{
            Integer.parseInt(exp.trim());
        }
        catch(NumberFormatException ne)
        {
            if(exp.contains("'-'"))
                return false; 

            if ((exp.contains("(") && exp.contains(")")) || exp.contains("+")
                    || exp.contains("-") || exp.contains("*") || exp.contains("/")
                    || exp.toUpperCase().contains("CASE")
                    || exp.toUpperCase().contains("DISTINCT ")
                    || exp.toUpperCase().contains("IN ")
                    || exp.toUpperCase().contains("<")
                    || exp.toUpperCase().contains("=")) {
                return true;
            }
        }
        return false;

    }

    // --Mapping for DSSUI functions
    public Function queryFunction(String desc) {
        return DSSUI_OP.getFunction(desc, this);
    }

    public enum DSSUI_OP {// mapping from function names found in DSSUI to GH
        // Function IDs.
        NONE("None", -1),
        AND("AND", 0),
        OR("OR", 1),
        NOT("NOT", 2),
        EQUALS("EQ", 3),
        LESS_THAN("LT", 5),
        LESS_THAN_OR_EQUAL_TO("LTE", 6),
        GREATER_THAN("GT", 7),
        GREATER_THAN_OR_EQUAL_TO("GTE", 8),
        BETWEEN("RANGE", 9),
        PLUS("PLUS", 11),
        MINUS("MINUS", 12),
        TRUNC("TRUNC", 21),
        IN("IN", 30),
        RUNDATE("RUNDATE", 31),
        TRUNC_DATE("TRUNC_DATE", 33),
        ADD_MONTHS("ADD_MONTHS", 34),
        SIGN("SIGN", 43),
        LIKE_BEGINS_WITH("LIKE_BEGINS_WITH", 47);

        private String description;
        private long functionId;

        DSSUI_OP(String desc, int id) {
            description = desc;
            functionId = id;
        }

        public String getDescription() {
            return description;
        }

        public long getFunctionID() {
            return functionId;
        }

        public static Function getFunction(String desc, GHQFactory factory) {

            for (DSSUI_OP op : DSSUI_OP.values()) {
                if (op.description.equals(desc))
                    return factory.queryFunction(op.getFunctionID(), DBType.SQL.getId());
            }

            throw new RuntimeException("No matching GH Function found." + desc);
        }

    }

    @SuppressWarnings("unchecked")
    public HashMap<String, Long> retrieveMapping() {

        HashMap<String, Long> toReturn = new HashMap<String, Long>();

        SAXBuilder builder = new SAXBuilder();
        File toParse = new File(COL_ID_TO_ATTR_ID_MAPPING_FILENAME);
        Document document;
        try {
            document = (Document) builder.build(toParse);

            Element root = document.getRootElement();

            for (Element e : (List<Element>) root.getChildren()) {
                toReturn.put(e.getAttributeValue("dssid"),
                        Long.parseLong(e.getAttributeValue("attrid")));
            }
            return toReturn;
        } catch (JDOMException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        throw new RuntimeException("Failed to create mapping");
    }

    @SuppressWarnings("unchecked")
    public void generateGroupedMapping() {

        HashMap<String, List<String>> mappingGroupToColInst = new HashMap<String, List<String>>();
        HashMap<String, List<String>> mappingGroupToGroup = new HashMap<String, List<String>>();

        try {
            FileOutputStream outCols = new FileOutputStream(
                    GROUP_TO_COL_MAPPING_FILENAME);
            FileOutputStream outGroups = new FileOutputStream(
                    GROUP_TO_GROUP_MAPPING_FILENAME);

            ObjectOutputStream outObjCols = new ObjectOutputStream(outCols);
            ObjectOutputStream outObjGroups = new ObjectOutputStream(outGroups);

            String[] groups = { "COLUMN", "RESTRICTION" };

            SAXBuilder builder = new SAXBuilder();
            File toParse = new File(SOURCE);
            Document document = (Document) builder.build(toParse);
            // two passes,
            for (String g : groups) {
                for (Object colGroup : document.getRootElement().getChildren(
                        g + "_GROUP")) {
                    Element group = (Element) colGroup;
                    String id = group.getAttributeValue("ID");
                    String relatedGroups = group.getAttributeValue(g
                            + "_GROUPS");
                    String actCols = group.getAttributeValue("COLUMNS");

                    if (relatedGroups != null) {
                        mappingGroupToGroup.put(id,
                                Arrays.asList(relatedGroups.split(",")));
                    }
                    if (!actCols.equals("") || actCols != null) {
                        mappingGroupToColInst.put(id,
                                Arrays.asList(actCols.split(",")));
                    }
                }
            }

            outObjCols.writeObject(mappingGroupToColInst);
            outObjGroups.writeObject(mappingGroupToGroup);
            outObjCols.close();
            outObjGroups.close();
            outCols.close();
            outGroups.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JDOMException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, String> generateAltsMapping() {

        HashMap<String, String> altsMapping = new HashMap<String, String>();

        SAXBuilder builder = new SAXBuilder();
        File toParse = new File(SOURCE);
        Document document;
        try {
            document = (Document) builder.build(toParse);
            Element root = document.getRootElement().getChild(
            "COLUMN_ALTERNATES");
            for (Object colAlt : root.getChildren("COLUMN_ALTERNATE")) {
                Element alternate = (Element) colAlt;
                for (String s : alternate.getAttributeValue("COLUMNS").split(
                ",")) {
                    altsMapping.put(alternate.getAttributeValue("ID"), s);
                }
            }
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return altsMapping;
    }

    public HashMap<String, String> generateReverseAltsMapping() {

        HashMap<String, String> altsMapping = new HashMap<String, String>();

        SAXBuilder builder = new SAXBuilder();
        File toParse = new File(SOURCE);
        Document document;
        try {
            document = (Document) builder.build(toParse);
            Element root = document.getRootElement().getChild(
            "COLUMN_ALTERNATES");
            for (Object colAlt : root.getChildren("COLUMN_ALTERNATE")) {
                Element alternate = (Element) colAlt;
                for (String s : alternate.getAttributeValue("COLUMNS").split(
                ",")) {
                    altsMapping.put(s, alternate.getAttributeValue("ID"));
                }
            }
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return altsMapping;
    }

    public HashMap<String, String> findCustomCols() {

        HashMap<String, String> custCols = new HashMap<String, String>();

        SAXBuilder builder = new SAXBuilder();
        File toParse = new File(SOURCE);
        Document document;
        try {
            document = (Document) builder.build(toParse);
            Element root = document.getRootElement().getChild("COLUMNS");

            for (Object col : root.getChildren("COLUMN")) {
                Element column = (Element) col;
                if (column.getAttributeValue("EXPR_ORACLE") != null) {
                    custCols.put(column.getAttributeValue("ID"),
                            column.getAttributeValue("EXPR_ORACLE"));
                } else if (column.getAttributeValue("EXPR") != null
                        && ((column.getAttributeValue("EXPR").contains("(") && column
                                .getAttributeValue("EXPR").contains(")"))
                                || (column.getAttributeValue("EXPR").indexOf(
                                ".") != column
                                .getAttributeValue("EXPR").lastIndexOf(
                                ".")) || (column
                                        .getAttributeValue("EXPR").contains("*")))) {
                    custCols.put(column.getAttributeValue("ID"),
                            column.getAttributeValue("EXPR"));
                }
            }

            root = document.getRootElement().getChild("RESTRICTIONS");
            for (Object e : root.getChildren("RESTRICTION")) {
                Element res = (Element) e;
                custCols.put(res.getAttributeValue("ID"),
                        res.getAttributeValue("EXPR"));
            }

        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return custCols;
    }
    
}
