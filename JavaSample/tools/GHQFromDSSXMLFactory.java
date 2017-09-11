package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazon.dw.grasshopper.compilerservice.GHQValidation.FunctionTypeValidity;
import com.amazon.dw.grasshopper.compilerservice.GHQValidation.InnerGHQexpressionType;
import com.amazon.dw.grasshopper.compilerservice.ghqfactory.GHQFactory;
import com.amazon.dw.grasshopper.metadata.MetaDataStore;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.Function;
import com.amazon.dw.grasshopper.model.MetaDataExtendedAttributes;
import com.amazon.dw.grasshopper.model.Table;
import com.amazon.dw.grasshopper.model.GHQuery.AggregateFunctions;
import com.amazon.dw.grasshopper.model.GHQuery.AggregatedQueryTreeNode;
import com.amazon.dw.grasshopper.model.GHQuery.AggregatedQueryTreeNodeInternalObject;
import com.amazon.dw.grasshopper.model.GHQuery.AggregatedQueryTreeNodeObject;
import com.amazon.dw.grasshopper.model.GHQuery.FinalizedReportNode;
import com.amazon.dw.grasshopper.model.GHQuery.OrderingDetails;
import com.amazon.dw.grasshopper.model.GHQuery.GHQuery;
import com.amazon.dw.grasshopper.model.GHQuery.QueryAttribute;
import com.amazon.dw.grasshopper.model.GHQuery.QueryConstant;
import com.amazon.dw.grasshopper.model.GHQuery.QueryInternalNode;
import com.amazon.dw.grasshopper.model.GHQuery.QueryTreeNode;


public class GHQFromDSSXMLFactory extends GHQFactory {

    private static final String COL_ID_TO_ATTR_ID_MAPPING_FILENAME = "metadata-content/dssAttributeMap.xml";
    
    private static Logger LOGGER = Logger.getLogger(GHQFromDSSXMLFactory.class);
  
   
    public static class Statistics {
        public int numOfSegments = 0;
        HashMap<String, Long> hashMostMissedAttributes;
        HashMap<String, Long> hashMostMissedDateAttributes;
        
        public Statistics(){
            hashMostMissedAttributes = new HashMap<String, Long>();
            hashMostMissedDateAttributes = new HashMap<String, Long>();
        }
    }
    
    
    public static class CurrentRunData{
        long areaId = -1;
        boolean isPerfectConversion;
        StringBuilder log;
        
        public CurrentRunData(){
            areaId = -1;
            isPerfectConversion = true;
            log = new StringBuilder();
        }
        
        public String getLog(){
            return log.toString();
        }
        
        public boolean isPerfectConversion(){
            return isPerfectConversion;
        }
        
        public void clear(){
            areaId = -1;
            isPerfectConversion = true;
            log = new StringBuilder();
        }
    }
    
    HashMap<String, Long> colIdToAttrMapping;
    HashMap<Long, Long> hashAreaActivityDay;
    
    
    Statistics statistics;
    CurrentRunData currentRunData;
    
    public Statistics getStatistics() {
        return statistics;
    }
    
    public CurrentRunData getCurrentRunData(){
        return currentRunData;
    }

    public static void addCountToHash(HashMap<String, Long> hash, String id){
        if(hash.containsKey(id)==false)
            hash.put(id, Long.valueOf(0));
        Long val = hash.get(id) + 1;
        hash.put(id, val);
    }
    
   

    public GHQFromDSSXMLFactory(MetaDataStore metadata) {
        super(metadata);
        colIdToAttrMapping = retrieveMapping();
        initHashAreaActivityDay();
        statistics = new Statistics();
        currentRunData = new CurrentRunData();
    }

    private void initHashAreaActivityDay(){
        hashAreaActivityDay = new HashMap<Long, Long>();
        hashAreaActivityDay.put(Long.valueOf(1), Long.valueOf(1588));
        hashAreaActivityDay.put(Long.valueOf(3), Long.valueOf(1719));
        hashAreaActivityDay.put(Long.valueOf(4), Long.valueOf(1605));
        hashAreaActivityDay.put(Long.valueOf(5), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(6), Long.valueOf(1596));
        hashAreaActivityDay.put(Long.valueOf(7), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(8), Long.valueOf(1594));
        hashAreaActivityDay.put(Long.valueOf(9), Long.valueOf(1584));
        hashAreaActivityDay.put(Long.valueOf(10), Long.valueOf(1584));
        hashAreaActivityDay.put(Long.valueOf(11), Long.valueOf(1584));
        hashAreaActivityDay.put(Long.valueOf(12), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(13), Long.valueOf(1604));
        hashAreaActivityDay.put(Long.valueOf(14), Long.valueOf(1582));
        hashAreaActivityDay.put(Long.valueOf(15), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(16), Long.valueOf(1601));
        hashAreaActivityDay.put(Long.valueOf(18), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(19), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(20), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(21), Long.valueOf(1600));
        hashAreaActivityDay.put(Long.valueOf(22), Long.valueOf(1599));
        
    }
    
    private void writeToLog(String message){
        LOGGER.info(message);
        currentRunData.log.append(message + "\n");
    }
    

    /**
     * Tries to convert a DSS into a GH query.
     * @param queryRoot
     * @param queryType
     * @param text
     * @return null on failure due to unsupported area-template, otherwise the converted GHQ. 
     */
    public GHQuery generateQuery(Element queryRoot, long queryType, String text) {
        currentRunData.clear();
        GHQuery query = new GHQuery();
        Area area = findArea(queryRoot);
        if(area != null){
            query.setAreaId(area.getId());
            currentRunData.areaId = area.getId();
        }
        else
        {
            String areaStr = "No DESC attribute";
            if(queryRoot.getAttribute("DESC")!=null)
                areaStr = queryRoot.getAttribute("DESC").getValue();
            writeToLog("Unknown area: " +  areaStr);
            return null;
        }
        
        if(currentRunData.areaId == 17){
            if(text.contains("TABLE=\"vsc\"")){
                hashAreaActivityDay.put(Long.valueOf(17), Long.valueOf(1608));
            }
            else
                hashAreaActivityDay.put(Long.valueOf(17), Long.valueOf(1600));
        }
        if(currentRunData.areaId == 2){
            if(text.contains("TABLE=\"dbol\"")){
                hashAreaActivityDay.put(Long.valueOf(2), Long.valueOf(1600));
            }
            else if(text.contains("TABLE=\"dsdpol\""))
                hashAreaActivityDay.put(Long.valueOf(2), Long.valueOf(1585));
            else hashAreaActivityDay.remove(Long.valueOf(2));
        }
        if(currentRunData.areaId == 10){
            if(text.contains("TABLE=\"drf\"")){
                //Daily Refunds Activity Day
                hashAreaActivityDay.put(Long.valueOf(10), Long.valueOf(1589));
            }
            else{
                //Daily Promotion Activity Day
                hashAreaActivityDay.put(Long.valueOf(10), Long.valueOf(1584));
            }                
        }
        
        /*Some loyalty-points templates are actually based on D_DAILY_ORDERS.IS_NYP, which makes them
         * a different area.    */
        if(currentRunData.areaId == 5 && text.indexOf("DESC=\"Loyalty Points\"") > 0
                && text.indexOf("COL_ID=\"is_nyp\"") > 0){
            query.setAreaId(6);
            currentRunData.areaId = 6;
        }

        query.setReportRoots(getReportedColumns(queryRoot));
        query.setConditionQueryTree(getConditionExpression(queryRoot));
        query.setMetaDataVersion("global");
        return query;
    }
    
    
    /**
     * Creates the list of column in report
     * @param queryRoot
     * @return
     */
    private ArrayList<FinalizedReportNode> getReportedColumns(Element queryRoot){
        ArrayList<FinalizedReportNode> reportedNodes = new ArrayList<FinalizedReportNode>();
        HashSet<String> synNames = new HashSet<String>();
        @SuppressWarnings("unchecked")
        List<Element> columns = queryRoot.getChildren("COLUMN");
        int counter = 0;
        for(Element elem : columns){
            FinalizedReportNode column = getReportedNode(elem);
            if(column!=null && elem.getAttribute("DESC")!=null){
                String newSyn = elem.getAttributeValue("DESC");
                if(newSyn.contains("'"))
                    newSyn = newSyn.replace("'", "");
                if(synNames.contains(newSyn))
                    newSyn = newSyn + Integer.toString(counter);
                synNames.add(newSyn);
                column.setSyn(newSyn);
            }
            if(column!=null){
                reportedNodes.add(column);
            }
            counter++;
        }
        //reportedNodes.add(new FinalizedReportNode(new QueryAttribute(queryAttribute(296))));
        return reportedNodes;
    }
    
    /**
     * Creates a single FinalizedReportnode out of a COLUMN.
     * @param columnElem
     * @return
     */
    private FinalizedReportNode getReportedNode(Element columnElem){
        String id = columnElem.getAttributeValue("ID");
        String type = columnElem.getAttributeValue("TYPE");
        AggregateFunctions aggFunction = AggregateFunctions.NONE;
        if(/* columnElem.getAttribute("EXPR")==null && */ columnElem.getAttribute("DIM_TYPE") != null && columnElem.getAttribute("DIM_TYPE").getValue().equalsIgnoreCase("FACT")){
            if(columnElem.getAttribute("TYPE").getValue().equalsIgnoreCase("Numeric") || columnElem.getAttribute("TYPE").getValue().equalsIgnoreCase("Currency"))
            aggFunction = AggregateFunctions.SUM;
        }
        
        OrderingDetails ordering = null;
        if(columnElem.getAttribute("ORDER")!=null)
        {
            try{
                int orderNum = columnElem.getAttribute("ORDER").getIntValue();
                boolean asc = true;
                if(orderNum <0)
                {
                    asc = false;
                    orderNum *= -1;
                }
                
                ordering = new OrderingDetails(asc, orderNum);
            }
            catch(Exception ex){
                writeToLog("Failed to convert ordering: " + columnElem.getAttribute("ORDER").getValue());
            }
        }
                
        if(type.equalsIgnoreCase("DATE") && currentRunData.areaId > -1){
            
            // calculated columns
            QueryTreeNode expression = getCalculatedColumns(id);
            if(expression!=null){
                return new FinalizedReportNode(expression, "", ordering);
            }
            
            //the "Default" date attribute
            if((columnElem.getAttribute("TABLE")==null || columnElem.getAttribute("TABLE").getValue().equalsIgnoreCase("rday")) && hashAreaActivityDay.containsKey(Long.valueOf(currentRunData.areaId))){
                addCountToHash(statistics.hashMostMissedDateAttributes, id);
                
                long attrId = hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue();
                return new FinalizedReportNode(new QueryAttribute(queryAttribute(attrId)), "", ordering);
            }
            else if(colIdToAttrMapping.containsKey(id)){//non-default date attributes
                long attrId = colIdToAttrMapping.get(id).longValue();
                if(queryAttribute(attrId)!=null)
                    return new FinalizedReportNode(new QueryAttribute(queryAttribute(attrId)),"", ordering);
                else{
                    writeToLog("Attribute " + attrId + " doesn't exist in metadata, but exists in dssMapping,");
                    return null;
                }
            }
            else{
                addCountToHash(statistics.hashMostMissedDateAttributes, id);
                writeToLog("Date mapping not found! - " + currentRunData.areaId);
                return null;
            }
                
        }
        else{
            FinalizedReportNode aggNode = getAggregatedCalculatedColumns(id);
            if(aggNode!=null){
                aggNode.setOrdering(ordering);
                return aggNode;
            }
                
            
            if(colIdToAttrMapping.containsKey(id)){
                long attrId = colIdToAttrMapping.get(id).longValue();
                
                if(queryAttribute(attrId)!=null){
                    QueryTreeNode customColumn = getCustomColumn(queryAttribute(attrId));
                    if(customColumn!=null)
                        return new FinalizedReportNode(createAggregatedQueryTreeNodeObject(customColumn, aggFunction),"", ordering);
                    else
                        return new FinalizedReportNode(new QueryAttribute(queryAttribute(attrId)), "", ordering, aggFunction);    
                }
                    
                writeToLog("attribute not found-" + id + " ("+attrId+")" );
                return null;
            }
            else{
                QueryTreeNode node = getCalculatedColumns(id);
                if(node==null){
                    addMissedAttribute(id);
                    writeToLog("attribute with DSS-ID (" + id + ") not found." );
                }
                else{
                    return new FinalizedReportNode(createAggregatedQueryTreeNodeObject(node,aggFunction), "", ordering);
                }
                 
            }
        }
        return null;
    }
    
    //a hack to copy date expression date from the QueryInternalNode level, to the AggregateQueryTreeNodeObject level
    private AggregatedQueryTreeNodeObject createAggregatedQueryTreeNodeObject(QueryTreeNode node, AggregateFunctions agg){
        AggregatedQueryTreeNodeObject retval = new AggregatedQueryTreeNodeObject(node, agg);
        if(node.getClass() == QueryInternalNode.class){
            QueryInternalNode qin = (QueryInternalNode)node;
            String dateExpName = qin.getAdditionalAttribute("dateExpName");
            String dateAttrId = qin.getAdditionalAttribute("dateAttrId");
            if(dateExpName != null && dateAttrId != null){
                retval.addAdditionalAttribute("dateExpName", dateExpName);
                retval.addAdditionalAttribute("dateAttrId", dateAttrId);
            }
        }
        return retval;
    }
    
    /**
     * Adds a record to the counting-collection of missing dss-attributes, so we can
     * see which missing dss-attributes are most-used.
     * @param dssIDName
     */
    private void addMissedAttribute(String dssIDName){
        currentRunData.isPerfectConversion = false;
        addCountToHash(statistics.hashMostMissedAttributes, dssIDName);
    }
    
    /**
     * Translates dummy-attributes to expressions (attributes that originally appear in rday table)
     * @param attr
     * @return
     */
    private QueryTreeNode getCustomColumn(Attribute attr){
        if(attr.getId()==772){ // rday_reporting_week_of_year
            Attribute actualAttr = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(
                queryFunction(26, DBType.SQL.getId()), // TO_CHAR
                new QueryTreeNode[] {
                    new QueryInternalNode(
                        queryFunction(11, DBType.SQL.getId()), // PLUS
                        new QueryTreeNode[] {
                            new QueryAttribute(actualAttr),
                            new QueryConstant("1")
                        }
                    ),
                    new QueryConstant("'IW'")
                }
            );
            date.addAdditionalAttribute("dateExpName", "Reporting Week");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==768){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'MM'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Day of Week");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==769){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'Q'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Quarter");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==771){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'YYYY'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Year");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==774){ // rday_reporting_year
            Attribute actualAttr = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(
                queryFunction(26, DBType.SQL.getId()), // TO_CHAR
                new QueryTreeNode[] {
                    new QueryInternalNode(
                        queryFunction(11, DBType.SQL.getId()), // PLUS
                        new QueryTreeNode[] {
                            new QueryAttribute(actualAttr),
                            new QueryConstant("1")
                        }
                    ),
                    new QueryConstant("'IYYY'")
                }
            );
            date.addAdditionalAttribute("dateExpName", "Reporting Year");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==770){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'WW'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Week Of Year");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==766){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'D'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Day of Week");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==767){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'DDD'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Day of Year");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        if(attr.getId()==765){
            Attribute actualAttr  = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue());
            QueryInternalNode date = new QueryInternalNode(queryFunction(26, DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(actualAttr), new QueryConstant("'DD'")});
            date.addAdditionalAttribute("dateExpName", "Calendar Day of Month");
            date.addAdditionalAttribute("dateAttrId", Long.toString(actualAttr.getId()));
            return date;
        }
        else
            return null;
    }
    
    
    private FinalizedReportNode getAggregatedCalculatedColumns(String id){
        //TODO: refactor this code!!
        if(id.equalsIgnoreCase("cp_per_unit")){
            /*<COLUMN ID="cp_per_unit" DESC="CP Per unit(CP Amount/Quantity Shipped)" TABLE="cp" EXPR="SUM(cp.CONTRIBUTION_PROFIT_AMT)/SUM(cp.QUANTITY_SHIPPED)"
             *  TYPE="Currency" DB_TYPE="integer" DIM_TYPE="FACT"></COLUMN>*/
            AggregatedQueryTreeNodeObject obj1 = new AggregatedQueryTreeNodeObject(new QueryAttribute(queryAttribute(1175)), AggregateFunctions.SUM);
            AggregatedQueryTreeNodeObject obj2 = new AggregatedQueryTreeNodeObject(new QueryAttribute(queryAttribute(1196)), AggregateFunctions.SUM);
            AggregatedQueryTreeNodeInternalObject complex = new AggregatedQueryTreeNodeInternalObject(queryFunction(14, DBType.SQL.getId()), Arrays.asList(new AggregatedQueryTreeNode[] { obj1, obj2 } ));
            complex.addAdditionalAttribute("expressionId", "151");
            complex.addAdditionalAttribute("expressionName", "CP per unit");
            return new FinalizedReportNode(complex, "cp_per_unit", null);
            
        }   
        else if(id.equalsIgnoreCase("d_ord_avg_sales_price")){
            return new FinalizedReportNode(new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord.avg_sales_price").longValue())), AggregateFunctions.AVG); 
            /*<COLUMN ID="d_ord_avg_sales_price" DESC="Price -- Average Sales Price (ASP)" TABLE="d_ord" EXPR="AVG(d_ord.AVG_SALES_PRICE)" TYPE="Double" DB_TYPE="INTEGER" DIM_TYPE="FACT"> </COLUMN>*/
        }
        else if(id.equalsIgnoreCase("fin_inv_asin_count") || id.equalsIgnoreCase("daa_asin_count") ||
                id.equalsIgnoreCase("ds_asin_count") || id.equalsIgnoreCase("ddoi_asin_count")
                || id.equalsIgnoreCase("vsc_asin_count_d") || id.equalsIgnoreCase("d_ord_asin_count")){
            return new FinalizedReportNode(new QueryAttribute(queryAttribute(719)), AggregateFunctions.COUNT_DISTINCT );
        }
        
        //////////////////////////////////////////////////////////
        // Added default columns 
        //////////////////////////////////////////////////////////
        if(id.equalsIgnoreCase("net_units")){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_units"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_units"))) });
            node.addAdditionalAttribute("expressionId", "201");
            node.addAdditionalAttribute("expressionName", "Units -- Ordered Net");
            return new FinalizedReportNode(node, "net_units", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("net_ops")){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_amt"))) });
            node.addAdditionalAttribute("expressionId", "202");
            node.addAdditionalAttribute("expressionName", "Order Product Sales Net");
            return new FinalizedReportNode(node, "net_ops", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("d_ic_derived_cost")){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ic_cost"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ic_on_hand_quantity"))) });
            node.addAdditionalAttribute("expressionId", "203");
            node.addAdditionalAttribute("expressionName", "Extended FIFO Cost (On Hand Quantity)");
            return new FinalizedReportNode(node, "d_ic_derived_cost", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("vsc_cost") && hashAreaActivityDay.containsKey(currentRunData.areaId)){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("vsc_quantity_unpacked"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("vsc_vendor_cost"))) });
            node.addAdditionalAttribute("expressionId", "204");
            node.addAdditionalAttribute("expressionName", "Extended FIFO Cost");
            return new FinalizedReportNode(node, "vsc_cost", AggregateFunctions.SUM);
           
       }
        else if(id.equalsIgnoreCase("d_ord_tot_net_sales_price")){
            QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_units"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_units"))) });
            QueryInternalNode retval = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1736)), node } );
            retval.addAdditionalAttribute("expressionId", "205");
            retval.addAdditionalAttribute("expressionName", "Amount -- Total Net Ordered Selling Price");
            return new FinalizedReportNode(retval, "d_ord_tot_net_sales_price", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("d_ord_tot_net_list_price")){
            QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_units"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_units"))) });
            QueryInternalNode retval = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1737)), node } );
            retval.addAdditionalAttribute("expressionId", "206");
            retval.addAdditionalAttribute("expressionName", "Amount -- Total Net Ordered List Price");
            return new FinalizedReportNode(retval, "d_ord_tot_net_list_price", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("d_odoi_net_open_order_qty") ){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity_unpacked"))) });
            node.addAdditionalAttribute("expressionId", "207");
            node.addAdditionalAttribute("expressionName", "Net Confirmed (Open) Order Quantity (Net of Receipts)");
            return new FinalizedReportNode(node, "d_odoi_net_open_order_qty", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("ds_tot_sales_price") ){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1735)), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ds_shipped_units"))) });
            node.addAdditionalAttribute("expressionId", "208");
            node.addAdditionalAttribute("expressionName", "Amount -- Total  Shipped Selling Price");
            
            return new FinalizedReportNode(node, "ds_tot_sales_price", AggregateFunctions.SUM);
        }
        
        else if(id.equalsIgnoreCase("ds_tot_list_price") ){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1732)), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ds_shipped_units"))) });
            node.addAdditionalAttribute("expressionId", "209");
            node.addAdditionalAttribute("expressionName", "Amount -- Total Shipped List Price");
            return new FinalizedReportNode(node, "ds_tot_list_price", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("d_ord_tot_gro_sales_price")){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                new QueryAttribute(queryAttribute(1072)), new QueryAttribute(queryAttribute(1736))
            });
            node.addAdditionalAttribute("expressionId", "210");
            node.addAdditionalAttribute("expressionName", "Amount -- Total Gross Ordered Selling Price");
            return new FinalizedReportNode(node, "d_ord_tot_gro_sales_price", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("ddoi_extended_cost")){
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ddoi_cost"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ddoi_quantity"))) });
            node.addAdditionalAttribute("expressionId", "211");
            node.addAdditionalAttribute("expressionName", "Extended Order Cost");
            return new FinalizedReportNode(node, "ddoi_extended_cost", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("cp_net_revenue")){
            QueryTreeNode node1 = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_shipping_revenue_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp.refunds_revenue_amt"))) });
            QueryTreeNode node2= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_other_revenue_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_giftwrap_revenue_amt"))) });
            QueryTreeNode node3= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_subscription_revenue_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_liquidation_revenue_amt"))) });
            QueryTreeNode node4= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp.revenue_share_amt"))), node3});
            QueryTreeNode node5= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node2, node4});
            QueryInternalNode node6= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node1, node5});
            node6.addAdditionalAttribute("expressionId", "146");
            node6.addAdditionalAttribute("expressionName", "CP net revenue");
            return new FinalizedReportNode(node6, "cp_net_revenue", AggregateFunctions.SUM);
            
        }
        else if(id.equalsIgnoreCase("d_odoi_extndd_net_opn_ordr_qty") ){
            QueryTreeNode diff = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity_unpacked"))) });
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                diff, new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ddoi_cost")))
            });
            node.addAdditionalAttribute("expressionName","Extended Net Confirmed (Open) Order Quantity (Net of Receipts)");
          //TODO: node.addAdditionalAttribute("expressionId", "");
            return new FinalizedReportNode(node, "d_odoi_extndd_net_opn_ordr_qty", AggregateFunctions.SUM);
        }
        else if(id.equalsIgnoreCase("ds_avg_sales_price") ){
            return new FinalizedReportNode(new QueryAttribute(queryAttribute(1735)), "ds_avg_sales_price", AggregateFunctions.AVG);
        /*<COLUMN ID="ds_avg_sales_price" DESC="Price -- Average Sales Price (ASP)" TABLE="ds" EXPR="AVG(ds.AVG_SALES_PRICE)" TYPE="Numeric" DB_TYPE="INTEGER" DIM_TYPE="FACT">
        </COLUMN>*/
        }
        else if(id.equalsIgnoreCase("ds_avg_list_price") ){
            return new FinalizedReportNode(new QueryAttribute(queryAttribute(1732)), "ds_avg_list_price", AggregateFunctions.AVG);
        /*<COLUMN ID="ds_avg_sales_price" DESC="Price -- Average Sales Price (ASP)" TABLE="ds" EXPR="AVG(ds.AVG_SALES_PRICE)" TYPE="Numeric" DB_TYPE="INTEGER" DIM_TYPE="FACT">
        </COLUMN>*/
        }
        else if(id.equalsIgnoreCase("d_ord_avg_list_price")){
            return new FinalizedReportNode(new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord.avg_list_price").longValue())), AggregateFunctions.AVG); 
            /*<COLUMN ID="d_ord_avg_sales_price" DESC="Price -- Average Sales Price (ASP)" TABLE="d_ord" EXPR="AVG(d_ord.AVG_SALES_PRICE)" TYPE="Double" DB_TYPE="INTEGER" DIM_TYPE="FACT"> </COLUMN>*/
        }
        return null;
    }
    
    /**
     * Returns a QueryTreeNode of calculated expressions. 
     * The definitions are from the dssui.xml.
     * These columns usually appear in the "REPORT" part and NOT the "RESTRICTIONS" part
     * @param dssColumnName
     * @return
     */
    private QueryTreeNode getCalculatedColumns(String dssColumnName){
        if(dssColumnName.equalsIgnoreCase("net_units")){
            QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_units"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_units"))) });
            return node;
        }
        else if(dssColumnName.equalsIgnoreCase("net_ops")){
            QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_amt"))) });
            return node;
        }
        else if(dssColumnName.equalsIgnoreCase("d_ic_derived_cost")){
            QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ic_cost"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ic_on_hand_quantity"))) });
            return node;
        }
       else if(dssColumnName.equalsIgnoreCase("d_month_as_int_year") && hashAreaActivityDay.containsKey(currentRunData.areaId)){
            long attrID = hashAreaActivityDay.get(currentRunData.areaId);
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.TO_CHAR.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(attrID)), new QueryConstant("'YYYY-MM'") });
            node.addAdditionalAttribute("dateExpName", "YYYY-MM");
            node.addAdditionalAttribute("dateAttrId", Long.toString(attrID));
            return node;
            
        }
       else if(dssColumnName.equalsIgnoreCase("d_month") && hashAreaActivityDay.containsKey(currentRunData.areaId)){
           long attrID = hashAreaActivityDay.get(currentRunData.areaId);
           QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.TO_CHAR.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(attrID)), new QueryConstant("'MONTH'") });
           node.addAdditionalAttribute("dateExpName", "MONTH name");
           node.addAdditionalAttribute("dateAttrId", Long.toString(attrID));
           return node;
           
       }
        
        if(dssColumnName.equalsIgnoreCase("d_week_ending")){
            if(currentRunData.areaId == 0){
                writeToLog("No date defined for area.");
                return null;
            }
                
            long attrId = hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId)).longValue();
            QueryAttribute node = new QueryAttribute(queryAttribute(attrId));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                    node, 
                    new QueryConstant("'D'")}),
                new QueryConstant("6")
            });
            date.addAdditionalAttribute("dateExpName", "Week End Date");
            date.addAdditionalAttribute("dateAttrId", Long.toString(attrId));
            return date;
        }
       else if(dssColumnName.equalsIgnoreCase("vsc_cost") && hashAreaActivityDay.containsKey(currentRunData.areaId)){
            QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("vsc_quantity_unpacked"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("vsc_vendor_cost"))) });
            return node;
           
       }
       else if(dssColumnName.equalsIgnoreCase("dist_id_seg_alt")){
           QueryTreeNode node = new QueryAttribute(queryAttribute(1154));
           return node;
          
      }
       else if(dssColumnName.equalsIgnoreCase("d_ord_tot_net_sales_price")){
           QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_units"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_units"))) });
           QueryTreeNode retval = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1736)), node } );
           return retval;
       }
       else if(dssColumnName.equalsIgnoreCase("d_ord_tot_net_list_price")){
           QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_ordered_units"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_ord_adjusted_units"))) });
           QueryTreeNode retval = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1737)), node } );
           return retval;
       }
       else if(dssColumnName.equalsIgnoreCase("vendor_order_cond_desc") || dssColumnName.equalsIgnoreCase("ddo_condition_description")){
           QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.DECODE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
               new QueryAttribute(queryAttribute(1394)),
               new QueryConstant("0"), new QueryConstant("'Submitted'"),
               new QueryConstant("1"), new QueryConstant("'Confirmed'"),
               new QueryConstant("2"), new QueryConstant("'Complete'"),
               new QueryConstant("'Unknown'")
           });
           node.addAdditionalAttribute("expressionId", "144");
           node.addAdditionalAttribute("expressionName","Order Condition Description");
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("d_odoi_net_open_order_qty") ){
           QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity_unpacked"))) });
           
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("ds_tot_sales_price") ){
           QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1735)), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ds_shipped_units"))) });
           
           return node;
       }
       
       else if(dssColumnName.equalsIgnoreCase("ds_tot_list_price") ){
           QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1732)), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ds_shipped_units"))) });
           
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("mer_merchant_type") ){
           QueryTreeNode inStmt = new QueryInternalNode(queryFunction(DSSUI_OP.IN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(25)), 
           new QueryConstant("9"), new QueryConstant("10"), new QueryConstant("11"), new QueryConstant("12"), new QueryConstant("13"), new QueryConstant("608808520"), new QueryConstant("754443913"), new QueryConstant("473940355"), new QueryConstant("756980553"),
                   new QueryConstant("1195204730"), new QueryConstant("1146949031"), new QueryConstant("1147063211") });
           //functionId = 29 SQL CASE
           QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.CASE.functionId, DBType.SQL.getId()), new QueryTreeNode[] { inStmt, new QueryConstant("'Retail Merchant'"), new QueryConstant("'3P Merchant'") });
           node.addAdditionalAttribute("expressionId", "145");
           node.addAdditionalAttribute("expressionName", "Merchant Type(Retail/3P)");
           return node;
       }
        //
       else if(dssColumnName.equalsIgnoreCase("d_ic_distributor_id")) {
           QueryTreeNode node = new QueryInternalNode(queryFunction( DSSUI_OP.TRIM.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(1154)) } );
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("d_quarter_year")) {
           long attrID = hashAreaActivityDay.get(currentRunData.areaId);
           QueryInternalNode retval = new QueryInternalNode(queryFunction(25, DBType.SQL.getId()), new QueryTreeNode[] {
               new QueryInternalNode(queryFunction(25, DBType.SQL.getId()), new QueryTreeNode[] {
                   new QueryInternalNode(queryFunction(DSSUI_OP.TO_CHAR.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(attrID)), new QueryConstant("'YYYY'") }),
                   new QueryConstant("'-'")
                   
               }),
               
               new QueryInternalNode(queryFunction(25, DBType.SQL.getId()), new QueryTreeNode[] {
                   new QueryConstant("'Q'"),
                   new QueryInternalNode(queryFunction(DSSUI_OP.TO_CHAR.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(attrID)), new QueryConstant("'Q'") })
               })
           } );
           retval.addAdditionalAttribute("dateExpName", "YYYY-Q1");
           retval.addAdditionalAttribute("dateAttrId", Long.toString(attrID));
           return retval;
       }
       else if(dssColumnName.equalsIgnoreCase("med_asin_artist_primary")){
           QueryInternalNode node = new QueryInternalNode(queryFunction(45, DBType.SQL.getId()), new QueryTreeNode[] {
               new QueryAttribute(queryAttribute(1733)), new QueryAttribute(queryAttribute(600))
           });
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("d_ord_tot_gro_sales_price")){
           QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
               new QueryAttribute(queryAttribute(1072)), new QueryAttribute(queryAttribute(1736))
           });
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("ddoi_extended_cost")){
           QueryTreeNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ddoi_cost"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ddoi_quantity"))) });
           
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("cp_net_revenue")){
           QueryTreeNode node1 = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_shipping_revenue_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp.refunds_revenue_amt"))) });
           QueryTreeNode node2= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_other_revenue_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_giftwrap_revenue_amt"))) });
           QueryTreeNode node3= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_subscription_revenue_amt"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp_liquidation_revenue_amt"))) });
           QueryTreeNode node4= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("cp.revenue_share_amt"))), node3});
           QueryTreeNode node5= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node2, node4});
           QueryInternalNode node6= new QueryInternalNode(queryFunction(DSSUI_OP.PLUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node1, node5});
           node6.addAdditionalAttribute("expressionId", "146");
           node6.addAdditionalAttribute("expressionName", "CP net revenue");
           return node6;
           
       }
       else if(dssColumnName.equalsIgnoreCase("d_ic_inv_age") ){
           QueryInternalNode value = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
               new QueryAttribute(queryAttribute(1600)),
               new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                 new QueryAttribute(queryAttribute(1282)),
                 new QueryConstant("'DDD'")
               })
           });
           QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.CASE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
               new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {value, new QueryConstant("0"), new QueryConstant("30")}), new QueryConstant("'Current'"),
               new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {value, new QueryConstant("31"), new QueryConstant("60")}),new QueryConstant("'31 to 60'"),
               new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {value, new QueryConstant("61"), new QueryConstant("90")}),new QueryConstant("'61 to 90'"),
               new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {value, new QueryConstant("91"), new QueryConstant("120")}), new QueryConstant("'91 to 120'"),
               new QueryConstant("'Over 120'")
           });
           
           //TODO: node.addAdditionalAttribute("expressionId", "");
           node.addAdditionalAttribute("expressionName","Financial Inventory Age");
           return node;
       }
       else if(dssColumnName.equalsIgnoreCase("d_odoi_extndd_net_opn_ordr_qty") ){
           QueryTreeNode diff = new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity"))), new QueryAttribute(queryAttribute(colIdToAttrMapping.get("d_odoi_quantity_unpacked"))) });
           QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.MULTIPLY.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
               diff, new QueryAttribute(queryAttribute(colIdToAttrMapping.get("ddoi_cost")))
           });
           node.addAdditionalAttribute("expressionName","Extended Net Confirmed (Open) Order Quantity (Net of Receipts)");
         //TODO: node.addAdditionalAttribute("expressionId", "");
           return node;
       }
       return null;
        /* <COLUMN ID="net_units" DESC="Units -- Ordered Net" TABLE="d_ord" EXPR="d_ord.ordered_units + d_ord.adjusted_units" TYPE="Numeric" DB_TYPE="INTEGER" DIM_TYPE="FACT"/>
        <COLUMN ID="net_ops" DESC="Order Product Sales Net" TABLE="d_ord" EXPR="d_ord.ordered_amt + d_ord.adjusted_amt" TYPE="Currency" DB_TYPE="INTEGER" DIM_TYPE="FACT"/>
        <COLUMN ID="d_ic_derived_cost" DESC="Extended FIFO Cost (On Hand Quantity)" REQUIRED_COL_IDS="d_ic_snapshot_date" COL_IDS="d_ic_cost,d_ic_on_hand_quantity" EXPR="(d_ic_cost * d_ic_on_hand_quantity)" TYPE="Currency" DB_TYPE="integer" DIM_TYPE="FACT"/>
        <COLUMN ID="d_month_as_int_year" DESC="Calendar Month-Year (2005-12)" COL_ID="date" EXPR="concat(datename(year,date),'-',string(datepart(month,date)))" EXPR_ORACLE="to_char(date,'YYYY-MM')" TYPE="Text" DB_TYPE="char(14)" DIM_TYPE="DIMENSION"/>
        <COLUMN ID="vsc_cost" DESC="Extended FIFO Cost" COL_IDS="vsc_quantity_unpacked,vsc_vendor_cost" EXPR="vsc_quantity_unpacked * vsc_vendor_cost" TYPE="Currency" DB_TYPE="integer" DIM_TYPE="FACT"/>
        <COLUMN_ALTERNATE ID="dist_id_seg_alt" COLUMNS="d_cri_distributor_id,d_dr_distributor_id,d_dri_distributor_id,ddoi_distributor_id"/>
        <COLUMN ID="d_ord_tot_net_sales_price" DESC="Amount -- Total Net Ordered Selling Price" TABLE="d_ord" EXPR="d_ord.AVG_SALES_PRICE * (d_ord.ordered_units + d_ord.adjusted_units) " TYPE="Currency" DB_TYPE="INTEGER" DIM_TYPE="FACT"/>
        <COLUMN ID="d_ord_tot_net_list_price" DESC="Amount -- Total Net Ordered List Price" TABLE="d_ord" EXPR="d_ord.AVG_LIST_PRICE * (d_ord.ordered_units + d_ord.adjusted_units) " TYPE="Currency" DB_TYPE="INTEGER" DIM_TYPE="FACT">
 <COLUMN_ALTERNATE ID="vendor_order_cond_desc" COLUMNS="ddo_condition_description"/>
      <COLUMN ID="ddo_condition_description" DESC="Vendor Order Condition Desc." TABLE="ddo" EXPR="'decode(ddo.CONDITION,0,'Submitted',1,'Confirmed',2,'Complete','Unknown')" TYPE="Text" DB_TYPE="VARCHAR2" DIM_TYPE="DIMENSION"/>
        <COLUMN ID="d_odoi_net_open_order_qty" DESC="Net Confirmed (Open) Order Quantity (Net of Receipts)" TABLE="d_odoi" EXPR="(d_odoi.QUANTITY - d_odoi.QUANTITY_UNPACKED)" TYPE="Numeric" DB_TYPE="integer" DIM_TYPE="FACT"/>
        <COLUMN ID="ds_tot_sales_price" DESC="Amount -- Total  Shipped Selling Price" TABLE="ds" EXPR="ds.AVG_SALES_PRICE * ds.shipped_units" TYPE="Numeric" DB_TYPE="INTEGER" DIM_TYPE="FACT">
</COLUMN>
<COLUMN ID="ds_tot_list_price" DESC="Amount -- Total Shipped List Price" TABLE="ds" EXPR="ds.AVG_LIST_PRICE * ds.shipped_units " TYPE="Numeric" DB_TYPE="INTEGER" DIM_TYPE="FACT">
</COLUMN>
        <COLUMN ID="mer_merchant_type" DESC="Merchant Type(Retail/3P)" TABLE="mer" EXPR="CASE  WHEN mer.MERCHANT_CUSTOMER_ID IN  (9, 10, 11, 12, 13, 608808520, 754443913, 473940355, 756980553, 1195204730, 1146949031, 1147063211) THEN 'Retail Merchant' ELSE '3P Merchant' END" TYPE="Text" DB_TYPE="VARCHAR2" DIM_TYPE="DIMENSION">
</COLUMN>
<COLUMN ID="d_ic_distributor_id" DESC="Vendor Code" TABLE="d_ic" EXPR="trim(d_ic.DISTRIBUTOR_ID)" TYPE="Text" DB_TYPE="VARCHAR2" DIM_TYPE="DIMENSION"/>
,(to_char(ds.ACTIVITY_DAY,'YYYY') || '-' || 'Q' || to_char(ds.ACTIVITY_DAY,'Q')) AS d_quarter_year 
<COLUMN ID="med_asin_artist_primary" DESC="Artist Primary" TABLE="med_asin" EXPR="coalesce(med_asin.ARTIST_PRIMARY,med_asin.CONTRIBUTOR_PRIMARY)" TYPE="Text" DB_TYPE="VARCHAR2" DIM_TYPE="DIMENSION">
</COLUMN>
<COLUMN ID="d_ord_tot_gro_sales_price" DESC="Amount -- Total Gross Ordered Selling Price" TABLE="d_ord" EXPR="d_ord.AVG_SALES_PRICE * d_ord.ordered_units" TYPE="Currency" DB_TYPE="INTEGER" DIM_TYPE="FACT">
</COLUMN>
    <COLUMN ID="ddoi_extended_cost" DESC="Extended Order Cost" TABLE="ddoi" EXPR="ddoi.COST*ddoi.QUANTITY" TYPE="Currency" DB_TYPE="integer" DIM_TYPE="FACT"/>
     <COLUMN ID="cp_net_revenue" DESC="Net Revenue" TABLE="cp" EXPR="(cp.shipping_revenue_amt - cp.refunds_revenue_amt + cp.other_revenue_amt
                + cp.giftwrap_revenue_amt + cp.subscription_revenue_amt + cp.liquidation_revenue_amt + cp.REVENUE_SHARE_AMT)" TYPE="Currency" DB_TYPE="integer" DIM_TYPE="FACT">
        </COLUMN>
        <COLUMN ID="d_ic_inv_age" DESC="Financial Inventory Age" TABLE="d_ic" EXPR="decode(trunc((d_ic.SNAPSHOT_DAY - trunc(d_ic.COST_ACQUISITION_DATETIME))/30)
                    , 0, 'Current'
                    , 1, '31 to 60'
                    , 2, '61 to 90'
                    , 3, '91 to 120'
                    , 'Over 120')" TYPE="Text" DB_TYPE="VARCHAR" DIM_TYPE="DIMENSION"/>
<COLUMN ID="d_odoi_extndd_net_opn_ordr_qty" DESC="Extended Net Confirmed (Open) Order Quantity (Net of Receipts)" TABLE="d_odoi" EXPR="((d_odoi.QUANTITY - d_odoi.QUANTITY_UNPACKED)*ddoi.cost)" TYPE="Numeric" DB_TYPE="integer" DIM_TYPE="FACT"/>
        
*/
        
    }
    
    /**
     * In Area=2, the date used is a TIMESTAMP, so when comparising we want to trunc it (was the same in DSS-UI using join conditions)
     * @return
     */
    private QueryTreeNode getDateQueryNodeByArea(){
        if(currentRunData.areaId == 2 && hashAreaActivityDay.get(Long.valueOf(2)).longValue()==1585){
            return new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {new QueryAttribute(queryAttribute(1585)), new QueryConstant("'D'") });
        }
        return null;
    }
    
    
    /**
     * Recursively build a condition expression-tree.
     * @param parent
     * @param currLevel
     */
    @SuppressWarnings("unchecked")
    private void getConditionExpression(QueryInternalNode parent, Element currLevel){
        for(Element elem : (List<Element>)currLevel.getChildren("RESTRICTION")){
            QueryTreeNode node = getConditionFromRestrictionElement(elem);
            if(node != null){
                parent.addChild(node);
            }
        }
        if(currLevel.getChild("AND") != null){
            for(Element andClause : (List<Element>)currLevel.getChildren("AND")){
            //Element andClause = currLevel.getChild("AND");
                QueryInternalNode andNode = new QueryInternalNode(queryFunction(DSSUI_OP.AND.getFunctionID(), DBType.SQL.getId()));
                getConditionExpression(andNode, andClause);
                parent.addChild(andNode);
            }
        }
        if(currLevel.getChild("OR") != null){
            for(Element orClause : (List<Element>)currLevel.getChildren("OR")){
            //  Element orClause = currLevel.getChild("OR");
                QueryInternalNode orNode = new QueryInternalNode(queryFunction(DSSUI_OP.OR.getFunctionID(), DBType.SQL.getId()));
                getConditionExpression(orNode, orClause);
                parent.addChild(orNode);
            }
        }
        
    }
    
    /**
     * A simple restriction - from one attribute. 
     * @param attrId
     * @param restriction
     * @return
     */
    private QueryTreeNode getSimpleConditionFromRestriction(long attrId, Element restriction){
        QueryAttribute attr = null;
        attr =  new QueryAttribute(queryAttribute(attrId));
        
        QueryTreeNode actualNode = getCustomColumn(queryAttribute(attrId));
        if(actualNode == null)
            actualNode = attr;
        
               
        String valuesStr = restriction.getAttributeValue("VALUES");
        if(valuesStr==null){
            writeToLog("Attribute VALUES does not appear in restriction!");
            return null;
        }
        String values[] = valuesStr.split(",");
        
        String opCodeStr = restriction.getAttributeValue("OP");
        

        // A simple EQUALS for non-dates.
        if(attr.getAttribute().getColumnType()!=ColumnType.DATE && values.length == 1 && (opCodeStr==null || opCodeStr.equalsIgnoreCase("EQ"))){
            String s = values[0];
            s = prepareConstantString(values[0], queryAttribute(attrId));
            return new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { actualNode, new QueryConstant(s) } );
        }
        //A simple EQUALS for dates.
        if(attr.getAttribute().getColumnType()==ColumnType.DATE && values.length == 1 && (opCodeStr==null || opCodeStr.equalsIgnoreCase("EQ"))){
            if(attr.getAttribute().getId()==764){
                attr = new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            }
            QueryInternalNode date = parseDateString(valuesStr);
            if(date==null)
                return null;
            return new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attr, date } );
        }
        // IN/EQUALS with more than one value.
        else if((opCodeStr!=null && opCodeStr.equalsIgnoreCase("EQ") && values.length > 1 )||(opCodeStr!=null && opCodeStr.equalsIgnoreCase("IN")) || (opCodeStr==null && values.length > 1)){
            if(attr.getAttribute().getId()==764){// the generic date attribute-> replace with the specific one if exists.
                attrId = queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))).getId();
                attr = new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
                actualNode = attr;
            }
            String s[] = new String[values.length];
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.IN.getFunctionID(), DBType.SQL.getId()));
            node.addChild(actualNode);
            for(int i=0;i<values.length;i++){
                s[i] = prepareConstantString(values[i], queryAttribute(attrId));
               if(attr.getAttribute().getColumnType()!=ColumnType.DATE){
                    QueryConstant newConst = new QueryConstant(s[i]);
                    node.addChild(newConst);
                }
                else{
                    QueryInternalNode date = parseDateString(s[i]);
                    node.addChild(date);
                }
                    
            }
            return node;
            
        }
        //RANGE with dates.
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase("RANGE") && attr.getAttribute().getColumnType()==ColumnType.DATE){
            if(attr.getAttribute().getId()==764){
                attr = new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            }
            return getDateInternalNode_RANGE(attr, values);
        }
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase("RANGE") && attr.getAttribute().getColumnType()!=ColumnType.DATE){
            String max = values[1];
            String min = values[0];
            QueryInternalNode between = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { actualNode,
                new QueryConstant(min),
                new QueryConstant(max)
            } );
            return between;
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("GTE") && values.length==1){
            return handleOperandsSingleValue("GTE", valuesStr, attr);
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("GT") && values.length==1){
            return handleOperandsSingleValue("GT", valuesStr, attr);
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("LTE") && values.length==1){
            return handleOperandsSingleValue("LTE", valuesStr, attr);
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("LT") && values.length==1){
            return handleOperandsSingleValue("LT", valuesStr, attr);
        }
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase(DSSUI_OP.LIKE_BEGINS_WITH.getDescription())){
            //return new QueryInternalNode(queryFunction(DSSUI_OP.LIKE_BEGINS_WITH.getFunctionID()), new QueryTreeNode[] { attr, new QueryConstant(prepareConstantString(valuesStr, attr.getAttribute()))});
            return new QueryInternalNode(queryFunction(10, DBType.SQL.getId()), new QueryTreeNode[] { attr, new QueryConstant("'" + prepareConstantString(valuesStr, attr.getAttribute()).replace("'","") + "%'" )});
        }
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase("like_contains")){
            return new QueryInternalNode(queryFunction(10, DBType.SQL.getId()), new QueryTreeNode[] { attr, new QueryConstant("'%" + prepareConstantString(valuesStr, attr.getAttribute()).replace("'","") + "%'" )});
            
        }
        else{
            writeToLog("OpCodeStr: " + opCodeStr + " Number of values: "+ values.length);
            return null;
        }
    }
    
    
    /**
     * A simple restriction - from one attribute. 
     * @param attrId
     * @param restriction
     * @return
     */
    private QueryTreeNode getComputedColumnConditionFromRestriction(String dssId, Element restriction){
        
        QueryTreeNode actualNode = getCalculatedColumns(dssId);
        
        String valuesStr = restriction.getAttributeValue("VALUES");
        if(valuesStr==null){
            writeToLog("Attribute VALUES does not appear in restriction!");
            return null;
        }
        String values[] = valuesStr.split(",");
        
        String opCodeStr = restriction.getAttributeValue("OP");
        
        FunctionTypeValidity functionTypeCalculator = new FunctionTypeValidity();
        actualNode.accept(functionTypeCalculator);
        InnerGHQexpressionType ghqExType = functionTypeCalculator.getSubExpressionTypes().get(actualNode);

        // A simple EQUALS for non-dates.
        if(ghqExType!= InnerGHQexpressionType.DATE && values.length == 1 && (opCodeStr==null || opCodeStr.equalsIgnoreCase("EQ"))){
            String s = values[0];
            s = prepareConstantString(values[0], ghqExType);
            return new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { actualNode, new QueryConstant(s) } );
        }
      
        //A simple EQUALS for dates.
        if(ghqExType== InnerGHQexpressionType.DATE && values.length == 1 && (opCodeStr==null || opCodeStr.equalsIgnoreCase("EQ"))){
            writeToLog("getComputedColumnConditionFromRestriction for dates expression is not completely supported");
            return null;
        }
        // IN/EQUALS with more than one value.
        else if((opCodeStr!=null && opCodeStr.equalsIgnoreCase("EQ") && values.length > 1 )||(opCodeStr!=null && opCodeStr.equalsIgnoreCase("IN")) || (opCodeStr==null && values.length > 1)){
           
            String s[] = new String[values.length];
            QueryInternalNode node = new QueryInternalNode(queryFunction(DSSUI_OP.IN.getFunctionID(), DBType.SQL.getId()));
            node.addChild(actualNode);
            for(int i=0;i<values.length;i++){
                s[i] = prepareConstantString(values[i], ghqExType);
               if(ghqExType !=InnerGHQexpressionType.DATE){
                    QueryConstant newConst = new QueryConstant(s[i]);
                    node.addChild(newConst);
                }
                else{
                    QueryInternalNode date = parseDateString(s[i]);
                    node.addChild(date);
                }
                    
            }
            return node;
            
        }
        
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase("RANGE") && ghqExType != InnerGHQexpressionType.DATE){
            String max = values[1];
            String min = values[0];
            QueryInternalNode between = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { actualNode,
                new QueryConstant(min),
                new QueryConstant(max)
            } );
            return between;
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("GTE") && values.length==1){
            return handleOperandsSingleValue("GTE", valuesStr, ghqExType, actualNode);
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("GT") && values.length==1){
            return handleOperandsSingleValue("GT", valuesStr, ghqExType, actualNode);
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("LTE") && values.length==1){
            return handleOperandsSingleValue("LTE", valuesStr, ghqExType, actualNode);
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("LT") && values.length==1){
            return handleOperandsSingleValue("LT", valuesStr, ghqExType, actualNode);
        }
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase(DSSUI_OP.LIKE_BEGINS_WITH.getDescription())){
            //return new QueryInternalNode(queryFunction(DSSUI_OP.LIKE_BEGINS_WITH.getFunctionID()), new QueryTreeNode[] { attr, new QueryConstant(prepareConstantString(valuesStr, attr.getAttribute()))});
            return new QueryInternalNode(queryFunction(10, DBType.SQL.getId()), new QueryTreeNode[] { actualNode, new QueryConstant("'" + prepareConstantString(valuesStr, ghqExType).replace("'","") + "%'" )});
        }
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase("like_contains")){
            return new QueryInternalNode(queryFunction(10, DBType.SQL.getId()), new QueryTreeNode[] { actualNode, new QueryConstant("'%" + prepareConstantString(valuesStr, ghqExType).replace("'","") + "%'" )});
            
        }
        else{
            writeToLog("OpCodeStr: " + opCodeStr + " Number of values: "+ values.length);
            return null;
        }
    }
   
    
    /**
     * A simple restriction - from one attribute. 
     * @param attributeID
     * @param restriction
     * @return
     */
    private QueryTreeNode getSimpleConditionFromRestriction(String attributeID, Element restriction){
        long attrId = colIdToAttrMapping.get(attributeID);
        return getSimpleConditionFromRestriction(attrId, restriction);
   
    }
    
    /**
     * Parses a constant date-string value to a TO_DATE InternalQueryNode.
     * Accepts formats MM/DD/YYYY, YYYY/MM/DD, MM-DD-YYYY, but will translate to YYYY/MM/DD.
     * @param quotedDate date in format MM/DD/YYYY, YYYY/MM/DD, or MM-DD-YYYY
     * @return QueryInternalNode
     */
    private QueryInternalNode parseDateString(String quotedDate){
        
        // Remove any wrapping single quotes; we'll add them back later
        String unquotedDate;
        if (quotedDate.startsWith("'") && quotedDate.endsWith("'")) {
            unquotedDate = quotedDate.substring(1, quotedDate.length() - 1);
        }
        else {
            unquotedDate = quotedDate;
        }
        
        // Consider different formats and translate to the standard MM/DD/YYYY
        String MM_DD_YYYY_withSlashes;
        if (unquotedDate.matches("\\d\\d/\\d\\d/\\d\\d\\d\\d")) { // MM/DD/YYYY
            MM_DD_YYYY_withSlashes = unquotedDate;
        }
        else if (unquotedDate.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d")) { // YYYY/MM/DD
            String[] YYYY_MM_DD = unquotedDate.split("/");
            MM_DD_YYYY_withSlashes = YYYY_MM_DD[1] + "/" + YYYY_MM_DD[2] + "/" + YYYY_MM_DD[0];
        }
        else if (unquotedDate.matches("\\d\\d\\-\\d\\d\\-\\d\\d\\d\\d")) { // MM-DD-YYYY
            MM_DD_YYYY_withSlashes = unquotedDate.replace("-", "/");
        }
        else {
            throw new RuntimeException("Invalid date string: " + quotedDate);
        }
        
        // Build and return the QueryInternalNode for this date
        return new QueryInternalNode(
                queryFunction(DSSUI_OP.TO_DATE.getFunctionID(), DBType.SQL.getId()),
                new QueryTreeNode[] {
                    new QueryConstant("'" + MM_DD_YYYY_withSlashes + "'"),
                    new QueryConstant("'MM/DD/YYYY'")
                }
        );
    }
    
    
    /**
     * A helper method that generates the "right side" of a simple condition clause. (i.e. Attribute > _5_  , Attribute <= '12-31-2009' etc.)
     * parses date into a QueryInternalNode if needed.
     * @param operator
     * @param value
     * @param attr
     * @return
     */
    private QueryInternalNode handleOperandsSingleValue(String operator, String value, QueryAttribute attr){
        QueryTreeNode subExp = null;
        if(attr.getAttribute().getColumnType() == ColumnType.DATE){
            subExp = parseDateString(value);
        }
        else
            subExp = new QueryConstant(prepareConstantString(value, attr.getAttribute())) ;
        
        if(attr.getAttribute().getId()==764){
            attr = new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
        }            
        return new QueryInternalNode(DSSUI_OP.getFunction(operator, this), new QueryTreeNode[] { attr, subExp });
    }
    
    /**
     * A helper method that generates the "right side" of a simple condition clause. (i.e. Attribute > _5_  , Attribute <= '12-31-2009' etc.)
     * parses date into a QueryInternalNode if needed.
     * @param operator
     * @param value
     * @param attr
     * @return
     */
    private QueryInternalNode handleOperandsSingleValue(String operator, String value, InnerGHQexpressionType ghqType, QueryTreeNode node){
        QueryTreeNode subExp = null;
        if(ghqType == InnerGHQexpressionType.DATE){
            subExp = parseDateString(value);
        }
        else
            subExp = new QueryConstant(prepareConstantString(value, ghqType)) ;
        
                
        return new QueryInternalNode(DSSUI_OP.getFunction(operator, this), new QueryTreeNode[] { node, subExp });
    }
    
    /**
     * Creates a RANGE. if more than 2 values appear, create an OR with the ranges.
     * @param attr
     * @param values
     * @return
     */
    private QueryInternalNode getDateInternalNode_RANGE(QueryAttribute attr, String values[]){
        ArrayList<QueryInternalNode> betweenStatements = new ArrayList<QueryInternalNode>();
        for(int i=0; i<values.length/2; i++){
            QueryInternalNode between = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attr,
                parseDateString(values[i*2]),
                parseDateString(values[(i*2)+1])
                //new QueryInternalNode(queryFunction(DSSUI_OP.TO_DATE.getFunctionID()), new QueryTreeNode[] { new QueryConstant(values[i*2]), new QueryConstant("'MM/DD/YYYY'") }),
                //new QueryInternalNode(queryFunction(DSSUI_OP.TO_DATE.getFunctionID()), new QueryTreeNode[] { new QueryConstant(values[(i*2)+1]), new QueryConstant("'MM/DD/YYYY'") })
            } );
            betweenStatements.add(between);
        }
        if(betweenStatements.size() > 1){
            QueryInternalNode orStatement = new QueryInternalNode(queryFunction(DSSUI_OP.OR.getFunctionID(), DBType.SQL.getId()));
            for(QueryInternalNode node : betweenStatements){
                orStatement.addChild(node);
            }
            return orStatement;
        }
        else
        {
            return betweenStatements.get(0);
        }
    }
    
    /**
     * Removes/Adds quotes to constant values based on the attribute.
     * @param orig
     * @param attr
     * @return
     */
    private String prepareConstantString(String orig, Attribute attr){
        if(attr.getColumnType() == ColumnType.NUMERIC){
            if(orig.matches("^(-)?[0-9]+(.[0-9]+)?$"))
                return orig;
            else if(orig.matches("^'(-)?[0-9]+(.[0-9]+)?'$")){
                String retval = orig.replace("'","");
                return retval;
            }
                
        }
        if(attr.getColumnType() == ColumnType.CHAR){
            if(orig.startsWith("'"))
                return orig;
            else
                return "'" + orig + "'";
        }
        if(attr.getColumnType() == ColumnType.DATE){
            if(orig.startsWith("'"))
                return orig;
            else
                return "'" + orig + "'";
        }
            
        
        return orig;
    }
    
    /**
     * Removes/Adds quotes to constant values based on the attribute.
     * @param orig
     * @param attr
     * @return
     */
    private String prepareConstantString(String orig, InnerGHQexpressionType exType){
        if(exType == InnerGHQexpressionType.NUMERIC){
            if(orig.matches("^(-)?[0-9]+(.[0-9]+)?$"))
                return orig;
            else if(orig.matches("^'(-)?[0-9]+(.[0-9]+)?'$")){
                String retval = orig.replace("'","");
                return retval;
            }
                
        }
        if(exType == InnerGHQexpressionType.STRING){
            if(orig.startsWith("'"))
                return orig;
            else
                return "'" + orig + "'";
        }
        if(exType == InnerGHQexpressionType.DATE){
            if(orig.startsWith("'"))
                return orig;
            else
                return "'" + orig + "'";
        }
            
        
        return orig;
    }
    
    /**
     * Translates on restriction tag element into a GHQ condition.
     * Handles weird conditions (i.e yesterday etc.) a
     * @param restriction
     * @return
     */
    private QueryTreeNode getConditionFromRestrictionElement(Element restriction){
        String id = (restriction.getAttributeValue("ID") != null ? restriction
                .getAttributeValue("ID") : restriction.getAttributeValue("COL_ID"));
        
        if(id == null){
            if(restriction.getAttribute("SEGMENT_TYPE_ID")!=null){
                statistics.numOfSegments++;
                String segType = restriction.getAttributeValue("SEGMENT_TYPE_ID");
                String segIdString = "N/A";
                if(segType.equalsIgnoreCase("product") && restriction.getAttribute("VALUES")!=null){
                    segIdString = restriction.getAttributeValue("VALUES");
                    if(segIdString.matches("^[0-9]+$")){
                        return new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.functionId, DBType.SQL.getId()) , new QueryTreeNode[] { new QueryAttribute(queryAttribute(1581)), new QueryConstant(restriction.getAttributeValue("VALUES")) } );
                    }
                }
                writeToLog("Failed to convert segment of type: " + segType + " and values:"+segIdString);
            }
            return null;
        }
        
        id = cleanIdString(id);
        id = replaceIdIfNeeded(id);
        
        /*
         * If this restriction element is on "merchant_id" AND that attribute is not present in the driving table, ignore it.
         * This query probably is in a Vendor- or Distributor-related subject area where "merchant_id" doesn't make sense.
         * (Fix for https://tt.amazon.com/0028420287)
         */
        if (id.equalsIgnoreCase("merchant_id")) {
            Area currentArea = queryArea(currentRunData.areaId);
            MetaDataExtendedAttributes extendedAttributes = currentArea.getExtendedAttributes();
            JSONObject extendedAttributesData = extendedAttributes.getData();
            JSONArray tableRanks = extendedAttributesData.optJSONArray("AreaBasedTableRankingFromMDExtendedAttributes");
            Set<Attribute> drivingTableAttributes = new HashSet<Attribute>();
            for (int drivingTableIndex = 0; drivingTableIndex < tableRanks.length(); ++drivingTableIndex) {
                JSONObject tableRank = tableRanks.optJSONObject(drivingTableIndex);
                String tableName = tableRank.optString("TableName");
                Table table = queryTable(tableName);
                drivingTableAttributes.addAll(table.getAllAttributesCopy());
            }
            
            boolean foundMerchantIdInDrivingTable = false;
            for (Attribute drivingTableAttribute : drivingTableAttributes) {
                if (drivingTableAttribute.getId() == 25) { // DSS "merchant_id" or Grasshopper "Merchant's Customer ID"
                    foundMerchantIdInDrivingTable = true;
                    break;
                }
            }
            
            if (!foundMerchantIdInDrivingTable) {
                return null; // Ignore "merchant_id"; DSS-UI should not have included it in this query
            }
        }
        
        QueryTreeNode dateCondition = geCalculatedtDateCondition(restriction, id);
        if(dateCondition != null){
            return dateCondition;
        }
        
        if (colIdToAttrMapping.containsKey(id)) {
            return getSimpleConditionFromRestriction(id, restriction);
        }
        else if(getCalculatedColumns(id)!=null){
            return getComputedColumnConditionFromRestriction(id, restriction);
        }
        else{
            addMissedAttribute(id);
           writeToLog("column " + id + " not found in mapping!");
           return null;
        }
        
        
        
        
        
    }
    
    

    protected QueryTreeNode geCalculatedtDateCondition(Element restriction, String id) {
        if(id.equalsIgnoreCase("last_week")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node == null && hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))==null)
                System.out.println("Missing Area:" + currentRunData.areaId);
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                        new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                        new QueryConstant("'D'")}),
                    new QueryConstant("7") 
                }),
                
                new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                        new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                        new QueryConstant("'D'")}),
                    new QueryConstant("1") 
                }) 
            });
            return date;
        }
        else if(id.equalsIgnoreCase("last_7_days")){
            return getLast_N_DaysRestriction(7);
        }
        else if(id.equalsIgnoreCase("last_2_days")){
            return getLast_N_DaysRestriction(2);
        }
        else if(id.equalsIgnoreCase("last_31_days")){
            return getLast_N_DaysRestriction(31);
        }
        else if(id.equalsIgnoreCase("last_month")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node == null && hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))==null)
                System.out.println("Missing Area:" + currentRunData.areaId);
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                        new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                        new QueryConstant("'MM'")}),
                    new QueryConstant("-1")}),
                
                    new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                        new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                            new QueryConstant("'MM'")}),
                        new QueryConstant("1") 
                    }) 
            });
            return date;
        }
        else if(id.equalsIgnoreCase("last_n_days")){
            int n = Integer.parseInt(restriction.getAttributeValue("VALUES"));
            return getLast_N_DaysRestriction(n);
        }
        else if(id.equalsIgnoreCase("last_4_weeks")){
            return getLast_N_DaysRestriction(28);
        }
        
        else if(id.equalsIgnoreCase("yesterday")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node == null && hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))==null)
                System.out.println("Missing Area:" + currentRunData.areaId);
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}) });
            return date;
        }
        else if(id.equalsIgnoreCase("this_month_to_date")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                    new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                    new QueryConstant("'MM'")}),
                new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}) });
            return date;
        }
        else if(id.equalsIgnoreCase("rday_last_x_days")){
            return getLastXDays(restriction);
            /*  String value = restriction.getAttributeValue("VALUES");
            String[] values = value.split(",");
            if(values.length > 1){
                int n1 = Integer.parseInt(values[0].replace("'", ""));
                int n2 = Integer.parseInt(values[1].replace("'", ""));
                String max = Integer.toString(Math.max(n1, n2));
                String min = Integer.toString(Math.min(n1, n2));
                QueryTreeNode node = getDateQueryNodeByArea();
                if(node==null)
                    node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(areaId))));
                QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID()), new QueryTreeNode[] { node, 
                    new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID()), new QueryTreeNode[] { 
                        new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID()), new QueryTreeNode[] {}), 
                        new QueryConstant(max)}),
                        new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID()), new QueryTreeNode[] { 
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID()), new QueryTreeNode[] {}), 
                            new QueryConstant(min)}) });
                return date;
                
            }
            int n = Integer.parseInt(restriction.getAttributeValue("VALUES"));
            return getLast_N_DaysRestriction(n);*/
        }
        else if(id.equalsIgnoreCase("rday_calendar_day")){
            return getSimpleConditionFromRestriction(hashAreaActivityDay.get(currentRunData.areaId).longValue(), restriction);
            
        }
        else if(id.equalsIgnoreCase("last_month_last_year")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                        new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                        new QueryConstant("'MM'")}),
                    new QueryConstant("-13")}),
                
                    new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                        new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                            new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                                new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                                new QueryConstant("'MM'")}),
                            new QueryConstant("-12")}),
                        new QueryConstant("1") 
                    }) 
            });
            return date;
        }
        else if(id.equalsIgnoreCase("this_year_to_date")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                    new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                    new QueryConstant("'YYYY'")}),
                new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}) });
            return date;
 
        }
        else if(id.equalsIgnoreCase("this_week_to_date")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                    new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                    new QueryConstant("'D'")}),
                new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}) });
            return date;
        }
        else if(id.equalsIgnoreCase("last_week_last_year")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                        new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                            new QueryConstant("'D'")}),
                        new QueryConstant("-12")}),
                    new QueryConstant("7") 
                }),
                
                new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                        new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { 
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                            new QueryConstant("'D'")}),
                        new QueryConstant("-12")}),
                    new QueryConstant("1") 
                }) 
            });
            return date;
        }
        else if(id.equalsIgnoreCase("previous_n_days")){
            int n = Integer.parseInt(restriction.getAttributeValue("VALUES"));
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                            new QueryConstant("-12")}),
                    new QueryConstant(Integer.toString(n)) 
                }),
                
                new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                    new QueryConstant("-12")})
                     
            }) ;
            return date;
        }
        else if(id.equalsIgnoreCase("last_year_to_date")){
            QueryTreeNode node = getDateQueryNodeByArea();
            if(node==null)
                node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
            QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { node, 
                new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                            new QueryConstant("-12")}),
                    new QueryConstant("'YYYY'") 
                }),
                
                new QueryInternalNode(queryFunction(DSSUI_OP.ADD_MONTHS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
                    new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), 
                    new QueryConstant("-12")}) 
            });
            return date;
        }
        else if(id.equalsIgnoreCase("date")){
            return getSimpleConditionFromRestriction(hashAreaActivityDay.get(currentRunData.areaId).longValue(), restriction);
            
        }
        else
            return null;
    }
    
    private QueryInternalNode getLast_N_DaysRestriction(int n ){
        QueryTreeNode node = getDateQueryNodeByArea();
        if(node==null)
            node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
    
        QueryInternalNode date = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
            node,
            new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] 
                    { new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}), new QueryConstant(Integer.toString(n))} ),
            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {})
        });
        return date;
    }
    
    private QueryInternalNode getConditionExpression(Element queryRoot){
        if(queryRoot.getChild("AND")!=null){
            Element mainExpression = queryRoot.getChild("AND");
            if(mainExpression.getChildren().size() > 1){
                QueryInternalNode mainNode = new QueryInternalNode(queryFunction(DSSUI_OP.AND.getFunctionID(), DBType.SQL.getId()));
                getConditionExpression(mainNode, mainExpression);
                return mainNode;
            }
            else{
                writeToLog("main AND clause contains only one condition!");
            }
        }
        else if(queryRoot.getChild("OR")!=null){
            Element mainExpression = queryRoot.getChild("OR");
            if(mainExpression.getChildren().size() > 1){
                QueryInternalNode mainNode = new QueryInternalNode(queryFunction(DSSUI_OP.OR.getFunctionID(), DBType.SQL.getId()));
                getConditionExpression(mainNode, mainExpression);
                return mainNode;
            }
            else{
                writeToLog("main OR clause contains only one condition!");
            }
        }
        else{
            writeToLog("Main Element does not contain AND/OR element!");
        }
            
        QueryInternalNode conditionTree = new QueryInternalNode(queryFunction(3, DBType.SQL.getId()), new QueryTreeNode[] { new QueryConstant("1"), new QueryConstant("1")});
        return conditionTree;
    }
    
    private Area findArea(Element queryRoot){
        String desc = "";
        try{
            desc = queryRoot.getAttribute("DESC").getValue();
            for(Area area : getAllAreas()){
                if(area.getName().equalsIgnoreCase(desc))
                    return area;
            }
        }
        catch(Exception ex){}
        if(desc.equalsIgnoreCase("Financial Inventory Analysis (Beta Testing)"))
            return queryArea(15);
        else if(desc.equalsIgnoreCase("Vendor Receipt Analysis(Beta Testing)"))
            return queryArea(17);
        else if(desc.equalsIgnoreCase("Backlog(Beta testing)"))
            return queryArea(7);
        else {
            writeToLog("Unknown area: "+desc);
        }
        return null;
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
        if(id.equalsIgnoreCase("rday_calendar_day") || id.equalsIgnoreCase("date"))
            return id;
        

        if (colIdToAttrMapping.containsKey(id.replace(".", "_")))
            id = id.replace(".", "_");

        return id;

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
        MULTIPLY("MULTIPLY",13),
        TRUNC("TRUNC", 21),
        TO_CHAR("TO_CHAR",26),
        TO_DATE("TO_DATE",27),
        CASE("CASE", 29),
        IN("IN", 30),
        RUNDATE("RUNDATE", 31),
        TRUNC_DATE("TRUNC_DATE", 33),
        ADD_MONTHS("ADD_MONTHS", 34),
        TRIM("TRIM", 36),
        SIGN("SIGN", 43),
        DECODE("DECODE",44),
        LIKE_BEGINS_WITH("LIKE_BEGINS_WITH", 46);

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

 
    public void printMostMissedAttributes(){
        Set<String> missingAttrs = statistics.hashMostMissedAttributes.keySet();
        List<String> sortedMissing = new ArrayList<String>(missingAttrs);
        Collections.sort(sortedMissing, new Comparator<String>() {

            @Override
            public int compare(String arg0, String arg1) {
                long s1 = statistics.hashMostMissedAttributes.get(arg0).longValue();
                long s2 = statistics.hashMostMissedAttributes.get(arg1).longValue();
              
                if(s1 > s2)
                    return 1;
                if(s1 < s2)
                    return -1;
                return 0;
                
            }
        });
        for(String s : sortedMissing){
            System.out.println(s + "-" + statistics.hashMostMissedAttributes.get(s).toString());
        }
        System.out.println(statistics.hashMostMissedDateAttributes.toString());
    }
    
    
    /**
     * This unfortunately requires a similar but unique logic:
     * The attribute is actually a calculated attributes of type NUMERIC.
     * This value can be used with multiple functions.
     * TODO: Try to generalize it into getSimpleConditionFromRestriction
     * @param restriction
     * @return
     */
    private QueryTreeNode getLastXDays( Element restriction){
        
        QueryTreeNode node = getDateQueryNodeByArea();
        if(node==null)
            node =  new QueryAttribute( queryAttribute(hashAreaActivityDay.get(Long.valueOf(currentRunData.areaId))));
        
        QueryTreeNode attribute = new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {
            new QueryInternalNode(queryFunction(DSSUI_OP.MINUS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] 
                    {
                    node,
                    new QueryInternalNode(queryFunction(DSSUI_OP.TRUNC_DATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] 
                            {
                            new QueryInternalNode(queryFunction(DSSUI_OP.RUNDATE.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] {}),
                            new QueryConstant("'YYYY'") 
                            } )
                    })
            });
        
        String value = restriction.getAttributeValue("VALUES");
        String[] values = value.split(",");
        
     
        String opCodeStr = restriction.getAttributeValue("OP");
        

        // A simple EQUALS for non-dates.
        if(values.length == 1 && (opCodeStr==null || opCodeStr.equalsIgnoreCase("EQ"))){
            String s = values[0].replace("'", "");
            
            return new QueryInternalNode(queryFunction(DSSUI_OP.EQUALS.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attribute, new QueryConstant(s) } );
        }
        //RANGE 
        else if(opCodeStr!=null && opCodeStr.equalsIgnoreCase("RANGE") ){
            int n1 = Integer.parseInt(values[0].replace("'", ""));
            int n2 = Integer.parseInt(values[1].replace("'", ""));
            String max = Integer.toString(Math.max(n1, n2));
            String min = Integer.toString(Math.min(n1, n2));
            QueryInternalNode between = new QueryInternalNode(queryFunction(DSSUI_OP.BETWEEN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attribute,
                new QueryConstant(min),
                new QueryConstant(max)
            } );
            return between;
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("GTE") && values.length==1){
             String s = values[0].replace("'", "");
             return new QueryInternalNode(queryFunction(DSSUI_OP.GREATER_THAN_OR_EQUAL_TO.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attribute, new QueryConstant(s) } );
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("GT") && values.length==1){
            String s = values[0].replace("'", "");
            return new QueryInternalNode(queryFunction(DSSUI_OP.GREATER_THAN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attribute, new QueryConstant(s) } );
 
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("LTE") && values.length==1){
            String s = values[0].replace("'", "");
            return new QueryInternalNode(queryFunction(DSSUI_OP.LESS_THAN_OR_EQUAL_TO.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attribute, new QueryConstant(s) } );
 
        }
        else if(opCodeStr != null && opCodeStr.equalsIgnoreCase("LT") && values.length==1){
            String s = values[0].replace("'", "");
            return new QueryInternalNode(queryFunction(DSSUI_OP.LESS_THAN.getFunctionID(), DBType.SQL.getId()), new QueryTreeNode[] { attribute, new QueryConstant(s) } );
 
        }
        
        else{
            writeToLog("OpCodeStr: " + opCodeStr + " Number of values: "+ values.length);
            return null;
        }
    }
}
