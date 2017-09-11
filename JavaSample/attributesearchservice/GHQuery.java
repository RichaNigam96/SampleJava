package com.amazon.dw.grasshopper.attributesearchservice;

import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import org.json.simple.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * Class to construct one GHQ query.
 * @author mounicam
 */
public class GHQuery {

    private static final String SORT_PARAMETER = "sortParameter";
    private static final int FUNCTIONID_EQUALS = 3;
    private static final int FUNCTIONID_IN = 30;
    private static final int WEEK = 7;
    private static final int MONTH = -12;
    private static final String NUMBER = "number";
    private int sortNumber, dateFlag;
    private List<String> dataStrings = new ArrayList<String>();
    private JSONParser parser = new JSONParser();

    private JSONObject ghq;

    /**
     * Initializes the GrassHopper Query with default parameters.
     * @throws JSONException
     */
    public GHQuery() throws JSONException {

        ghq = new JSONObject();
        ghq.put("areaID", "");
        ghq.put("reportedAttributes", new JSONArray());
        ghq.put("conditionsTree", new JSONObject());
        ghq.put("limitTopNRows", "-1");
        ghq.put("metaDataVersion", "global");

    }

    /**
     * Modifies the areaID in the GHQ.
     * @param areaId
     * @throws JSONException
     * @throws GrassHopperQueryException
     */
    public void addAreaId(String areaId) throws JSONException {

        if (areaId == null) {
            throw new GrassHopperQueryException("Area Id argument cannot be null.");
        }
        ghq.put("areaID", areaId);
    }

    /**
     * Adds the reported attributes in the GrassHopper query.
     * @param areaId Subject area in the GHQ
     * @param attributes Attributes to be added to the GHQ
     * @param filterResults Filter parameters to be added to the GHQ
     *                         SortFilter and DateFilter parameters are needed.
     * @param attributeInfo Gives the name and data-type of each attribute.
     * @throws JSONException
     * @throws GrassHopperQueryException
     * @throws ParseException
     */
    public void addReportedAttributes(String areaId, ArrayList<String> attributes, JSONObject filterResults, 
            Map<String, JSONObject> attributeInfo) throws JSONException, ParseException {

        if (areaId == null || areaId.isEmpty()) {
            throw new GrassHopperQueryException("Area Id argument cannot be empty or null.");
        }
        if (attributes == null || attributes.size() == 0) {
            throw new GrassHopperQueryException("Attributes argument cannot be empty or null.");
        }
        if (filterResults == null || filterResults.size() == 0) {
            throw new GrassHopperQueryException("Filter parameters argument cannot be empty or null.");
        }
        if (attributeInfo == null || attributeInfo.isEmpty()) {
            throw new GrassHopperQueryException("Attrubute Information argument cannot be empty or null.");
        }

        JSONArray reportedAttributes = new JSONArray();
        for (int i = 0; i < attributes.size(); i++)  {
            String attrId = (String) attributes.get(i);
            JSONObject attrInfo = attributeInfo.get(attrId);
            JSONObject attrObject = getEachReportedAttribute(areaId, attrId, filterResults, attrInfo);
            reportedAttributes.add(attrObject);

            JSONObject dateFilter = (JSONObject) filterResults.get("DateFilter");
            if (isDateAttribute(attrInfo, dateFilter) && isDateWeekly(dateFilter)) {
                dateFlag = 1;
                attrObject = getEachReportedAttribute(areaId, attrId, filterResults, attrInfo);
                reportedAttributes.add(attrObject);
                dateFlag = 0;
            }

        }
        ghq.put("reportedAttributes", reportedAttributes);

    }

    /**
     * Adds the filter parameters to the GHQ.
     * @param filterResults Filter parameters to be added to the GHQ
     * @throws JSONException
     * @throws ParseException 
     * @throws GrassHopperQueryException
     */
    public void addConditionTree(JSONObject filterResults) throws JSONException, ParseException {

        if (filterResults == null || filterResults.size() == 0) {
            throw new GrassHopperQueryException("Filter parameters argument cannot be empty or null.");
        }

        JSONObject filterObject = new JSONObject();
        JSONArray andOperands = new JSONArray();
        JSONObject operand;

        JSONArray filterArray = (JSONArray) filterResults.get("MarketPlaceFilter");
        if (filterArray.size() > 0) {
            operand = getOtherFilters("296", filterArray);
            andOperands.add(operand);
        }

        filterArray = (JSONArray) filterResults.get("LegalEntityFilter");
        if (filterArray.size() > 0) {
            operand = getOtherFilters("1008", filterArray);
            andOperands.add(operand);
        }

        filterArray = (JSONArray) filterResults.get("RegionFilter");
        if (filterArray.size() > 0) {
            operand = getOtherFilters("1549", filterArray);
            andOperands.add(operand);
        }

        JSONObject dateFilter = (JSONObject) filterResults.get("DateFilter");
        if (dateFilter.size() > 0) {
            for (String date : dataStrings) {
                operand = (JSONObject) parser.parse(date);
                andOperands.add(operand);
            }
        }

        filterObject.put("children", andOperands);
        filterObject.put("functionId", 0);

        ghq.put("conditionsTree", filterObject);

    }

    /**
     * Add rowLimit to the GHQ.
     * @param sortObject
     * @throws JSONException
     * @throws GrassHopperQueryException
     */
    public void  addlimitTopNRows(JSONObject sortObject) throws JSONException {

        if (sortObject == null) {
            throw new GrassHopperQueryException("SortFilter argument cannot be null.");
        }
        ghq.put("limitTopNRows", getTopRows(sortObject));

    }

    /**
     * Add metadata version to the GHQ.
     * @param mdVersion
     * @throws JSONException
     * @throws GrassHopperQueryException
     */
    public void  addMetaDataVersion(String mdVersion) throws JSONException {

        if (mdVersion == null || mdVersion.isEmpty()) {
            throw new GrassHopperQueryException("MetaDataVersion argument cannot be empty or null.");
        }
        ghq.put("metaDataVersion", mdVersion);

    }

    /**
     * Gets the GHQuery.
     * @return GrassHopper query JSONObject.
     * @throws JSONException
     */
    public JSONObject getGHQ() throws JSONException {

        return ghq;
    }

    /**
     * Adds one attribute to the list of reported attributes.
     * @param areaId Subject area ID
     * @param attrId Attribute ID
     * @param Filter parameters to be added to the GHQ
     *        SortFilter and DateFilter parameters are needed.
     * @param attributeInfo Gives the name and data-type of each attribute.
     * @return JSONObject which gives name, and information of each reported
     *         attribute.
     * @throws JSONException
     * @throws ParseException
     */
    private JSONObject getEachReportedAttribute(String areaId, String attrId, JSONObject filterResults, 
            JSONObject attributeInfo) throws JSONException, ParseException {

        String finalAttributeString = "{";

        String attrName = (String) attributeInfo.get("name");        
        finalAttributeString += "\"syn\":\"" + attrName.trim() + "\",";

        JSONObject sortObject = (JSONObject) filterResults.get("SortFilter");
        if (isSortAttribute(attrId, sortObject)) {
            String sortString = "\"ordering\" : {\"order\":" + String.valueOf(sortNumber) 
                    + ", \"ascending\":true } , ";
            finalAttributeString += sortString;
            sortNumber++;
        }

        String idString = "{\"attributeId\":" + attrId + "}";
        String aggregatedRootString = "\"AggregatedQueryTreeNodeRoot\":" 
                + " {\"aggregateFunction\": -1 ," 
                + "\"root\":" + idString + "}";

        JSONObject dateObject = (JSONObject) filterResults.get("DateFilter");
        if (isDateAttribute(attributeInfo, dateObject)) {

            String duration = (String) dateObject.get("duration");
            if (duration.equals("W")) {
                duration += String.valueOf(dateFlag);
            }
            String constant = getConstantString(duration);
            String dateExpName = getDateExpName(duration);

            aggregatedRootString = "\"AggregatedQueryTreeNodeRoot\":{\"aggregateFunction\":-1," 
                    + "\"root\":{\"functionId\":26,\"children\":[" + idString 
                    + ",{\"constant\":\"" + constant + "\"}]},\"dateAttrId\":" + attrId 
                    + ",\"dateExpName\":\"" + dateExpName + "\"}";

            if (dateObject.containsKey(NUMBER)) {
                String number = (String) dateObject.get(NUMBER);
                dataStrings.add(getDateFilterString(attrId, number, duration));
            }

        }
        finalAttributeString += aggregatedRootString;
        finalAttributeString += "}";

        JSONObject attrInfo  = (JSONObject) parser.parse(finalAttributeString);
        return attrInfo;

    }

    /**
     * Checks if the attribute id is in the list of ids of sort filter parameters.
     * @param attrId Attribute ID
     * @param sortObject Sort Filter parameters
     * @return true/false based on if the attribute id is sort paramter or not.
     * @throws JSONException
     */
    private boolean isSortAttribute(String attrId, JSONObject sortObject) throws JSONException {

        if (sortObject.size() > 0 && sortObject.containsKey(SORT_PARAMETER)) {
            JSONArray sortParam = (JSONArray) sortObject.get(SORT_PARAMETER);
            if (sortParam.contains(attrId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the attribute is of data-type DATE
     * @param info
     * @param dateObject
     * @return
     * @throws JSONException
     */
    private boolean isDateAttribute(JSONObject info, JSONObject dateObject) throws JSONException {
        String type = String.valueOf(info.get("type"));
        if (type.equals("DATE") && dateObject.size() > 0) {
            return true;
        }
        return false;

    }

    private boolean isDateWeekly(JSONObject dateFilter) throws JSONException {
        String duration = (String) dateFilter.get("duration");
        if (dateFilter.containsKey("duration") && duration.equals("W")) {
            return true;
        }
        return false;
    }


    /**
     * Constructs "conditionsTree" object of GHQ for MarketPlace ID, Legal Entity
     * and Region ID filters.
     * @param attributeId
     * @param parameters
     * @return
     * @throws JSONException
     */
    private JSONObject getOtherFilters(String attributeId, JSONArray parameters) throws JSONException {

        JSONObject filterObject = new JSONObject();
        int function = FUNCTIONID_EQUALS;
        if (parameters.size() > 1) {   
            function = FUNCTIONID_IN;
        }
        filterObject.put("functionId", function);
        JSONArray operands = new JSONArray();
        JSONObject attribute = new JSONObject();
        attribute.put("attributeId", attributeId);
        operands.add(attribute);
        for (int i = 0; i < parameters.size(); i++) {
            JSONObject constant = new JSONObject();
            constant.put("constant", (String) parameters.get(i));
            operands.add(constant);
        }
        filterObject.put("children", operands);
        return filterObject;

    }

    /**
     * Returns the rowLimit for GHQ based on the sort filter parameters.
     * @param sortObject
     * @return
     * @throws JSONException
     */
    private String getTopRows(JSONObject sortObject) throws JSONException {

        if (sortObject.size() > 0 && sortObject.containsKey(NUMBER)) {
            return (String) sortObject.get(NUMBER);
        }
        return "-1";
    }

    /**
     * Return date parameters for GHQ.
     * @param duration
     * @return
     */
    public String getDateExpName(String duration) {

        String flagName;
        switch (duration) {
        case "W0":
            flagName = "Calendar Week Of Year";
            break;
        case "W1":
            flagName = "Week Start Date";
            break;
        case "Y":
            flagName = "Calendar Year";
            break;
        case "M":
            flagName = "YYYY-MM";
            break;
        case "YE":
            flagName = "YYYY-MM-DD";
            break;
        case "D":
            flagName = "YYYY-MM-DD";
            break;
        default: 
            flagName = "";
            break;
        }
        return flagName;
    }

    /**
     * Return date parameters for GHQ.
     * @param duration
     * @return
     */
    public String getConstantString(String duration) {

        String flagName;
        switch (duration) {
        case "W0":
            flagName = "'WW'";
            break;
        case "W1":
            flagName = "'D'";
            break;
        case "Y":
            flagName = "'YYYY'";
            break;
        case "M":
            flagName = "'YYYY-MM'";
            break;
        case "YE":
            flagName = "'YYYY-MM-DD'";
            break;
        case "D":
            flagName = "'YYYY-MM-DD'";
            break;
        default: 
            flagName = "";
            break;
        }
        return flagName;
    }

    private String getDateFilterString(String attributeId, String number, String duration) {

        String constantString;
        int n = Integer.parseInt(number);
        String firstString = "{\"functionId\":9,\"children\":[{\"attributeId\":" + attributeId;
        if (duration.equals("W0") || duration.equals("W1")) {
            constantString = String.valueOf(WEEK * n);
            return  firstString 
                    + "},{\"functionId\":12,\"children\":[{\"functionId\":33,\"children\":[{\"functionId\":31" 
                    + ",\"children\":[]},{\"constant\":\"'D'\"}]},{\"constant\":" + constantString 
                    + "}]},{\"functionId\":12,\"children\":[{\"functionId\":33,\"children\":[{\"functionId\":31" 
                    + ",\"children\":[]},{\"constant\":\"'D'\"}]},{\"constant\":\"1\"}]}]}";
        } else if (duration.equals("M")) {
            constantString = String.valueOf(-1 * n);
            return  firstString
                    + "},{\"functionId\":34,\"children\":[{\"functionId\":33,\"children\":[{\"function\":31" 
                    + ",\"children\":[]},{\"constant\":\"'MM'\"}]},{\"constant\":" + constantString 
                    + "}]},{\"functionId\":12,\"children\":[{\"functionId\":33,\"children\":[" 
                    + "{\"functionId\":31,\"children\":[]},{\"constant\":\"'MM'\"}]},{\"constant\":\"1\"}]}]}";
        } else if (duration.equals("Y")) {
            constantString = String.valueOf(MONTH * n);
            return firstString
                    + "},{\"functionId\":34,\"children\":[{\"functionId\":33,\"children\":" 
                    + "[{\"functionId\":31,\"children\":[]}," 
                    + "{\"constant\":\"'YYYY'\"}]},{\"constant\":" + constantString
                    + "}]},{\"functionId\":12,\"children\":[{\"functionId\":33,\"children\":[" 
                    + "{\"functionId\":31,\"children\":[]}," 
                    + "{\"constant\":\"'YYYY'\"}]},{\"constant\":\"1\"}]}]}";

        } else if (duration.equals("D")) {
            return firstString 
                    + "},{\"functionId\":12,\"children\":[{\"functionId\":31,\"children\":[]},{\"constant\":\"6\"}]}" 
                    + ",{\"functionId\":31,\"children\":[]}]}";
        } else if (duration.equals("YE")) {
            return "{\"functionId\":3,\"children\":[{\"attributeId\":" + attributeId 
                    + "},{\"functionId\":31,\"children\":[]}]}";
        } else {
            return "{}";
        }

    }

}
