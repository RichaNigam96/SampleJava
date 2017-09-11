package com.amazon.dw.grasshopper.attributesearchservice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 * Class to generate GrassHopper Queries.
 * @author mounicam
 *
 */

public class GHQGenerator {
    
    private static final String SORT_PARAMETER = "sortParameter";
    
   
    public GHQGenerator() {
    }

    /**
     * Generate list of GrassHopper queries 
     * @param CloudSearch results which contains list of subject areas with the closest
     *        attributes in each subject area.
     * @param Filter parameters 
     * @param Maps attribute id to its name and datatype.
     * @return List of GrassHopper queries.
     * @throws JSONException
     */
    public List<String> generateGHQ(JSONObject attributeSearchResults, JSONObject filterResults, 
            Map<String, JSONObject> attributeInfo) throws JSONException, ParseException {
        
        if (attributeSearchResults == null) {
            throw new GrassHopperQueryException("AttributeSearchResults argument cannot be null.");
        }
        if (filterResults == null || filterResults.size() == 0) {
            throw new GrassHopperQueryException("Filter parameters argument cannot be empty or null.");
        }
        if (attributeInfo == null) {
            throw new GrassHopperQueryException("Attrubute Information argument cannot be null.");
        }

        List<String> ghqList = new ArrayList<String>();
        JSONArray areaInfoList = (JSONArray) attributeSearchResults.get("areaInfoList");
        for (int i = 0; i < areaInfoList.size(); i++) {
            JSONObject area = (JSONObject) areaInfoList.get(i);
            JSONObject ghq = generateGHQForEachSubjectArea((String) area.get("areaId"), 
                    (ArrayList<String>) area.get("attributes"), filterResults, attributeInfo);
            ghqList.add(ghq.toString());
        }
        return ghqList;
    }

    /**
     * Generates GrassHopper query for each subject area. 
     * @param CloudSearch results which contains list of subject areas with the closest
     *        attributes in each subject area.
     * @param Filter parameters 
     * @param Maps attribute id to its name and datatype.
     * @return List of GrassHopper queries.
     * @throws JSONException
     */
    public JSONObject generateGHQForEachSubjectArea(String areaId, ArrayList<String> attributes, 
            JSONObject filterResults, Map<String, JSONObject> attributeInfo) throws JSONException, ParseException {

        if (areaId == null || areaId.isEmpty()) {
            throw new GrassHopperQueryException("Area Id argument cannot be empty or null.");
        }
        if (attributes == null || attributes.size() == 0) {
            throw new GrassHopperQueryException("Attributes argument cannot be empty or null.");
        }
        if (filterResults == null || filterResults.size() == 0) {
            throw new GrassHopperQueryException("Filter parameters argument cannot be empty or null.");
        }
        if (attributeInfo == null || attributeInfo.size() == 0) {
            throw new GrassHopperQueryException("Attrubute Information argument cannot be empty or null.");
        }
        
        GHQuery ghq = new GHQuery();
        ghq.addAreaId(areaId);
        ghq.addReportedAttributes(areaId, attributes, filterResults, attributeInfo);
        ghq.addConditionTree(filterResults);
        JSONObject sortObject = (JSONObject) filterResults.get("SortFilter");
        ghq.addlimitTopNRows(sortObject);
        JSONObject finalJSONObject = ghq.getGHQ();
        return finalJSONObject;

    }

    /**
     * Adds sort Filter parameters to the input JSON Objects.
     * @param CloudSearch results which contains list of subject areas with the closest
     *        attributes in each subject area.
     * @param sort filter parameters 
     * @param Maps attribute id to its name and datatype.
     * @return modified attributeSearchResults and sortFilter JSON Objects.
     * @throws JSONException
     */
    public JSONObject changeSortFilter(JSONObject attributeSearchResults, JSONObject sortFilter, 
            Map<String, Map<String, Double>> areaAttributeMap) throws JSONException {

        if (attributeSearchResults == null) {
            throw new GrassHopperQueryException("AttributeSearchResults argument cannot be null.");
        }
        if (sortFilter == null) {
            throw new GrassHopperQueryException("Sort Filter argument cannot be null.");
        }
        if (areaAttributeMap == null) {
            throw new GrassHopperQueryException("AreaAttributeMap argument cannot be null.");
        }
        
        if (sortFilter.containsKey(SORT_PARAMETER)) {

            JSONArray areaInfoList = (JSONArray) attributeSearchResults.get("areaInfoList");
            JSONArray sortParameters = new JSONArray();
            JSONArray areaInfoListNew = new JSONArray(); 
            for (int i = 0; i < areaInfoList.size(); i++) {
                JSONObject area = (JSONObject) areaInfoList.get(i);
                String areaName = (String) area.get("areaId");
                if (areaAttributeMap.containsKey(areaName))  {
                    JSONArray attributes = (JSONArray) area.get("attributes");
                    Map<String, Double> attrMap = areaAttributeMap.get(areaName);
                    String bestAttr = getHighestScoreAttribute(attrMap);
                    if (!attributes.contains(bestAttr)) {
                        attributes.add(bestAttr);
                    }
                    area.put("attributes", attributes);
                    sortParameters.add(bestAttr);   
                }
                areaInfoListNew.add(area);
            }
            sortFilter.put(SORT_PARAMETER, sortParameters);
            attributeSearchResults.put("areaInfoList", areaInfoList);

        }

        JSONObject finalObject = new JSONObject();
        finalObject.put("sort", sortFilter);
        finalObject.put("attributeSearch", attributeSearchResults);
        return finalObject;

    }
    

    /**
     * Returns the attribute with the largest score.
     * @param attributes
     * @return
     */
    private String getHighestScoreAttribute(Map<String, Double> attributes) {

        double maxScore = 0.0;
        String bestAttribute = null;
        for (Map.Entry<String, Double> entry : attributes.entrySet()) {
            double score = entry.getValue().doubleValue();
            if (score > maxScore) {
                maxScore = score;
                bestAttribute = entry.getKey();
            }
        }
        return bestAttribute;
    }

}

