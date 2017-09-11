package com.amazon.dw.grasshopper.attributesearchservice;

import java.lang.Double;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.json.simple.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;

/**
 * Class to merge the attribute search results to
 * choose the subject area and closest attributes.
 * @author mounicam
 *
 */

public class AttributeCloudSearchOutputRanker {

    protected static class AreaInformation implements Comparable<AttributeCloudSearchOutputRanker.AreaInformation> {

        /**
         * Encapsulates the aggregate information for each subject area. 
         * Used while aggregating the cloud search results and ranking the subject areas.
         */

        private int subjectAreaCoverage;
        private double relevanceScore;
        private String areaId; 
        private List< String > bestAttributes; 
        AreaInformation(String area) {
            subjectAreaCoverage = 0;
            areaId = area;
            relevanceScore = 0.0;
            bestAttributes = new ArrayList<String>();
        }


        public void setCoverage(int coverage) {
            subjectAreaCoverage = coverage;
        }

        public void setScore(double score) {
            relevanceScore = score;
        }

        public void setBestAttributes(List< String > attributes) {
            for(String attr : attributes) {
                bestAttributes.add(attr);
            }
        }

        public void incrementScore(double score) {
            relevanceScore += score;
        }

        public void incrementCoverage(int coverage) {
            subjectAreaCoverage += coverage;
        }

        public void addAttribute(String attribute) {
            if(!bestAttributes.contains(attribute)) {
                bestAttributes.add(attribute);
            }
        }

        public double getScore() {
            return relevanceScore;
        }

        public int getCoverage() {
            return subjectAreaCoverage;
        }

        public String getAreaID() {
            return areaId;
        }

        public List<String> getBestAttributes() {
            return bestAttributes;
        }

        public JSONArray convertToJSON() {

            JSONArray attributeJSON = new JSONArray();
            for(String attribute : bestAttributes) {
                attributeJSON.add(attribute);
            }
            return attributeJSON;

        }

        @Override
        public int compareTo(AreaInformation arg0) {

            if (arg0 == null) {
                throw new CloudSearchException("compareTo argument cannot be null.");
            }

            if(subjectAreaCoverage < arg0.getCoverage() ) {
                return 1;
            } else if(arg0.getCoverage() == subjectAreaCoverage) {
                Double score1 = Double.valueOf(arg0.getScore());
                Double score2 = Double.valueOf(relevanceScore);
                return Double.compare(score1, score2);
            } else {
                return -1;
            }
        }

        @Override
        public boolean equals(Object arg0) {
           
            if (this == arg0) return true;
            if (!(arg0 instanceof AreaInformation)) return false;

            AreaInformation argAreaInfo = (AreaInformation)arg0;
            int coverage = argAreaInfo.getCoverage();
            Double score1 = Double.valueOf(argAreaInfo.getScore());
            Double score2 = Double.valueOf(relevanceScore);
            return
              (this.subjectAreaCoverage == coverage) &&
              (Double.compare(score1, score2) == 0);
        }
        
        @Override
        public int hashCode() {
            Double score = Double.valueOf(relevanceScore);
            return score.hashCode() + subjectAreaCoverage;
        }

    }

    private static int TOP_K_AREAS = 5;

    /**
     * To set the K parameter. K indicates the number of best subject areas to be displayed after aggregating the cloud search results.
     * @param K
     * @throws 
     */
    public static void setTopKAreas(int K) {
        TOP_K_AREAS = K;
    }

    public AttributeCloudSearchOutputRanker() {

    }

    /**
     * To aggregate cloud search results.
     * @param Map< search-term, Map< area ID, Map< attribute ID, relevance score > > > 
     * @return JSONObject with JSONArray containing top K areas along with their best attributes.
     * @throws JSONException, IllegalArgumentException
     */
    public JSONArray mergeCloudSearchResults(Map< String, Map< String, Map< String, Double > > > cloudSearchResults) throws JSONException, IllegalArgumentException {

        if (cloudSearchResults == null) {
            throw new IllegalArgumentException("The cloud search results data structure should not be null.");
        }

        List< AreaInformation > rankedSubjectAreas = rankSubjectAreas(cloudSearchResults);
        JSONArray subjectAreasWithAttributes = getFinalRankedResults(rankedSubjectAreas);
        return subjectAreasWithAttributes;

    }

    /**
     * Calculates the score of each subject area, and sorts them.
     * @param Map< search-term, Map< area ID, Map< attribute ID, relevance score > > > 
     * @return Ranked and sorted list of objects which store the aggregate information of each subject area.
     */
    @Nonnull
    public List<AreaInformation> rankSubjectAreas(
            Map<String, Map<String, Map<String, Double>>> cloudSearchResults) {

        String area = null;
        Map< String, AreaInformation > areaToRankMap = new HashMap< String, AreaInformation >() ;
        for (Map.Entry< String, Map< String, Map< String, Double > > > entryTerm : cloudSearchResults.entrySet()) {
            Map< String, Map< String, Double > > areaAttributeMapping = entryTerm.getValue();
            area = null;
            for(Map.Entry< String, Map< String, Double > > entryArea : areaAttributeMapping.entrySet()) {
                area = entryArea.getKey();
                Map< String, Double > attributes = entryArea.getValue(); 
                String bestAttribute = getHighestScoreAttribute(attributes);
                AreaInformation existingObject = null;
                if(areaToRankMap.containsKey(area)) {
                    existingObject = areaToRankMap.get(area);
                } 
                existingObject = getMappingObject(existingObject, area, attributes.get(bestAttribute), bestAttribute);
                areaToRankMap.put(area, existingObject);
            }
        }
        List< AreaInformation > areasList = new ArrayList< AreaInformation >(areaToRankMap.values());
        Collections.sort(areasList);
        return areasList;

    }

    /**
     * Creates the final ranked results object. 
     * @param List  of objects which store the aggregate information, score and best attributes of a subject area .
     * @return JSONArray containing top K areas along with their best attributes.
     * @throws JSONException
     */
    private JSONArray getFinalRankedResults(
            List<AreaInformation> rankedSubjectAreas) throws JSONException {

        int areaSize = rankedSubjectAreas.size();
        int K = TOP_K_AREAS < areaSize ? TOP_K_AREAS : areaSize;
        JSONArray finalList = new JSONArray();
        for(int i = 0; i < K ; i++) {

            AreaInformation areaInfo = rankedSubjectAreas.get(i);
            JSONObject areaJSON = new JSONObject();
            areaJSON.put("area_id", areaInfo.getAreaID());
            areaJSON.put("attributes", areaInfo.convertToJSON());
            finalList.add(areaJSON);

        }
        return finalList;
    }


    private AreaInformation getMappingObject(AreaInformation existingObject, String area , Double score, String attribute) {

        int coverage = 1;
        if(existingObject==null)
            existingObject = new AreaInformation(area);
        existingObject.incrementCoverage(coverage);
        existingObject.incrementScore(score);
        existingObject.addAttribute(attribute);
        return existingObject;
    }

    /**
     * Selects the attribute with largest score.   
     * @param Map of attribute IDs with their scores .
     * @return Best attribute ID.
     */
    private String getHighestScoreAttribute(Map< String, Double > attributes) {

        double maxScore = 0.0;
        String bestAttribute = null;
        for (Map.Entry< String, Double > entry : attributes.entrySet()) {
            double score = entry.getValue().doubleValue();
            if(score > maxScore) {
                maxScore = score;
                bestAttribute = entry.getKey();
            }
        }
        return bestAttribute;
    }

}
