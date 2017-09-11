package com.amazon.dw.grasshopper.attributesearchservice;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazon.dw.grasshopper.util.aws.CloudSearchQuery;


/**
 * Wrapper to query Cloud-Search domain and convert the cloud search results into a mapping.
 * @author mounicam
 *
 */
public class AttributeSearchWrapper {

    private CloudSearchQuery attributeQuery;

    public AttributeSearchWrapper(String endPoint, String apiVersion) {
        attributeQuery = new CloudSearchQuery(endPoint, apiVersion);
    }

    public AttributeSearchWrapper(CloudSearchQuery cquery) {
        attributeQuery = cquery;
    }


    /**
     *  Returns search results.
     *  @param String of search terms separated by spaces
     *  @return Map of subject area-ids, and the attributes in each area along with the corresponding scores.
     *  @throws IOException, CloudSearchException if there is an error in communicating with the cloud
     *  search service, or query syntax.
     */
    public Map<String, Map< String, Double>> searchQuery(String searchQuery)  throws IOException {

        JSONObject cloudSearchResults = null;
        NameValuePair[] parameters = new NameValuePair[3];
        parameters[0] = new NameValuePair("q", searchQuery);
        parameters[1] = new NameValuePair("q.options", "{ fields:['name^4', 'description^2', 'categories_names', 'areas_names'] }");
        parameters[2] = new NameValuePair("return", "attribute_id,name,areas_ids,_score");

        try {

            cloudSearchResults = attributeQuery.generalStringQuery(parameters);
            
            Map< String, Map< String, Double > > finalSearchResults = new HashMap< String, Map< String, Double > >();
            if(cloudSearchResults.has("hits") && cloudSearchResults.getJSONObject("hits").has("hit")) {
                JSONArray hits = cloudSearchResults.getJSONObject("hits").getJSONArray("hit");
                for(int i=0; i < hits.length(); i++) {
                    JSONObject attribute = hits.getJSONObject(i).getJSONObject("fields");
                    JSONArray areas_ids = attribute.getJSONArray("areas_ids");
                    
                    for(int j=0; j < areas_ids.length(); j++) {
                        String key = areas_ids.getString(j);
                        Map<String, Double> value;
                        if(finalSearchResults.containsKey(key)) {
                            value = getAttributeObject(attribute, finalSearchResults.get(key));
                        }
                        else {
                            value = getAttributeObject(attribute, null);
                        }
                        finalSearchResults.put(key, value);
                    }                  
                }
            }
            return finalSearchResults;

        } catch(HttpException exception) {
            throw new CloudSearchException("Error while retrieving data from cloud search", exception);    
        } catch(JSONException exception) {
            throw new CloudSearchException("Error reading JSON Object", exception);
        } catch(URISyntaxException exception) {
            throw new CloudSearchException("Error in the query syntax", exception);
        } 
 
    }

    private Map<String,Double> getAttributeObject(JSONObject attribute,  Map<String,Double> attributeList) throws JSONException {

        if(attributeList==null)
            attributeList = new HashMap<String,Double>();
        String key = attribute.getString("attribute_id");
        Double value = attribute.optDouble("_score");
        attributeList.put(key, value);
        return attributeList;

    }

}

