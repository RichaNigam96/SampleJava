package com.amazon.dw.grasshopper.userPreferences;

import java.util.Comparator;
import java.util.PriorityQueue;

import com.amazon.dw.grasshopper.AddPreferenceInput;
import com.amazon.dw.grasshopper.model.UserPreferenceListType;
import com.amazon.dw.grasshopper.model.UserPreferenceType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The basic implementation for updating a user preference.
 * The work is mainly done using the JSONObject method.
 * In order to sort in descending order, it uses a priority queue and defines local class and Comparator.
 */
public class UpdatePreferenceDescendingOrderLogic implements UpdatePreferenceLogic {
    /**
     * Updated the Internal_Data attribute of the user-preference according to the data sent by the user.
     * @param internalData
     * @param input
     * @throws JSONException
     */
    void updateInternalDataJSON(JSONObject internalData, AddPreferenceInput input) throws JSONException{
        UserPreferenceType type = UserPreferenceType.parse(input.getPreferenceType());
        if(type.getListType() == UserPreferenceListType.FREQUENTLY_USED_LIST){
            for(Long idLong : input.getPreferenceIdList()){
                String s = idLong.toString();
                long oldValue = internalData.optInt(s, 0);
                internalData.put(s, oldValue+1);
            }
        } else if(type.getListType() == UserPreferenceListType.RECENTLY_USED_LIST) {
            for(Long idLong : input.getPreferenceIdList()){
                String s = idLong.toString();
                internalData.put(s, System.currentTimeMillis());
            }
        }
    }
    
    /**
     * Calculates the new "ready-for-use" value of the user-preference.
     * Sorts the preferences (in descending order) and returns the first K elements (where K is defined by configuration).
     * @param internalData
     * @return
     * @throws JSONException
     */
    JSONObject calculateNewUserPreference(JSONObject internalData) throws JSONException{
       
        //define temp class for sorting.
        class Entry{
            public String key;
            public Long value;
            public Entry(String k, Long v){
                key = k; value = v;
            }
        };
        
        //define temp class for comparator
        class EntryComparator implements Comparator<Entry>{

            @Override
            public int compare(Entry arg0, Entry arg1) {
              //we want a descending order, therefore:
                return arg1.value.compareTo(arg0.value);
            }
            
        };
        
        //loads existing data to priority-queue (i.e. sort)
        PriorityQueue<Entry> heap = new PriorityQueue<Entry>(JSONObject.getNames(internalData).length, new EntryComparator());
        for(String key : JSONObject.getNames(internalData)){
            Long value = Long.valueOf(internalData.optLong(key, 0));
            heap.add(new Entry(key, value));
        }
        
        // returns a JSON with the biggest K values.
        JSONObject retval = new JSONObject();
        JSONArray arr = new JSONArray();
        int counter = 0;
        while(counter < UserPreferencesConfig.getInstance().getUserPreferenceListSize() && heap.peek()!=null){
            Entry entry = heap.poll();
            retval.put(entry.key, entry.value);
            arr.put(entry.key);
            counter++;
        }
        
        retval.put("list", arr);
        
        return retval;
        
    }
    
    /**
     * The Orchestrator for calculating the UpdatePreference Logic.
     * @param internalData
     * @param input
     * @return
     * @throws JSONException
     */
    @Override
    public JSONObject updateAndCalculatePreference(JSONObject internalData, AddPreferenceInput input) throws JSONException {
        UserPreferenceType type = UserPreferenceType.parse(input.getPreferenceType());
        if(type.getListType() == UserPreferenceListType.NONE) {
            return new JSONObject(input.getJSONData());
        } else {
            updateInternalDataJSON(internalData, input);
            return calculateNewUserPreference(internalData);
        }
    }
    
}
