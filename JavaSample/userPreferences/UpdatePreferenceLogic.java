package com.amazon.dw.grasshopper.userPreferences;

import com.amazon.dw.grasshopper.AddPreferenceInput;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * An interface for updating user preference.
 */
public interface UpdatePreferenceLogic {
    public JSONObject updateAndCalculatePreference(JSONObject internalData, AddPreferenceInput input) throws JSONException;
        
}
