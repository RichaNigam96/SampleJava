package com.amazon.dw.grasshopper.tools.GHQModifiers;

import java.util.Iterator;

import com.amazon.dw.grasshopper.tools.ghqmodifiers.AbstractGHQModifier;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ReplaceAttributesModifier extends AbstractGHQModifier{

    public static final String SOURCE_ATTRIBUTE_PROPERTY_NAME = "ReplaceAttributesModifier.source";
    public static final String TARGET_ATTRIBUTE_PROPERTY_NAME = "ReplaceAttributesModifier.target";
    long attrIdOriginal;
    long attrIdTarget;
    
    public ReplaceAttributesModifier(){
        String src = System.getProperty(SOURCE_ATTRIBUTE_PROPERTY_NAME);
        String trg = System.getProperty(TARGET_ATTRIBUTE_PROPERTY_NAME);
        attrIdOriginal = Long.parseLong(src);
        attrIdTarget = Long.parseLong(trg);
    }
    
    private void recursivelyTraverse(JSONObject obj) throws JSONException{
        if(obj.has("attributeId"))
            handleAttribute(obj);
        else{
            if(obj.has("children")){//go into arrays
                JSONArray arr = obj.getJSONArray("children");
                for(int i=0;i<arr.length();i++){
                    JSONObject newObj = arr.getJSONObject(i);
                    recursivelyTraverse(newObj);    
                }
                
            }
            if(obj.has("reportedAttributes")){//go into arrays
                JSONArray arr = obj.getJSONArray("reportedAttributes");
                for(int i=0;i<arr.length();i++){
                    JSONObject newObj = arr.getJSONObject(i);
                    recursivelyTraverse(newObj);    
                }
                
            }
            if(obj.length() > 0){
                @SuppressWarnings("unchecked")
                Iterator<String> keys = obj.keys();
                while(keys.hasNext()){
                  String key = keys.next();
                  JSONObject newObj = obj.optJSONObject(key);
                  if(newObj != null)
                      recursivelyTraverse(newObj);
              }
            }
        }
    }
    
    private void handleAttribute(JSONObject obj) throws JSONException{
       if(obj.getLong("attributeId")==attrIdOriginal)
           obj.put("attributeId", Long.valueOf(attrIdTarget));
    }
    
    @Override 
    public JSONObject modify(JSONObject obj) {
        try{
            String copy = obj.toString();
            JSONObject copyObj = new JSONObject(copy);
            recursivelyTraverse(copyObj);
            return copyObj;
        }
        catch(JSONException ex){
            return obj;
        }
    }
    
    @Override
    public String modify(String ghq) {
        return ghq;
    }
}
