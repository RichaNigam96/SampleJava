package com.amazon.dw.grasshopper.tools.GHQModifiers;

import java.util.Iterator;

import com.amazon.dw.grasshopper.tools.ghqmodifiers.AbstractGHQModifier;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This GHQModifier class should change constant GHQ elements to be of String type. i.e.:
 * { "constant" : 1 } -> { "constant" : "1" }
 */
public class MakeConstantStringModifier extends AbstractGHQModifier {

    public MakeConstantStringModifier(){
        
    }
    
    
    private void recursivelyTraverse(JSONObject obj) throws JSONException{
        if(obj.has("constant"))
            handleConstant(obj);
        else{
            if(obj.has("children")){//go into arrays
                JSONArray arr = obj.getJSONArray("children");
                for(int i=0;i<arr.length();i++){
                    JSONObject newObj = arr.getJSONObject(i);
                    recursivelyTraverse(newObj);    
                }
                
            }
            else if(obj.has("reportedAttributes")){//go into arrays
                JSONArray arr = obj.getJSONArray("reportedAttributes");
                for(int i=0;i<arr.length();i++){
                    JSONObject newObj = arr.getJSONObject(i);
                    recursivelyTraverse(newObj);    
                }
                
            }
            else{ // go to every child that is a JSONObject
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
    }
    
    private void handleConstant(JSONObject obj) throws JSONException{
        if(obj.opt("constant") instanceof String == false)
        {
            if(obj.opt("constant") instanceof Double || obj.opt("constant") instanceof Float){
                double val = obj.getDouble("constant");
                obj.remove("constant");
                obj.put("constant", Double.toString(val));
            }
            else{
                long val = obj.getLong("constant");
                obj.remove("constant");
                obj.put("constant", Long.toString(val));
            }
                
        }
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
