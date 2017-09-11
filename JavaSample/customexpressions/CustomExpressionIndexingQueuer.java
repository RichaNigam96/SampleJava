package com.amazon.dw.grasshopper.customexpressions;

import org.apache.log4j.Logger;

import com.amazon.dw.grasshopper.model.CustomExpression;
import com.amazon.dw.grasshopper.util.BaseIndexingQueuer;
import com.amazon.dw.grasshopper.util.aws.AmazonSQSHelper;
import org.json.JSONException;
import org.json.JSONObject;

public class CustomExpressionIndexingQueuer extends BaseIndexingQueuer<CustomExpression>{
    private static final Logger LOGGER = Logger.getLogger(CustomExpressionIndexingQueuer.class);
    
    
    public CustomExpressionIndexingQueuer(AmazonSQSHelper helper, String queue){
        setSqsHelper(helper);
        setQueueName(queue);
    }
    
    @Override
    protected JSONObject createFieldsJSON(final CustomExpression obj){
        JSONObject retval = new JSONObject();
        try{
            CustomExpression cx = (CustomExpression)obj;
            retval.put("name", cx.getName());
            retval.put("description", cx.getDescrption());
            retval.put("createdby", cx.getCreatedBy());
            retval.put("createddate", CustomExpression.getCreationDateString(cx.getCreationDate()));
            retval.put("metadataversion", cx.getMetadataVersion());
        }
        catch(JSONException ex){
            LOGGER.error(ex);
        }
        return retval;
    }

    @Override
    protected String createIDField(final CustomExpression obj) {
       return Long.toString(obj.getExpressionID());
    }
    
    @Override
    protected int createVersionField(CustomExpression obj) {
        return (int)Math.min(obj.getRevision(), (long)Integer.MAX_VALUE);
    }
}
