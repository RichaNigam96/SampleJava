package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.FileSystemResource;

import com.amazon.coral.service.Identity;
import com.amazon.dw.grasshopper.CustomExpressionModel;
import com.amazon.dw.grasshopper.GetCustomExpressionsInput;
import com.amazon.dw.grasshopper.GetCustomExpressionsOutput;
import com.amazon.dw.grasshopper.customexpressions.CustomExpressionIndexingQueuer;
import com.amazon.dw.grasshopper.model.CustomExpression;
import com.amazon.dw.grasshopper.security.GrassHopperAuthenticationHandler;
import com.amazon.dw.grasshopper.userPreferences.UserPreferencesService;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.Sets;


/**
 * This driver is useful for re-indexing CustomExpressions.
 * For example, when the fields to index per Custom-Expression change.
 * This driver uses the UserPreferencesService (locally) to retrieve the CustomExpression.
 * It then uses the CustomExpressionIndexingQueuer to send item to SQS indexing queue.
 * @author srebrnik
 *
 */
public class CustomExpressionBackfillDriver {
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        
        String user = "";
        ArrayList<Long> cxIds = new ArrayList<Long>();
        boolean readFromDynamo = false;
        
        try{
            user = args[0];
            String[] vals = args[1].replace("\"","").trim().split(",");
            for(String s : vals){
                if(s.trim().matches("^[0-9]+$")){
                    cxIds.add(Long.valueOf(Long.parseLong(s.trim())));
                }
                else if(s.trim().matches("^all$")){
                    readFromDynamo = true;
                }
            }
        }
        catch(Exception ex){
            System.out.println("CXBackfill - re-sends CX to SQS queue to be indexed.");
            System.out.println("\tCXBackfill <user> <CX_ID>");
            System.out.println("\tCXBackfill <user> \"<LIST_OF_COMMA_SEPERATED_CX_ID>\"");
            System.out.println("\tCXBackfill <user> *");
            System.exit(0);
               
        }
        
        
        GenericApplicationContext ctx = new GenericApplicationContext();
        XmlBeanDefinitionReader xmlReader = new XmlBeanDefinitionReader(ctx);
        FileSystemResource resource = new FileSystemResource(new File("spring-configuration/CXBackfill.xml"));
        xmlReader.loadBeanDefinitions(resource);
        ctx.refresh();
        
        
        UserPreferencesService service = ctx.getBean(UserPreferencesService.class);
        CustomExpressionIndexingQueuer customExpressionIndexer = (CustomExpressionIndexingQueuer) ctx.getBean("CustomExpressionIndexer");
        
        if(readFromDynamo)
        {
        
            AmazonDynamoDB dynamoClient = ctx.getBean(com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient.class);
            
            ScanRequest scanRequest = new ScanRequest();
            scanRequest.setTableName("CustomExpressionsHistory");
            
            ScanResult scanResult = dynamoClient.scan(scanRequest);
            System.out.println("Num of Results: " +scanResult.getCount());
            
            for(Map<String, AttributeValue> item : scanResult.getItems()){
                Integer value = Integer.valueOf(item.get("CustomExpressionID").getN());
                cxIds.add(Long.valueOf(value));
                System.out.println("Adding id " + item.get("CustomExpressionID").getN());
            }
        }
        Identity identity = new Identity();
        identity.setAttribute(GrassHopperAuthenticationHandler.USERNAME_ATTRIBUTE, user);
        service.setIdentity(identity);
        System.out.println("Set Identity to " + user);
        
        
        try{
            GetCustomExpressionsInput input = new GetCustomExpressionsInput();
            input.setCustomExpressionIDS(cxIds);
            GetCustomExpressionsOutput output =  service.getCustomExpressions(input);
            for(CustomExpressionModel model : output.getCustomExpressionsList()){
                CustomExpression cx = new CustomExpression(model.getCustomExpressionId(), model.getGhq(), model.getName(), model.getDescription(), CustomExpression.parseCreationDateString(model.getCreationDate()), model.getCreatedBy(), 1, "");
                if(customExpressionIndexer.addElementToIndexingQueue(cx))
                    System.out.println("Successfully inserted cx " + model.getCustomExpressionId() + " to SQS for indexing.");
                else
                    System.out.println("Failed inserting cx " + model.getCustomExpressionId() + " to SQS for indexing.");
            }
         }
         catch(Exception ex){
             ex.printStackTrace();
        }
        System.exit(0);
  }
}

