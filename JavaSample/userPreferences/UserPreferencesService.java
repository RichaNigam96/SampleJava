package com.amazon.dw.grasshopper.userPreferences;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.security.NotAuthorizedException;
import com.amazon.coral.service.Activity;
import com.amazon.coral.service.Identity;
import com.amazon.coral.validate.Validated;

import com.amazon.dw.grasshopper.GetCustomExpressionInput;
import com.amazon.dw.grasshopper.GetPopularAreasForManagerInput;
import com.amazon.dw.grasshopper.GetPopularAreasForManagerOutput;
import com.amazon.dw.grasshopper.GetUserPreferenceInput;
import com.amazon.dw.grasshopper.SubjectArea;
import com.amazon.dw.grasshopper.UserPreferenceServiceException;
import com.amazon.dw.grasshopper.GetUserPreferenceOutput;
import com.amazon.dw.grasshopper.AddPreferenceInput;
import com.amazon.dw.grasshopper.CustomExpressionModel;
import com.amazon.dw.grasshopper.GetCustomExpressionsOutput;
import com.amazon.dw.grasshopper.GetCustomExpressionsInput;
import com.amazon.dw.grasshopper.GetCustomExpressionHistoryInput;
import com.amazon.dw.grasshopper.customexpressions.CustomExpressionDAO;
import com.amazon.dw.grasshopper.customexpressions.CustomExpressionIndexingQueuer;
import com.amazon.dw.grasshopper.model.CustomExpression;
import com.amazon.dw.grasshopper.model.UserPreferenceListType;
import com.amazon.dw.grasshopper.model.UserPreferenceType;
import com.amazon.dw.grasshopper.orgchart.OrgChartPopularAreaManager;
import com.amazon.dw.grasshopper.security.DWPSecurityServiceCaller;
import com.amazon.dw.grasshopper.security.GrassHopperAuthenticationHandler;
import com.amazon.dw.grasshopper.tools.ImmutablePair;
import com.amazon.dw.grasshopper.util.aws.DynamoGetCallable;
import org.json.JSONArray;
import org.json.JSONObject;
import com.amazon.dw.grasshopper.util.RetryMetric;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;


@Service("UserPreferencesService")
public class UserPreferencesService extends Activity {

    private static Logger LOGGER = Logger.getLogger(UserPreferencesService.class);
        
    public static final String USER_PREFERENCES_TABLE = "UserPreferences";
    public static final String USER_PREFERENCES_PK_NAME = "Username";
    public static final String USER_PREFERENCES_DATA = "Data";
  
    private static final int NUM_OF_RETRIES = 3;
    private static final long SLEEP_TIME = 100;
    private static final int DYNAMO_MAX_SIZE = 64000;
    private static final long DEFAULT_POPULAR_AREAS_NUM_RESULTS = 5;
    
    public static final String USER_PREFERENCE_DYNAMO_FAILURES = "UserPreferencesDynamoDBFailures";
    public static final String USER_PREFERENCE_DYNAMO_LATENCY = "UserPreferencesDynamoDBLatency";
    public static final String USER_PREFERENCE_S3_FAILURES = "UserPreferencesS3Failures";
    public static final String USER_PREFERENCE_S3_LATENCY = "UserPreferencesS3Latency";
     
    //Spring initialized members;
    private UserPreferencesUpdater userPreferenceUpdater;
    private AmazonDynamoDB dynamoClient;
    private DWPSecurityServiceCaller callDWPSecurityService;
    private CustomExpressionIndexingQueuer customExpressionIndexer;
    private CustomExpressionIndexingQueuer customExpressionDeIndexer;
    private CustomExpressionDAO customExpressionDao;
    private OrgChartPopularAreaManager popularAreaManager;
    
    
    public void setUserPreferenceUpdater(
            UserPreferencesUpdater userPreferenceUpdater) {
        this.userPreferenceUpdater = userPreferenceUpdater;
    }

   public AmazonDynamoDB getDynamoClient() {
        return dynamoClient;
    }

    public void setDynamoClient(AmazonDynamoDB dynamoClient) {
        this.dynamoClient = dynamoClient;
    }

    public DWPSecurityServiceCaller getCallDWPSecurityService() {
        return callDWPSecurityService;
    }

    public void setCallDWPSecurityService(
            DWPSecurityServiceCaller callDWPSecurityService) {
        this.callDWPSecurityService = callDWPSecurityService;
    }

    public void setCustomExpressionIndexer(
            CustomExpressionIndexingQueuer customExpressionIndexer) {
        this.customExpressionIndexer = customExpressionIndexer;
    }

    public void setCustomExpressionDeIndexer(
            CustomExpressionIndexingQueuer customExpressionIndexer) {
        this.customExpressionDeIndexer = customExpressionIndexer;
    }

    public void setCustomExpressionDao(CustomExpressionDAO customExpressionDao) {
        this.customExpressionDao = customExpressionDao;
    }
    
    public void setPopularAreaManager(OrgChartPopularAreaManager popularAreaManager) {
        this.popularAreaManager = popularAreaManager;
    }
    
    @Validated
    @Operation("getPreferenceIds")
    public GetUserPreferenceOutput getPreference(GetUserPreferenceInput input) throws UserPreferenceServiceException{
        try{
            if(input.getUserGroupName()==null || input.getUserGroupName().trim().length() == 0)
                throw new IllegalArgumentException("User/Group name is empty.");
            UserPreferenceType type = UserPreferenceType.parse(input.getPreferenceType());
            if(type==null)
                throw new IllegalArgumentException("Illegal/Unknown User-Preference-Type used.");
            
            validateAuthentication();
            Identity identity = getIdentity();
            String username = identity.getAttribute(GrassHopperAuthenticationHandler.USERNAME_ATTRIBUTE);
            String groupName = input.getUserGroupName().toLowerCase().trim();
            if(hasPermissionToGroup(username, groupName)==false){
                throw new NotAuthorizedException("User "+username+ " is not authorized for group "+ groupName);
            }
            String pk = generatePK_user(groupName, type);
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("getPreference calling Dynamo with PK:"+pk);
            
            Map<String, AttributeValue> hashKey = new HashMap<String, AttributeValue>();
            hashKey.put(USER_PREFERENCES_PK_NAME, new AttributeValue().withS(pk));
            
            GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(USER_PREFERENCES_TABLE)
                .withAttributesToGet(USER_PREFERENCES_DATA)
                .withConsistentRead(true)
                .withKey(hashKey);
            
            RetryMetric<GetItemResult> retry = new RetryMetric<GetItemResult>(LOGGER, getMetrics(), USER_PREFERENCE_DYNAMO_FAILURES, USER_PREFERENCE_DYNAMO_LATENCY, 
                                new DynamoGetCallable(dynamoClient, getItemRequest), NUM_OF_RETRIES, SLEEP_TIME);
            
            GetItemResult result = null;
            
            result = retry.call();
            
            GetUserPreferenceOutput retval = new GetUserPreferenceOutput();
            if(result.getItem()==null){
                retval.setPreferenceIdList(new ArrayList<Long>());
                retval.setJSONData("");
                return retval;
            }
            
            if(type.getListType() == UserPreferenceListType.NONE) {
                retval.setJSONData(result.getItem().get(USER_PREFERENCES_DATA).getS());
            } else {
                JSONObject obj = new JSONObject(result.getItem().get(USER_PREFERENCES_DATA).getS());
                ArrayList<Long> list = new ArrayList<Long>();
                JSONArray arr = obj.getJSONArray("list");
                for(int i=0;i<arr.length();i++){
                    list.add(Long.parseLong(arr.getString(i)));
                }
               
                retval.setPreferenceIdList(list);
            }
            
            return retval;
        }
        catch(Exception ex){
            LOGGER.error("Error getting user-preferences.", ex);
            throw new UserPreferenceServiceException("Error getting user-preferences.", ex);
        }
        
    }
    
    @Validated
    @Operation("addPreference")
    /**
     * Adds the AddPreferenceInput to the User-Preference-Updater queue.
     * @param input
     * @throws UserPreferenceServiceException
     */
    public void addPreference(AddPreferenceInput input) throws UserPreferenceServiceException{
        try{
            if(input.getUserGroupName()==null || input.getUserGroupName().trim().length() == 0)
                throw new IllegalArgumentException("User/Group name is empty.");
            UserPreferenceType type = UserPreferenceType.parse(input.getPreferenceType());
            if(type==null)
                throw new IllegalArgumentException("Illegal/Unknown User-Preference-Type used.");
            validateAuthentication();
            
            if(type.getListType() == UserPreferenceListType.NONE &&
               input.getJSONData().getBytes("UTF-8").length > DYNAMO_MAX_SIZE) {
                    throw new UserPreferenceServiceException("JSON data is too large. Maximum size is 64KB");
            }
            
            if(type.getListType() == UserPreferenceListType.NONE ||
                    (input.getPreferenceIdList()!=null && input.getPreferenceIdList().size() > 0)) {
                userPreferenceUpdater.addPreferenceToHandle(input);
            }
        }
        catch(Exception ex){
            LOGGER.error("Failed adding preference.", ex);
            throw new UserPreferenceServiceException("Failed adding preference.", ex);
        }
    }
    
    @Operation("deleteCustomExpressionIndex")
    public void deleteCustomExpressionIndex(GetCustomExpressionInput input)
            throws UserPreferenceServiceException {
        ArrayList<Long> failedCustomExpressions = new ArrayList<Long>();
        try {
            validateAuthentication();
            List<Long> listOfIds = new ArrayList<Long>();
            listOfIds.add(input.getCustomExpressionID());

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("About to get custom-expressions for :"
                        + listOfIds.toString());

            ArrayList<CustomExpression> retrievedCXList = new ArrayList<CustomExpression>();
            customExpressionDao.getCustomExpressions(listOfIds,
                    retrievedCXList, failedCustomExpressions, getMetrics());

            // ensure success for get
            if (retrievedCXList != null && retrievedCXList.size() != 1) {
                LOGGER.error("Failed getting custom-expression: "
                        + input.getCustomExpressionID());
                throw new UserPreferenceServiceException(
                        "Failed getting custom-expression: "
                                + input.getCustomExpressionID());
            }

            // update record
            CustomExpression retrievedCX = retrievedCXList.get(0);
            CustomExpressionModel cxModel = retrievedCX.toModel();
            cxModel.setCreatedBy(getUser());
            cxModel.setName("Deleted - " + cxModel.getName());
            cxModel.setDescription("Deleted - " + cxModel.getDescription());
            cxModel.setDeleted(true);
            CustomExpression updatedExpression = customExpressionDao
                    .updateCustomExpression(cxModel, getMetrics());

            // add to deindex queue
            if (!customExpressionDeIndexer
                    .addElementToDeIndexingQueue(updatedExpression)) {
                throw new UserPreferenceServiceException(
                        "Unable to add custom expresstion to de-index queue.");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to delete custom-expression index.", ex);
            throw new UserPreferenceServiceException(
                    "Failed to delete custom-expression index: "
                            + ex.getMessage(), ex);
        }

    }

    @Operation("getCustomExpressions")
    /**
     * For each customExpressionId tries to get it from DB,
     * @param input
     * @return
     * @throws UserPreferenceServiceException
     */
    public GetCustomExpressionsOutput getCustomExpressions(GetCustomExpressionsInput input) throws UserPreferenceServiceException{
        GetCustomExpressionsOutput retval = new GetCustomExpressionsOutput();
        ArrayList<CustomExpressionModel> expressions = new ArrayList<CustomExpressionModel>();
        ArrayList<Long> failedCustomExpressions = new ArrayList<Long>();
        try{
            List<Long> lists = input.getCustomExpressionIDS();
            
            if(lists == null){
                retval.setCustomExpressionsList(new ArrayList<CustomExpressionModel>());
                retval.setFailedCustomExpressions(new ArrayList<Long>());
                return retval;
            }
            
            if(LOGGER.isDebugEnabled())
                LOGGER.debug("About to get custom-expressions for :"+lists.toString());
            
            ArrayList<CustomExpression> retrievedCX = new ArrayList<CustomExpression>();
            customExpressionDao.getCustomExpressions(lists, retrievedCX, failedCustomExpressions, getMetrics());
            
            for(CustomExpression expr :  retrievedCX)
            {
                expressions.add(expr.toModel());
            }
        }
        catch(Exception ex){
            LOGGER.error("Failed getting custom-expression.", ex);
            throw new UserPreferenceServiceException("Failed getting custom-expression.", ex);
        }
        
       retval.setCustomExpressionsList(expressions);
       retval.setFailedCustomExpressions(failedCustomExpressions);
       return retval;
    }
    
    @Operation("addCustomExpression")
    /**
     * Todo: add to local cache too.
     * Gets a new unique PK
     * Calls S3 to save file with serialized data.
     * Calls DynamoDB to store CustomExpressionDetails record.
     * @param input
     * @return
     * @throws UserPreferenceServiceException
     */
    public CustomExpressionModel addCustomExpression(CustomExpressionModel input) throws UserPreferenceServiceException{
        try{
            
            validateAuthentication();
            input.setCreatedBy(getUser()); // we don't trust the value that is set in "CreatedBy" and we set it ourself.
            CustomExpression newCustomExpression = customExpressionDao.addCustomExpression(input, getMetrics());
            
            customExpressionIndexer.addElementToIndexingQueue(newCustomExpression);
            
            input.setCustomExpressionId(newCustomExpression.getExpressionID());
            input.setCreationDate(CustomExpression.getCreationDateString(newCustomExpression.getCreationDate()));
            input.setCreatedBy(newCustomExpression.getCreatedBy());
            input.setExpressionRevision(newCustomExpression.getRevision());
        }
        catch(Exception ex){
            LOGGER.error("Failed adding custom expression.", ex);
            throw new UserPreferenceServiceException("Failed adding custom expression.", ex);
        }
        return input;
    }
    
    @Operation("updateCustomExpression")
    public CustomExpressionModel updateCustomExpression(CustomExpressionModel input) throws UserPreferenceServiceException{
        try{
            validateAuthentication();
            
            input.setCreatedBy(getUser()); // we don't trust the value that is set in "CreatedBy" and we set it ourself.
            CustomExpression updatedExpression = customExpressionDao.updateCustomExpression(input, getMetrics());
            customExpressionIndexer.addElementToIndexingQueue(updatedExpression);
            
            input.setCustomExpressionId(updatedExpression.getExpressionID());
            input.setCreationDate(CustomExpression.getCreationDateString(updatedExpression.getCreationDate()));
            input.setCreatedBy(updatedExpression.getCreatedBy());
            input.setExpressionRevision(updatedExpression.getRevision());
        }
        catch(Exception ex){
            LOGGER.error("Failed updating custom expression.", ex);
            throw new UserPreferenceServiceException("Failed updating custom expression.", ex);
        }
        return input;
    }
    
    @Operation("getCustomExpressionHistory")
    public GetCustomExpressionsOutput getCustomExpressionHistory(GetCustomExpressionHistoryInput input) throws UserPreferenceServiceException{
        GetCustomExpressionsOutput retval = new GetCustomExpressionsOutput();
        ArrayList<CustomExpressionModel> expressions = new ArrayList<CustomExpressionModel>();
        ArrayList<Long> failedCustomExpressions = new ArrayList<Long>();
       
        try{
            Collection<CustomExpression> revisions = customExpressionDao.getCustomExpressionHistory(input.getCustomExpressionID(), getMetrics());
            for(CustomExpression expr : revisions){
                expressions.add(expr.toModel());
            }
            retval.setCustomExpressionsList(expressions);
            retval.setFailedCustomExpressions(failedCustomExpressions);
        }
        catch(Exception ex){
            LOGGER.error("Failed retrieving custom expression history for CX -"+input.getCustomExpressionID(), ex);
            throw new UserPreferenceServiceException("Failed retrieving custom expression history", ex);
        }
        return retval;
    }
    
    
    @Operation("getPopularAreasForManager")
    @Validated
    public GetPopularAreasForManagerOutput getPopularAreasForManager(GetPopularAreasForManagerInput input) throws UserPreferenceServiceException {
        GetPopularAreasForManagerOutput output = new GetPopularAreasForManagerOutput();
        String mgrLogin = input.getManagerLogin();
        long limit = input.getMaxResults();
        List<String> mdVersionList = input.getFilterByMDVersions();
        List<ImmutablePair<String, Long>> subjectAreaPairs;
        
        // If no limit (or an invalid limit) is provided, use the default value
        if(limit <= 0) {
            limit = DEFAULT_POPULAR_AREAS_NUM_RESULTS;
        }
        
        // If the popularAreaManager was never initialized, throw an exception
        if(popularAreaManager == null) {
            throw new UserPreferenceServiceException("No data available for subject area popularity at this time");
        }
        
        // Get the popular subject areas for the given input
        if(mdVersionList == null) {
            subjectAreaPairs = popularAreaManager.getPopularAreasForManager(mgrLogin, limit);
        } else {
            Set<String> mdVersionSet = new HashSet<String>();
            for(String mdVersion : mdVersionList) {
                mdVersionSet.add(mdVersion);
            }
            
            subjectAreaPairs = popularAreaManager.getPopularAreasForManager(mgrLogin, limit, mdVersionSet);
        }
        
        // Copy the popular subject areas to the proper format
        List<SubjectArea> subjectAreas = new LinkedList<SubjectArea>();
        for(ImmutablePair<String, Long> pair : subjectAreaPairs) {
            SubjectArea area = new SubjectArea();
            area.setMdVersion(pair.getLeft());
            area.setAreaId(pair.getRight());
            
            subjectAreas.add(area);
        }
        
        output.setAreas(subjectAreas);
        return output;
    }
    
    public static String generatePK_user(String groupUserName, UserPreferenceType type) {
       return groupUserName+"_"+type.getPreferenceTypeName();
    }
    
     
    //verifies that the groupName is a group to which the user has permissions, or it's actually
    // the username.
    private boolean hasPermissionToGroup(String username, String groupname){
        if(username.equalsIgnoreCase(groupname))
            return true;
        Collection<String> groups = callDWPSecurityService.getGroupsForUser(username).values();
        for(String s : groups){
            if(s.equalsIgnoreCase(groupname))
                return true;
        }
        return false;
    }
    
    
    private void validateAuthentication() throws NotAuthorizedException{
        String username = getUser();
        if(username==null ){
            throw new NotAuthorizedException("user is not authorized");
        }
    }
    
    private String getUser(){
        Identity identity = getIdentity();
        return identity.getAttribute(GrassHopperAuthenticationHandler.USERNAME_ATTRIBUTE);
    }
}
