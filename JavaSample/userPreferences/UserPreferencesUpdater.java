package com.amazon.dw.grasshopper.userPreferences;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.dw.grasshopper.AddPreferenceInput;
import com.amazon.dw.grasshopper.model.UserPreferenceType;
import com.amazon.dw.grasshopper.util.RetryMetric;
import com.amazon.dw.grasshopper.util.aws.DynamoGetCallable;
import com.amazon.dw.grasshopper.util.aws.DynamoPutCallable;
import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import org.apache.log4j.Logger;

/**
 * Handles UserPreference saving.
 * Holds a Queue of preferences to add.
 * Creates a worker-thread to clean-up the queue.
 *
 */
public class UserPreferencesUpdater {
    private static final String COUNTER_USER_PREFERENCES_QUEUE = "UserPreferencesQueue";
    public static final String USER_PREFERENCES_INTERNAL_DATA = "InternalData";
    public static final String ADD_USER_PREFERENCES_FAILED_COUNTER = "AddPreferenceFailed";
    private static final Logger LOGGER = Logger.getLogger(UserPreferencesUpdater.class);

    private ScheduledThreadPoolExecutor executer;
    private ConcurrentLinkedQueue<AddPreferenceInput> queue; 
    private AtomicInteger queueSizeCounter;
    

    //     initialized by Spring:
    private  MetricsFactory metricsFactory;
    private long sleepTime;
    private long startingDelayTime;
    private int threadCount;
    private int retriesNumberDynamo;
    private long sleepTimeRetryDynamo;
    private AmazonDynamoDB dynamoClient;
    private UpdatePreferenceLogic preferenceUpdateLogic;
        
    // relevant to unit-testing.
    private UserPreferencesWorkerThread dummyWorkerThread;
    
    
    
    /**
     * A worker thread to add preferences to DB.
     */
    public final class  UserPreferencesWorkerThread implements Runnable{

        /**
         * Get old/existing preference from DynamoDB
         * @param input
         * @param metrics
         * @return
         * @throws Exception
         */
        GetItemResult getPreference(AddPreferenceInput input, Metrics metrics) throws Exception{
           
            UserPreferenceType type = UserPreferenceType.parse(input.getPreferenceType());
              
            String pk = UserPreferencesService.generatePK_user(input.getUserGroupName().toLowerCase().trim(), type);
            LOGGER.trace("getPreference calling Dynamo with PK:"+pk);
            
            Map<String, AttributeValue> hashKey = new HashMap<String, AttributeValue>();
            hashKey.put(UserPreferencesService.USER_PREFERENCES_PK_NAME, new AttributeValue().withS(pk));
            
            GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(UserPreferencesService.USER_PREFERENCES_TABLE)
                .withAttributesToGet(UserPreferencesService.USER_PREFERENCES_DATA)
                .withAttributesToGet(USER_PREFERENCES_INTERNAL_DATA)
                .withConsistentRead(true)
                .withKey(hashKey);
            
            RetryMetric<GetItemResult> retry = new RetryMetric<GetItemResult>(LOGGER, metrics, UserPreferencesService.USER_PREFERENCE_DYNAMO_FAILURES, UserPreferencesService.USER_PREFERENCE_DYNAMO_LATENCY, new DynamoGetCallable(dynamoClient, getItemRequest), retriesNumberDynamo, sleepTimeRetryDynamo);
            return retry.call();
        }
        
        /**
         * Saves a user-preference to DynamoDB, in a "ready-for-use" format.
         * @param input
         * @param data
         * @param internalData
         * @param metrics
         * @throws Exception
         */
        void savePreference(AddPreferenceInput input, JSONObject data, JSONObject internalData, Metrics metrics) throws Exception{
            UserPreferenceType type = UserPreferenceType.parse(input.getPreferenceType());
            String pk = UserPreferencesService.generatePK_user(input.getUserGroupName().toLowerCase().trim(), type);
            if(LOGGER.isTraceEnabled())
                LOGGER.trace("savePreference calling Dynamo with PK:"+pk);
            
            String jsonFormat = data.toString();
            String jsonFormatInternalDate = internalData.toString();
            
            Map<String, AttributeValue> newVersion = new HashMap<String, AttributeValue>();
            newVersion.put(UserPreferencesService.USER_PREFERENCES_PK_NAME, new AttributeValue().withS(pk));
            newVersion.put(UserPreferencesService.USER_PREFERENCES_DATA, new AttributeValue().withS(jsonFormat));
            newVersion.put(USER_PREFERENCES_INTERNAL_DATA, new AttributeValue().withS(jsonFormatInternalDate));
            
            PutItemRequest putItemRequest = new PutItemRequest()
                .withItem(newVersion)
                .withTableName(UserPreferencesService.USER_PREFERENCES_TABLE)
                .withReturnValues(ReturnValue.ALL_OLD);
            
            RetryMetric<PutItemResult> retry = new RetryMetric<PutItemResult>(LOGGER, metrics, UserPreferencesService.USER_PREFERENCE_DYNAMO_FAILURES, UserPreferencesService.USER_PREFERENCE_DYNAMO_LATENCY, new DynamoPutCallable(dynamoClient, putItemRequest), retriesNumberDynamo, sleepTimeRetryDynamo);
            retry.call();
        }
        
        
        /**
         * The Worker-Thread run method.
         * Polls queue, if there's something to do, 
         *      Remove object from queue, updated counter
         *      Get UserPreference from Dynamo.
         *      Update The Internal_Data attribute & Derive the new "ready-for-use" preference
         *      Save to Dynamo.
         *  Saves metrics and Logging
         */
        @Override
        public void run() {
            Metrics metric = metricsFactory.newMetrics();
            AddPreferenceInput input = new AddPreferenceInput();
            long start = System.currentTimeMillis();
            long stop = start;
            boolean hasAnalyzedPreferenceReuest = true;
            try{
                input =  queue.poll();
                if(input == null){
                    LOGGER.trace("No items in queue. Going back to sleep.");
                    hasAnalyzedPreferenceReuest = false;
                }
                else{
                    queueSizeCounter.addAndGet(-1);
                    GetItemResult result = getPreference(input, metric);
                    JSONObject jsonInternalData = null;
                    
                    if( result.getItem() == null || result.getItem().get(USER_PREFERENCES_INTERNAL_DATA)==null){
                        jsonInternalData = new JSONObject();
                    }
                    else{
                        jsonInternalData = new JSONObject(result.getItem().get(USER_PREFERENCES_INTERNAL_DATA).getS());
                    }
                    
                    JSONObject newUserPreferenceData = preferenceUpdateLogic.updateAndCalculatePreference(jsonInternalData, input);
                    
                    savePreference(input, newUserPreferenceData, jsonInternalData, metric);
                }
                
                stop = System.currentTimeMillis();
                
                
            }
            catch(Exception ex){
                LOGGER.error("Failed updating preference for input: (" + input.getPreferenceType()+","+input.getUserGroupName()+") - " + input.getPreferenceIdList().toString(), ex );
                metric.addCount(ADD_USER_PREFERENCES_FAILED_COUNTER, 1.0, Unit.ONE);
            }
            if(hasAnalyzedPreferenceReuest)
                reportMetrics(queueSizeCounter.get(), metric, start, stop, "UpdateUserPreference");
           
        }
            
    }
    
    public void setMetricsFactory(MetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
    }

    /**
     * Adds a new element to the AddPreference queue, for it to be processed by the Worker-Thread.
    * @param input
     */
    public void addPreferenceToHandle(AddPreferenceInput input){
        long start = System.currentTimeMillis();
        queue.add(input);
        int size =  queueSizeCounter.incrementAndGet();
        long stop = System.currentTimeMillis();
        reportMetrics(size, metricsFactory.newMetrics(),start, stop, "AddPreferenceToHandle" );
    }

   protected void reportMetrics(int size, Metrics metric, long start, long stop, String operationName){
        metric.addDate("StartTime", start);
        metric.addDate("EndTime", stop);
        metric.addTime("Time", stop - start, SI.MILLI(SI.SECOND));
        metric.addProperty("Operation", operationName);
        metric.addLevel(COUNTER_USER_PREFERENCES_QUEUE, (double)size, Unit.ONE);
        metric.close();
    }
    
    public void init() {
        executer = new ScheduledThreadPoolExecutor(getThreadCount());
        queue = new ConcurrentLinkedQueue<AddPreferenceInput>();
        queueSizeCounter = new AtomicInteger(0);
        for(int i=0;i<getThreadCount();i++)
            executer.scheduleWithFixedDelay(new UserPreferencesWorkerThread(), getStartingDelayTime() , getSleepTime() , TimeUnit.MILLISECONDS);
    }
    
    public void init_unitTesting(){
        queue = new ConcurrentLinkedQueue<AddPreferenceInput>();
        queueSizeCounter = new AtomicInteger(0);
        dummyWorkerThread = new UserPreferencesWorkerThread();
    }
    
    public UserPreferencesWorkerThread getDummyWorkThread(){
        return dummyWorkerThread;
    }
    
    public void destroy(){
        if(executer!=null)
            executer.shutdown();
    }

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public long getStartingDelayTime() {
        return startingDelayTime;
    }

    public void setStartingDelayTime(long startingDelayTime) {
        this.startingDelayTime = startingDelayTime;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getRetriesNumberDynamo() {
        return retriesNumberDynamo;
    }

    public void setRetriesNumberDynamo(int retriesNumberDynamo) {
        this.retriesNumberDynamo = retriesNumberDynamo;
    }

    public long getSleepTimeRetryDynamo() {
        return sleepTimeRetryDynamo;
    }

    public void setSleepTimeRetryDynamo(long sleepTimeRetryDynamo) {
        this.sleepTimeRetryDynamo = sleepTimeRetryDynamo;
    }
    
    public AmazonDynamoDB getDynamoClient() {
        return dynamoClient;
    }

    public void setDynamoClient(AmazonDynamoDB dynamoClient) {
        this.dynamoClient = dynamoClient;
    }
    
    public UpdatePreferenceLogic getPreferenceUpdateLogic() {
        return preferenceUpdateLogic;
    }

    public void setPreferenceUpdateLogic(UpdatePreferenceLogic preferenceUpdateLogic) {
        this.preferenceUpdateLogic = preferenceUpdateLogic;
    }
}
