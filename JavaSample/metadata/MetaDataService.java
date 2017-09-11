package com.amazon.dw.grasshopper.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.google.common.collect.Lists;
import com.amazon.dw.grasshopper.attributesearchservice.AttributeSearchWrapper;
import com.amazon.dw.grasshopper.attributesearchservice.AttributeCloudSearchOutputRanker;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.dw.grasshopper.AreaListOutput;
import com.amazon.dw.grasshopper.AreaModel;
import com.amazon.dw.grasshopper.AttributeListOutput;
import com.amazon.dw.grasshopper.AttributeModel;
import com.amazon.dw.grasshopper.AttributeSearchInput;
import com.amazon.dw.grasshopper.AttributeSearchOutput;
import com.amazon.dw.grasshopper.AreaInformation;
import com.amazon.dw.grasshopper.CategoryListOutput;
import com.amazon.dw.grasshopper.CategoryModel;
import com.amazon.dw.grasshopper.ColumnInstanceModel;
import com.amazon.dw.grasshopper.CountObject;
import com.amazon.dw.grasshopper.CurrentVersionInput;
import com.amazon.dw.grasshopper.CurrentVersionOutput;
import com.amazon.dw.grasshopper.FunctionListOutput;
import com.amazon.dw.grasshopper.FunctionModel;
import com.amazon.dw.grasshopper.GetAllColumnInstancesReachableInput;
import com.amazon.dw.grasshopper.GetAllColumnInstancesReachableOutput;
import com.amazon.dw.grasshopper.GetMetaDataElementByIDInput;
import com.amazon.dw.grasshopper.GetTableByNameInput;
import com.amazon.dw.grasshopper.GHQInput;
import com.amazon.dw.grasshopper.GHQOutput;
import com.amazon.dw.grasshopper.GHQOutputFilter;
import com.amazon.dw.grasshopper.IDListInput;
import com.amazon.dw.grasshopper.MetaDataServiceException;
import com.amazon.dw.grasshopper.NameListInput;
import com.amazon.dw.grasshopper.TableAttributeTuple;
import com.amazon.dw.grasshopper.TableColumnTuple;
import com.amazon.dw.grasshopper.TableListOutput;
import com.amazon.dw.grasshopper.TableModel;
import com.amazon.dw.grasshopper.UserInput;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.Function;
import com.amazon.dw.grasshopper.model.Table;
//import com.amazon.dw.grasshopper.nlp.NounPhraseExtractor;
//import com.amazon.dw.grasshopper.filterprocessor.FilterParametersExtractor;
import com.amazon.dw.grasshopper.attributesearchservice.GHQGenerator;



import org.json.simple.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;



/**
 * A Coral web service that provides users information about GrassHopper metadata
 * 
 * @author shererr
 */
@Service("MetaDataService")
public class MetaDataService extends Activity {

    private static final Logger LOGGER = Logger.getLogger(MetaDataService.class);
    private MetaDataStoreManager dataStoreManager;
    private static final String searchURL = "search-attribute-json-vcvzp6g6s2f2eyfcetsgvwrmwm.us-east-1.cloudsearch.amazonaws.com";
    private static final String apiVersion = "2013-01-01"; 



    public MetaDataStoreManager getDataStoreManager() {
        return dataStoreManager;
    }


    public void setDataStoreManager(MetaDataStoreManager dataStoreManager) {
        this.dataStoreManager = dataStoreManager;
    }


    public MetaDataService() {
    }

    
    /**
     * Returns all areas in the backend data store
     * @return all areas in the backend data store
     */
    @Operation("getAreas")
    @Validated
    public AreaListOutput getAreas(CurrentVersionInput input)
    {
        Comparator<AreaModel> areaComparator = new Comparator<AreaModel>() {
            @Override
            public int compare(AreaModel model1, AreaModel model2) {
                if(model1 == null || model2 == null)
                    throw new NullPointerException();
                return model1.getName().compareTo(model2.getName());
            }
        };
        
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<AreaModel> areaModels = new LinkedList<AreaModel>();
        List<Area> areas = new LinkedList<Area>(dataStore.getAllAreas());

        for(Area area : areas) {
            areaModels.add(area.getModel());
        }

        //Sort the output on the server side, not the client side.
        Collection<AreaModel> allAreas = new TreeSet<AreaModel>(areaComparator);
        Collection<Long> usedAreaIds = new TreeSet<Long>();
        
        for (AreaModel model : areaModels) {
            Long areaId = model.getId();
            if (!usedAreaIds.contains(areaId)) {
                usedAreaIds.add(areaId);
                allAreas.add(model);
            }
        }
       
        AreaListOutput output =  new AreaListOutput();
        output.setAreas(Lists.newArrayList(allAreas));
        
        return output;
    }


    /**
     * Returns the <code>AreaModel</code> with the given unique identifier
     * @param id the unique identifier for the <code>AreaModel</code>
     * @return the <code>AreaModel</code> with the given unique identifier
     */
    @Operation("getAreaById")
    @Validated
    public AreaModel getAreaById(GetMetaDataElementByIDInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        AreaModel model = new AreaModel();
        Area area = dataStore.getArea(input.getId());
        if (area != null) {
            model = area.getModel();
        }
        return model;
    }


    /**
     * Returns the <code>AreaModel</code>s with the given unique identifiers
     * @param input a list of unique identifiers
     * @return a list of <code>AreaModel</code>s with the given unique identifiers
     */
    @Operation("getAreasById")
    @Validated
    public AreaListOutput getAreasById(IDListInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<AreaModel> areaModels = new LinkedList<AreaModel>();
        for(long id : input.getIds()) {
            Area area = dataStore.getArea(id);
            if(area != null) {
                areaModels.add(area.getModel());
            }
        }

        AreaListOutput output = new AreaListOutput();
        output.setAreas(areaModels);
        return output;
    }


    /**
     * Returns all functions in the backend data store
     * @return all functions in the backend data store
     */
    @Operation("getAllFunctions")
    @Validated
    public FunctionListOutput getAllFunctions(CurrentVersionInput input)
    {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<FunctionModel> functionModels = new LinkedList<FunctionModel>();
        List<Function> functions = new LinkedList<Function>(dataStore.getAllFunctions());

        for(Function function : functions) {
            functionModels.add(function.getModel());
        }

        FunctionListOutput output = new FunctionListOutput();
        output.setFunctions(functionModels);
        return output;
    }


    /**
     * Returns the <code>AttributeModel</code> with the given unique identifier
     * @param id the unique identifier for the <code>AttributeModel</code>
     * @return the <code>AttributeModel</code> with the given unique identifier
     */
    @Operation("getAttributeById")
    @Validated
    public AttributeModel getAttributeById(GetMetaDataElementByIDInput input) {
        AttributeModel model = new AttributeModel();
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        Attribute attribute = dataStore.getAttribute(input.getId());
        if (attribute != null) {
            model = attribute.getModel();
        }
        return model;
    }


    /**
     * Returns the <code>AttributeModel</code>s with the given unique identifiers
     * @param input the list of unique identifiers for the <code>AttributeModel</code>
     * @return the <code>AttributeModel</code> with the given unique identifier
     */
    @Operation("getAttributesById")
    @Validated
    public AttributeListOutput getAttributesById(IDListInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<AttributeModel> attributes = new LinkedList<AttributeModel>();

        for(long id : input.getIds()) {
            Attribute attr = dataStore.getAttribute(id);
            if(attr != null) {
                attributes.add(attr.getModel());
            }
        }

        AttributeListOutput output = new AttributeListOutput();
        output.setAttributes(attributes);
        return output;
    }


    /**
     * Returns the <code>FunctionModel</code> with the given unique identifier
     * @param id the unique identifier for the <code>FunctionModel</code>
     * @return the <code>FunctionModel</code> with the given unique identifier
     */
    @Operation("getFunctionById")
    @Validated
    public FunctionModel getFunctionById(GetMetaDataElementByIDInput input)
    {
        FunctionModel fModel = new FunctionModel();
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        Function function= dataStore.getFunction(input.getId(), DBType.SQL.getId());
        if(function != null){
            fModel = function.getModel();
        }
        return fModel;
    }


    /**
     * Returns the <code>FunctionModel</code>s with the given unique identifiers
     * @param input the list of unique identifiers for the <code>FunctionModel</code>s
     * @return the list of <code>FunctionModel</code>s with the given unique identifiers
     */
    @Operation("getFunctionsById")
    @Validated
    public FunctionListOutput getFunctionsById(IDListInput input)
    {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<FunctionModel> functions = new LinkedList<FunctionModel>();

        for(long id : input.getIds()) {
            Function function = dataStore.getFunction(id, DBType.SQL.getId());
            if(function != null) {
                functions.add(function.getModel());
            }
        }

        FunctionListOutput output = new FunctionListOutput();
        output.setFunctions(functions);
        return output;
    }


    /**
     * Returns the <code>TableModel</code> with the given logical name
     * @param name the logical name for the <code>TableModel</code>
     * @return the <code>TableModel</code> with the given logical name
     */
    @Operation("getTableByName")
    @Validated
    public TableModel getTableByName(GetTableByNameInput input) {
        TableModel model = new TableModel();
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        Table table = dataStore.getTable(input.getName());
        if (table != null) {
            model = table.getModel();
        }
        return model;
    }


    /**
     * Returns the <code>TableModel</code>s with the given logical names
     * @param input the list of logical names for the <code>TableModel</code>s
     * @return the list of <code>TableModel</code>s with the given logical names
     */
    @Operation("getTablesByName")
    @Validated
    public TableListOutput getTablesByName(NameListInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<TableModel> tables = new LinkedList<TableModel>();

        for(String name : input.getNames()) {
            Table table = dataStore.getTable(name);
            if(table != null) {
                tables.add(table.getModel());
            }
        }

        TableListOutput output = new TableListOutput();
        output.setTables(tables);
        return output;
    }


    /**
     * Returns the <code>CategoryModel</code> with the given unique identifier
     * @param id the unique identifier for the <code>CategoryModel</code>
     * @return the <code>CategoryModel</code> with the given unique identifier
     */
    @Operation("getCategoryById")
    @Validated
    public CategoryModel getCategoryById(GetMetaDataElementByIDInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        Category category = dataStore.getCategory(input.getId());
        if(category == null) {
            return new CategoryModel();
        }

        return category.getModel();
    }


    /**
     * Returns the <code>CategoryModel</code>s with the given unique identifiers
     * @param input the list of unique identifiers for the <code>CategoryModel</code>s
     * @return the list of <code>CategoryModel</code>s with the given unique identifiers
     */
    @Operation("getCategoriesById")
    @Validated
    public CategoryListOutput getCategoriesById(IDListInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        List<CategoryModel> categories = new LinkedList<CategoryModel>();

        for(long id : input.getIds()) {
            Category cat = dataStore.getCategory(id);
            if(cat != null) {
                categories.add(cat.getModel());
            }
        }

        CategoryListOutput output = new CategoryListOutput();
        output.setCategories(categories);
        return output;
    }


    /**
     * Returns statistics on the number of objects stored in the backend storage system
     * @return statistics on the number of objects stored in the backend storage system
     */
    @Operation("getStorageCounts")
    @Validated
    public CountObject getStorageCounts(CurrentVersionInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        CountObject counts = new CountObject();
        counts.setNumAreas(dataStore.getNumAreas());
        counts.setNumAttributes(dataStore.getNumAttributes());
        counts.setNumCategories(dataStore.getNumCategories());
        counts.setNumFunctions(dataStore.getNumFunctions());
        counts.setNumTables(dataStore.getNumTables());
        counts.setNumColumnInstances(dataStore.getNumColumnInstances());

        return counts;
    }


    @Operation("getColumnInstance")
    @Validated
    public ColumnInstanceModel getColumnInstance(TableAttributeTuple tuple) {
        MetaDataStore dataStore = dataStoreManager.getStore(tuple.getGroupName());
        String tableName = tuple.getTableName();
        long attrId = tuple.getAttributeId();

        Table table = dataStore.getTable(tableName);
        ColumnInstance ci =  table.getColumnInstance(attrId);

        if(ci == null) {
            return new ColumnInstanceModel();
        }

        return ci.getModel();
    }


    @Operation("getAllTables")
    @Validated
    public TableListOutput getAllTables(CurrentVersionInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        TableListOutput output = new TableListOutput();
        Collection<Table> tables = dataStore.getAllTables();
        List<TableModel> models = new LinkedList<TableModel>();

        for(Table table : tables) {
            models.add(table.getModel());
        }

        output.setTables(models);
        return output;
    }


    @Operation("getAllCategories")
    @Validated
    public CategoryListOutput getAllCategories(CurrentVersionInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        CategoryListOutput output = new CategoryListOutput();
        Collection<Category> categories = dataStore.getAllCategories();
        List<CategoryModel> models = new LinkedList<CategoryModel>();

        for(Category category : categories) {
            models.add(category.getModel());
        }

        output.setCategories(models);
        return output;
    }


    @Operation("getAllAttributes")
    @Validated
    public AttributeListOutput getAllAttributes(CurrentVersionInput input) {
        MetaDataStore dataStore = dataStoreManager.getStore(input.getGroupName());
        AttributeListOutput output = new AttributeListOutput();
        Collection<Attribute> attributes = dataStore.getAllAttributes();
        List<AttributeModel> models = new LinkedList<AttributeModel>();

        for(Attribute attribute : attributes) {
            models.add(attribute.getModel());
        }

        output.setAttributes(models);
        return output;
    }


    /**
     * Gets the current metadata version string
     * @return current metadata version string
     */
    @Operation("getCurrentVersion")
    @Validated
    public CurrentVersionOutput getCurrentVersion(CurrentVersionInput input) throws MetaDataServiceException {
        return dataStoreManager.getLatestMetaDataVersion(input.getGroupName());
    }

    /**
     * returns Tuples that contain all the ColumnInstances reachable from
     * the input driving-tables.
     */
    @Operation("getAllColumnInstancesReachable")
    @Validated
    public GetAllColumnInstancesReachableOutput getAllColumnInstancesReachable(GetAllColumnInstancesReachableInput input) throws MetaDataServiceException {
        MetaDataStore store = dataStoreManager.getStore(input.getVersionName());
        ArrayList<Table> tables = new ArrayList<Table>();
        for(String s : input.getTables()){
            if(store.getTable(s)!=null){
                tables.add(store.getTable(s));
            }
        }
        LOGGER.debug("GetAllColumnInstancesReachableOutput - Number of tables to handle:" + tables.size());
        MetaDataAnalyzer analyzer = new MetaDataAnalyzer(store);
        Collection<Table> transitiveClosure = analyzer.getListOfConnectedTables(tables, input.getAreaId());
        LOGGER.debug("GetAllColumnInstancesReachableOutput - Transitive Closure Size:" + transitiveClosure.size());
        GetAllColumnInstancesReachableOutput retval = new GetAllColumnInstancesReachableOutput();
        ArrayList<TableColumnTuple> tuples = new ArrayList<TableColumnTuple>();
        for(Table t : transitiveClosure){
            for(Entry<Long, ColumnInstance> entry : t.getAllAttributesMappingCopy().entrySet()){
                TableColumnTuple tuple = new TableColumnTuple();
                tuple.setAttributeId(entry.getKey().longValue());
                tuple.setColumnName(entry.getValue().getColumnName());
                if(t.getPhysicalName() != null && t.getPhysicalName().trim().length() > 0)
                    tuple.setTable(t.getPhysicalName());
                else
                    tuple.setTable(t.getTableName());
                tuples.add(tuple);
            }
        }
        LOGGER.debug("GetAllColumnInstancesReachableOutput - Returned collections size:" + tuples.size());
        retval.setTableColumnList(tuples);
        return retval;
    }

    /**
     * Queries the cloud search domain for each search term, 
     * Returns the best subject area and best attributes for each subject area
     * for every search term.
     * @param input List of search terms.
     * @return
     * @throws MetaDataServiceException
     */
    @Operation("getAttributeSearchResults")
    @Validated
    public AttributeSearchOutput getAttributeSearchResults(AttributeSearchInput input)  throws MetaDataServiceException {

        try {

            AttributeSearchWrapper wrapper = new AttributeSearchWrapper(searchURL, apiVersion);
            AttributeCloudSearchOutputRanker ranker = new AttributeCloudSearchOutputRanker();
            Map< String, Map< String, Map< String, Double > > > mapping = new  HashMap< String, Map< String, Map< String, Double > > >();                                   
            for(String searchTerm : input.getSearchTerms()) {
                Map< String, Map< String, Double > > singleTermMapping = wrapper.searchQuery(searchTerm);
                if(singleTermMapping != null && singleTermMapping.size() > 0) {
                    mapping.put(searchTerm, singleTermMapping);
                }
            }

            JSONArray areasJSON = ranker.mergeCloudSearchResults(mapping);
            List<AreaInformation> aInfoList = new ArrayList<AreaInformation>();
            for (int i = 0; i < areasJSON.size(); i++) {
                AreaInformation aInfo = new AreaInformation();
                JSONObject area = (JSONObject) areasJSON.get(i);
                String areaID = (String) area.get("area_id");
                JSONArray attrs = (JSONArray) area.get("attributes");
                List<String> attributes = new ArrayList<String>();
                for (int j = 0; j < attrs.size(); j++) {
                    attributes.add((String) attrs.get(j));
                }
                aInfo.setAreaId(areaID);
                aInfo.setAttributes(attributes);
                aInfoList.add(aInfo);
            }
            AttributeSearchOutput finalResults = new AttributeSearchOutput();
            finalResults.setAreaInfoList(aInfoList);
            return finalResults;

        } catch(IOException e) {
            throw new MetaDataServiceException();
        } catch(JSONException e) {
            throw new MetaDataServiceException();
        }
    }

    /**
     * Returns the filter parameters given user query.
     * @param input user query.
     * @return output is the JSONObject with filter parameters.
     * @throws JSONException
     * @throws IOException
     */
 /*   @Operation("getFilterParameters")
    @Validated
    public GHQOutputFilter getFilterParameters(GHQInput input) throws JSONException, IOException {

        String inputSentence = input.getInputSentence();
        FilterParametersExtractor extractor = new FilterParametersExtractor();
        JSONObject outputJSON = extractor.process(inputSentence);
        GHQOutputFilter output = new GHQOutputFilter();
        output.setOutputSentence(outputJSON.toString());
        return output;

    }
*/
    /**
     * Extracts search keywords from user query.
     * @param input user query.
     * @return output is list of keywords/phrases.
     */
/*    @Operation("getSearchTerms")
    @Validated
    public AttributeSearchInput getSearchTerms(UserInput input) {

        AttributeSearchInput output = new AttributeSearchInput();
        String inputSentence = input.getUserQuery();
        NounPhraseExtractor  NPE = new NounPhraseExtractor();
        List<String> nounPhrases = NPE.extractNounPhrases(inputSentence);
        output.setSearchTerms(nounPhrases);
        return output;

    }
    */

    /**
     * Constructs GHQuery from user query.
     * @param input user query
     * @return GHQuery string
     * @throws JSONException
     * @throws IOException
     * @throws ParseException
     */
/*    @Operation("getGHQuery")
    @Validated
    public GHQOutput getGHQuery(GHQInput input) throws JSONException, IOException, ParseException {

        GHQOutput output = new GHQOutput();
        String inputSentence = input.getInputSentence();

        // Extracts Noun Phrases from the user query.
        UserInput userInput = new  UserInput();
        userInput.setUserQuery(inputSentence);
        AttributeSearchInput searchTerms = getSearchTerms(userInput);


        // Get the closest subject areas and attributes.
        AttributeSearchOutput searchResults = getAttributeSearchResults(searchTerms);
        List<AreaInformation> areaList = searchResults.getAreaInfoList();
        JSONArray finalSearchResultsList = new JSONArray();
        for (AreaInformation area : areaList) {
            JSONObject areaObj = new JSONObject();
            areaObj.put("areaId", area.getAreaId());
            areaObj.put("attributes", area.getAttributes());
            finalSearchResultsList.add(areaObj);
        }
        JSONObject finalSearchResults = new JSONObject();
        finalSearchResults.put("areaInfoList", finalSearchResultsList);
        System.out.println(finalSearchResults.toString());

        // Gets the filter parameters
        FilterParametersExtractor extractor = new FilterParametersExtractor();
        JSONObject filtersJSON = extractor.process(inputSentence);
        System.out.println(filtersJSON.toString());

        // Gets Attribute Information
        JSONArray areasJSON = (JSONArray) finalSearchResults.get("areaInfoList");
        System.out.println(areasJSON.toString());
        Map<String, JSONObject> attributeInformation = getAttributeInformation(areasJSON);
        System.out.println(attributeInformation.toString());


        GHQGenerator genGHQ = new GHQGenerator();
        // Transforms Sort Filter parameters
        JSONObject sortObject = (JSONObject) filtersJSON.get("SortFilter");
        if(sortObject.size() > 0 && sortObject.containsKey("sortParameter")) {

            String sortParam = (String) sortObject.get("sortParameter");

            AttributeSearchWrapper wrapper = new AttributeSearchWrapper(searchURL, apiVersion);
            Map<String, Map<String, Double>> singleTermMapping = wrapper.searchQuery(sortParam);

            if (singleTermMapping.size() > 0) {
                JSONObject modifiedJSON = changeSortFilter(finalSearchResults, (JSONObject) filtersJSON.get("SortFilter"), singleTermMapping); 
                filtersJSON.put("SortFilter", (JSONObject) modifiedJSON.get("sort"));
                finalSearchResults  = (JSONObject) modifiedJSON.get("attributeSearch");
            }

        }

        System.out.println(finalSearchResults.toString());
        // Merge the results and create GHQuery
        List<String> ghqStrings = genGHQ.generateGHQ(finalSearchResults, filtersJSON, attributeInformation);
        System.out.println(ghqStrings.toString());

        output.setOutputSentence(ghqStrings);
        return output;

    }
    
   */

    /**
     * Gets the attribute information(name and data-type) for a list of attributes.
     * @param areasJSON
     * @return
     * @throws JSONException
     */
    private Map<String, JSONObject> getAttributeInformation(JSONArray areasJSON) throws JSONException {

        Map<String, JSONObject> attributeInformation = new HashMap<String, JSONObject>();
        List<Long> attributeIds = new ArrayList<Long>();
        for (int i = 0; i < areasJSON.size(); i++) {
            JSONObject areaObj = (JSONObject) areasJSON.get(i);
            String areaID = (String) areaObj.get("areaId");
            System.out.println(areaID);
            ArrayList<String> attrsArray = (ArrayList<String>) areaObj.get("attributes"); 
            for (int j = 0; j < attrsArray.size(); j++) {
                String id = (String) attrsArray.get(j);
                attributeIds.add(Long.parseLong(id));
            }
        }

        IDListInput inputIds = new IDListInput();
        inputIds.setIds(attributeIds);
        inputIds.setGroupName("foo");
        inputIds.setMinValidFrom(Long.valueOf(100000));
        AttributeListOutput attrInfo = getAttributesById(inputIds);

        List<AttributeModel> attributesInfoList = attrInfo.getAttributes();
        for (AttributeModel info : attributesInfoList) {
            String id = String.valueOf(info.getId());
            String name = info.getName();
            String type = String.valueOf(info.getColumnType());
            JSONObject attribute = new JSONObject();
            attribute.put("name", name);
            attribute.put("type", ColumnType.get(Long.parseLong(type)));
            attributeInformation.put(id, attribute);
        }

        return attributeInformation;

    }

    public JSONObject changeSortFilter(JSONObject attributeSearchResults, JSONObject sortFilter, 
            Map<String, Map<String, Double>> areaAttributeMap) throws JSONException {


        if (sortFilter.containsKey("sortParameter")) {

            JSONArray areaInfoList = (JSONArray) attributeSearchResults.get("areaInfoList");
            JSONArray sortParameters = new JSONArray();
            JSONArray areaInfoListNew = new JSONArray(); 
            for (int i = 0; i < areaInfoList.size(); i++) {
                JSONObject area = (JSONObject) areaInfoList.get(i);
                String areaName = (String) area.get("areaId");
                if (areaAttributeMap.containsKey(areaName))  {
                    ArrayList<String> attributes = (ArrayList<String>) area.get("attributes");
                    Map<String, Double> attrMap = areaAttributeMap.get(areaName);
                    String bestAttr = getHighestScoreAttribute(attrMap);
                    if (!attributes.contains(bestAttr)) {
                        attributes.add(bestAttr);
                    }
                    area.put("attributes", attributes);
                    sortParameters.add(bestAttr);   
                }
                areaInfoListNew.add(area);
            }
            sortFilter.put("sortParameter", sortParameters);
            attributeSearchResults.put("areaInfoList", areaInfoList);

        }

        JSONObject finalObject = new JSONObject();
        finalObject.put("sort", sortFilter);
        finalObject.put("attributeSearch", attributeSearchResults);
        return finalObject;

    }

    private String getHighestScoreAttribute(Map<String, Double> attributes) {

        double maxScore = 0.0;
        String bestAttribute = null;
        for (Map.Entry<String, Double> entry : attributes.entrySet()) {
            double score = entry.getValue().doubleValue();
            if (score > maxScore) {
                maxScore = score;
                bestAttribute = entry.getKey();
            }
        }
        return bestAttribute;
    }


}
