package com.amazon.dw.grasshopper.compilerservice;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.measure.unit.Unit;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.hibernate.jdbc.util.BasicFormatterImpl;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.dw.grasshopper.CompareGHQsInput;
import com.amazon.dw.grasshopper.CompareGHQsOutput;
import com.amazon.dw.grasshopper.CompilerInputStructure;
import com.amazon.dw.grasshopper.CompilerOutputStructure;
import com.amazon.dw.grasshopper.CompilerServiceException;
import com.amazon.dw.grasshopper.ConvertDSSToGHQInput;
import com.amazon.dw.grasshopper.ConvertDSSToGHQOutput;
import com.amazon.dw.grasshopper.GetDependenciesInput;
import com.amazon.dw.grasshopper.GetDependenciesOutput;
import com.amazon.dw.grasshopper.GetNativeQueryInput;
import com.amazon.dw.grasshopper.GetNativeQueryOutput;
import com.amazon.dw.grasshopper.ValidateSubExpressionOutput;
import com.amazon.dw.grasshopper.clients.IDatanetServiceWrapper;
import com.amazon.dw.grasshopper.compilerservice.ghqfactory.BaseGHQFromJSONFactory;
import com.amazon.dw.grasshopper.compilerservice.ghqfactory.IGHQFromJSONFactoryFactory;
import com.amazon.dw.grasshopper.metadata.MetaDataStore;
import com.amazon.dw.grasshopper.metadata.MetaDataStoreManager;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.Table;
import com.amazon.dw.grasshopper.model.GHQuery.GHQuery;
import com.amazon.dw.grasshopper.tools.GHQFromDSSXMLFactory;

@Service("CompilerService")
public class CompilerService extends Activity {
    private static final Logger LOGGER = Logger.getLogger(CompilerService.class);
    public static final String METRIC_NAME_COMPILE_REQUEST = "CompileRequest";
    public static final String METRIC_NAME_COMPILE_FAILED = "CompileFailed";
    public static final String METRIC_NAME_VALIDATION_REQUEST = "ValidationRequest";
    public static final String METRIC_NAME_VALIDATION_FAILED = "ValidationFailed";
   
    /* Notice that the data-structures are created within Coral framework, and their definition
     * appears in the the coral-generated-src package.*/
    
    protected CompilerConfiguration compilerConfiguration;
    protected MetaDataStoreManager dataStoreManager;
    protected IGHQFromJSONFactoryFactory ghqFactoryFactory;
    protected IDatanetServiceWrapper datanetServiceWrapper;
   
    public void setGhqFactoryFactory(IGHQFromJSONFactoryFactory ghqFactoryFactory) {
        this.ghqFactoryFactory = ghqFactoryFactory;
    }

    public void setDataStoreManager(MetaDataStoreManager dataStoreManager) {
        this.dataStoreManager = dataStoreManager;
    }

    public void setCompilerConfiguration(CompilerConfiguration compilerConfig) {
        compilerConfiguration = compilerConfig;
    }

    public void setDatanetServiceWrapper(
            IDatanetServiceWrapper datanetServiceWrapper) {
        this.datanetServiceWrapper = datanetServiceWrapper;
    }

    public CompilerService() {
    }
    
    @CompileAspectAnnotation( attemptMetricName=METRIC_NAME_COMPILE_REQUEST, failureMetricName=METRIC_NAME_COMPILE_FAILED)
    @Operation("compile")
    public CompilerOutputStructure compile(CompilerInputStructure CompilerInput) throws Exception{
        CompilerOutputStructure output = innerCompile(CompilerInput);
        String formattedSQL = new BasicFormatterImpl().format(output.getCompilerOutput());
        output.setCompilerOutput(formattedSQL);
        return output;
    }
    
    private CompilerOutputStructure innerCompile(CompilerInputStructure CompilerInput) throws Exception{
        CompilerOutputStructure output = new CompilerOutputStructure();
        CompilerOrchestrator orchestrator = new CompilerOrchestrator(compilerConfiguration, dataStoreManager,
                ghqFactoryFactory, datanetServiceWrapper, getMetrics());
        JSONObject ghqJSON = new JSONObject(CompilerInput.getGhqJSON());
        CompilerOrchestrator.CompileOutput compileOutput =  orchestrator.compile(ghqJSON, CompilerInput.getDBtype());
        if(compileOutput.validationError!=null && !compileOutput.validationError.isEmpty()){
            throw new CompilerServiceException("Validation failed:"+compileOutput.validationError);
        }
        
        output.setCompilerOutput(compileOutput.nativeQuery);
        
        //Post processing
        String suggestions = getSuggestions(compileOutput).toString();
        output.setCompilerQualitySuggestions(suggestions);
        return output;
    }
    
    @CompileAspectAnnotation( attemptMetricName=METRIC_NAME_COMPILE_REQUEST, failureMetricName=METRIC_NAME_COMPILE_FAILED)
    @Operation("getDependencies")
    public GetDependenciesOutput getDependencies(GetDependenciesInput input) throws Exception{
        
        //Default to 1 if no parameter is present.
        long dbType = 1;
        if(input.getParameters().containsKey("DBType")){
            dbType = Long.parseLong(input.getParameters().get("DBType"));
        }
        
        CompilerOrchestrator orchestrator = new CompilerOrchestrator(compilerConfiguration, dataStoreManager,
                ghqFactoryFactory, datanetServiceWrapper, getMetrics());
        JSONObject ghqJSON = new JSONObject(input.getGhqJSON());
        CompilerOrchestrator.CompileOutput compileOutput =  orchestrator.compile(ghqJSON, dbType);
        if(compileOutput.validationError!=null && !compileOutput.validationError.isEmpty()){
            throw new CompilerServiceException("Validation failed:"+compileOutput.validationError);
        }
        
        GetDependenciesOutput output = new GetDependenciesOutput();
        output.setDBtype(dbType);
        List<String> listStrings = new ArrayList<String>();
        for (Table table : compileOutput.dependencies) {
            listStrings.add(table.getPhysicalName());
        }
        output.setDependencies(listStrings);
        return output;
    }
    
    @CompileAspectAnnotation( attemptMetricName=METRIC_NAME_COMPILE_REQUEST, failureMetricName=METRIC_NAME_COMPILE_FAILED)
    @Operation("getNativeQuery")
    public GetNativeQueryOutput getNativeQuery (GetNativeQueryInput input) throws Exception {
        
        // TODO: Log the profileId and jobId, parse and use the optional parameters (TBD)
        
        // Call the "compile" Coral operation
        CompilerInputStructure compilerInput = new CompilerInputStructure();
        compilerInput.setDBtype(input.getDBtype());
        compilerInput.setGhqJSON(input.getGhqJSON());
        CompilerOutputStructure compilerOutput = innerCompile(compilerInput);
        
        GetNativeQueryOutput nativeQueryOutput = new GetNativeQueryOutput();
        nativeQueryOutput.setNativeQuery(compilerOutput.getCompilerOutput());
        return nativeQueryOutput;
    }
    
    @CompileAspectAnnotation( attemptMetricName=METRIC_NAME_VALIDATION_REQUEST, failureMetricName=METRIC_NAME_VALIDATION_FAILED)
     @Operation("validateSubExpression")
    public ValidateSubExpressionOutput validateSubExpression(CompilerInputStructure input) throws CompilerServiceException, JSONException{
        ValidateSubExpressionOutput output = new ValidateSubExpressionOutput();
        // Current systems that accept inner-query: Oracle, Redshift
        if(input.getDBtype() == DBType.SQL.getId() || input.getDBtype() == DBType.REDSHIFT.getId()) {
            try{
                CompilerOrchestrator orchestrator = new CompilerOrchestrator(compilerConfiguration, dataStoreManager,
                        ghqFactoryFactory, datanetServiceWrapper, getMetrics());
                CompilerOrchestrator.ValidationOutput validationOutput = orchestrator.validateSubExpression(input.getGhqJSON(), input.getDBtype());
                if((validationOutput.validationError != null) && (validationOutput.validationError.trim().length() > 0)){
                    output.setIsValid(false);
                    output.setValidationError(validationOutput.validationError);
                }
                else{
                    output.setIsValid(true);
                }
                
            }
            catch(CompilerException ex){
                getMetrics().addCount(METRIC_NAME_VALIDATION_FAILED, CompilerServiceAspect.METRIC_INCREMENT_VALUE  , Unit.ONE);
                output.setIsValid(false);
                output.setValidationError("GHQ is not well formatted.");
                LOGGER.debug("GHQ is not well formatted:" + input.getGhqJSON());
                return output;
            }
        } else {
            output.setIsValid(false);
            output.setValidationError("Invalid Database Type: only validate sub-expression for Oracle and Redshift");
        }
        
        return output;
    }
    
    // Validate method currently not used publicly, but can easily be exposed if desired
    @CompileAspectAnnotation( attemptMetricName=METRIC_NAME_VALIDATION_REQUEST, failureMetricName=METRIC_NAME_VALIDATION_FAILED)
    public CompilerOutputStructure validate(CompilerInputStructure CompilerInput)  throws Exception{
        CompilerOutputStructure output = new CompilerOutputStructure();
        JSONObject ghqJSON = new JSONObject(CompilerInput.getGhqJSON());
        
        CompilerOrchestrator orchestrator = new CompilerOrchestrator(compilerConfiguration, dataStoreManager,
                ghqFactoryFactory, datanetServiceWrapper, getMetrics());
        CompilerOrchestrator.ValidationOutput validationOutput = orchestrator.validate(ghqJSON, CompilerInput.getDBtype());
        output.setCompilerOutput(validationOutput.validationError);
        return output;
    }
    
    
    //Allows Compiler to return various suggestions about the query to the user, which they can use to enhance their query
    protected JSONObject getSuggestions(CompilerOrchestrator.CompileOutput output) throws JSONException{
        JSONObject toReturn = new JSONObject();
        
        toReturn.put("partitionKeys", output.missingPartitionKeys);
         
        return toReturn;
    }
    
    
    @Operation("compareGHQs")
    public CompareGHQsOutput compareGHQs(CompareGHQsInput input) throws Exception{
        
        JSONObject jsonOne = new JSONObject(input.getGhqOneJSON());
        JSONObject jsonTwo = new JSONObject(input.getGhqTwoJSON());
        MetaDataStore repository = dataStoreManager.getStore(getBIGroupFromGHQJSON(jsonOne));
        BaseGHQFromJSONFactory ghqFactory = ghqFactoryFactory.generateFactory(repository);
        
        
        GHQuery queryOne = ghqFactory.generateQuery(jsonOne, 0);
        GHQuery queryTwo = ghqFactory.generateQuery(jsonTwo, 0);
        
        CompareGHQsOutput output = new CompareGHQsOutput();
        output.setIsSame(queryOne.equals(queryTwo));
        return output;
       
    }
    
    protected String getBIGroupFromGHQJSON(JSONObject ghqjSON){
        return ghqjSON.optString("metaDataVersion","grasshopper");
    }
    
    @CompileAspectAnnotation( attemptMetricName=METRIC_NAME_COMPILE_REQUEST, failureMetricName=METRIC_NAME_COMPILE_FAILED)
    @Operation("convertDSSToGHQ")
    @Validated
    public ConvertDSSToGHQOutput convertDSSToGHQ(ConvertDSSToGHQInput input) throws Exception{
        ConvertDSSToGHQOutput output = new ConvertDSSToGHQOutput();
        GHQFromDSSXMLFactory ghqFactory = new GHQFromDSSXMLFactory(dataStoreManager.getStore("grasshopper"));
        SAXBuilder builder = new SAXBuilder();
        InputStream stream = null;
        try{
            stream = IOUtils.toInputStream(input.getDssXML());
            Document document = (Document) builder.build(stream);
            Element queryRoot = document.getRootElement().getChild("QUERY");   
            
            if(document.getRootElement().getAttributeValue("UNIVERSE") != null &&
                    document.getRootElement().getAttributeValue("UNIVERSE").equals("Consolidated ASIN")){
                GHQuery ghq = null;
                ghq = ghqFactory.generateQuery(queryRoot, DBType.SQL.getId(), input.getDssXML());
                if(ghq != null){
                    CompilerInputStructure compileInput = new CompilerInputStructure();
                    compileInput.setDBtype(DBType.SQL.getId());
                    String ghqString = ghq.toJSON().toString();
                    compileInput.setGhqJSON(ghqString);
                    CompilerOutputStructure compileOutput = null;
                    try{
                        compileOutput = innerCompile(compileInput);
                        output.setSqlString(compileOutput.getCompilerOutput());
                        output.setGhqString(ghqString);
                        output.setCompilerQualitySuggestions(compileOutput.getCompilerQualitySuggestions());
                        output.setConversionLog(ghqFactory.getCurrentRunData().getLog());
                    }
                    catch(Exception ex){
                        throw new CompilerServiceException("Failed compilation of converted DSS query.", ex);
                    }
                 }
                else{
                    throw new CompilerServiceException("Failed converting DSS-XML to GHQuery:" + ghqFactory.getCurrentRunData().getLog());
                }
            }
            else{
                throw new CompilerServiceException("Input DSS-XML is not supported - not a Consolidated ASIN template.");
            }
        }
        finally{
            if(stream!=null)
            {
                try{stream.close();} catch(Exception ex) { LOGGER.error("Error closing stream", ex);}
            }
        }
        return output;
    }
}


