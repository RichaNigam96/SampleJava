package com.amazon.dw.grasshopper.tools;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


import org.apache.commons.io.IOUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.FileSystemResource;


import com.amazon.coral.client.ClientBuilder;
import com.amazon.dw.grasshopper.CompilerInputStructure;
import com.amazon.dw.grasshopper.CompilerOutputStructure;
import com.amazon.dw.grasshopper.CompilerServiceException;
import com.amazon.dw.grasshopper.compilerservice.CompilerServiceClient;
import com.amazon.dw.grasshopper.tools.GHQFromDSSXMLFactory;
import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.GHQuery.GHQuery;
import org.json.JSONException;

public class DSSUIImport {

    private static String xmlDirectory = null;
    private static String outputDirectory = null;

    private static CompilerServiceClient client1;
    private static String endPoint1 = null;
    private static String config1 = null;

    
    public static void main(String [] args){
        GenericApplicationContext ctx = new GenericApplicationContext();
        XmlBeanDefinitionReader xmlReader = new XmlBeanDefinitionReader(ctx);
        FileSystemResource resource = new FileSystemResource(new File("spring-configuration/unit-testing-config.xml"));
        xmlReader.loadBeanDefinitions(resource);
        ctx.refresh();

        if(args.length != 3){
            System.out.println("DSSUIImport Bad arguments!");
            System.out.println("Usage: <InputDirectory> <OutputDirectory> <CompilerServiceEndPoint>");
            System.out.println("Usage: <InputFile> <OutputDirectory> <CompilerServiceEndPoint>");
            System.out.println("Example: /home/srebrnik/xml /home/srebrnik/gh_files/ https://localhost:20043/CompilerService");
            System.out.println("Example: /home/srebrnik/xml/dssXmlInput.xml /home/srebrnik/gh_files/ https://localhost:20043/CompilerService");
            return;
        }
        xmlDirectory = args[0];
        outputDirectory = args[1];
        if(outputDirectory.endsWith("/")==false)
            outputDirectory = outputDirectory + "/";
        endPoint1 = args[2];
        config1 = "CompilerService#Custom : { "
                + " \"protocols\" : [\"rpc\"], "
        + "  \"httpEndpoint\" : { "
        + "    \"url\" : \"" + endPoint1 + "\""
        + "  } "
        + "}";
        
        try{
            ClientBuilder cb1 = new ClientBuilder(new ByteArrayInputStream(config1.getBytes()));
            client1 = cb1.remoteOf(CompilerServiceClient.class).withConfiguration("Custom").newClient();       
        }
        catch(Exception ex){
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        SAXBuilder builder = new SAXBuilder();
        File directory = new File(xmlDirectory);
        
        GHQFromDSSXMLFactory ghqFactory = new GHQFromDSSXMLFactory(new InMemoryRepository());

        HashMap<String, Integer> unfoundColumns = new HashMap<String, Integer>();
        int successCounter = 0;
        int failureCounter = 0;
        
        HashMap<String, Integer> universesCount = new HashMap<String, Integer>();
        HashMap<String, Integer> alternativeAreasCount = new HashMap<String, Integer>();
        HashSet<String> perfectConversionUsers = new HashSet<String>();
        int noUniverseAttribute = 0;
        int perfectConversion = 0;
        // this code is useful to convert a single file
        // this is left only for Eclipse debugging.
        if(directory.isFile()){
            File singleFile = directory;
            try{
            Document document = (Document) builder.build(singleFile);

            Element queryRoot = document.getRootElement().getChild("QUERY");   
            
            if( (document.getRootElement().getAttributeValue("UNIVERSE") != null &&
                    document.getRootElement().getAttribute("UNIVERSE").getValue().equalsIgnoreCase("Consolidated ASIN"))
                    || 
                    (document.getRootElement().getAttributeValue("UNIVERSE") != null &&
                            document.getRootElement().getAttribute("UNIVERSE").getValue().equalsIgnoreCase("Original")
                            && queryRoot.getAttribute("DESC").getValue().equalsIgnoreCase("Vendor Receipt Analysis"))){
                GHQuery ghq = null;
                
                 ghq = ghqFactory.generateQuery(queryRoot, DBType.SQL.getId(), getText(singleFile));
                 String queryStr = "";
                 CompilerInputStructure input = new CompilerInputStructure();
                 input.setDBtype(1);
                 queryStr = ghq.toJSON().toString();
                 input.setGhqJSON(ghq.toJSON().toString());
                 System.out.println(queryStr);
                 CompilerOutputStructure output =  client1.newCompileCall().call(input);
                 System.out.println(output.getCompilerOutput());
                 
                }
            }
            catch(Exception ex){
                System.out.println(singleFile.getName() + " FAILED, " +  ex.getMessage());
                ex.printStackTrace();
            }
            return;
        }

        HashMap<Long, Long> failuresHits = new HashMap<Long, Long>();
        HashMap<Long, Long> usedHits = new HashMap<Long, Long>();
        for(File file : directory.listFiles()){
            if(file.getName().contains(".xml")){
                        try{
                              
                            // Parse query file
                        Document document = (Document) builder.build(file);

                        Element queryRoot = document.getRootElement().getChild("QUERY");   
                        if(document.getRootElement().getAttributeValue("UNIVERSE") == null){
                            noUniverseAttribute++;
                        }
                        if( document.getRootElement().getAttributeValue("UNIVERSE") != null &&
                                document.getRootElement().getAttributeValue("UNIVERSE").equals("Consolidated ASIN")==false){
                            String key = document.getRootElement().getAttributeValue("UNIVERSE");
                            if(universesCount.containsKey(key)){
                                Integer val = universesCount.get(key);
                                universesCount.put(key,Integer.valueOf(val+1));
                            }
                            else
                                universesCount.put(key, Integer.valueOf(1));
                            
                            key = document.getRootElement().getAttributeValue("ID");
                            if(key!=null){
                                if(alternativeAreasCount.containsKey(key)){
                                    Integer val = alternativeAreasCount.get(key);
                                    val = val++;
                                    alternativeAreasCount.put(key,Integer.valueOf(val+1));
                                }
                                else
                                    alternativeAreasCount.put(key, Integer.valueOf(1));
                            }
                        }
                        if( (document.getRootElement().getAttributeValue("UNIVERSE") != null &&
                                document.getRootElement().getAttributeValue("UNIVERSE").equalsIgnoreCase("Consolidated ASIN"))
                        || (document.getRootElement().getAttributeValue("UNIVERSE") != null &&
                                document.getRootElement().getAttribute("UNIVERSE").getValue().equalsIgnoreCase("Original")
                                && queryRoot.getAttribute("DESC").getValue().equalsIgnoreCase("Vendor Receipt Analysis"))){
                            String text = getText(file);
                            GHQuery ghq = ghqFactory.generateQuery(queryRoot, DBType.SQL.getId(), text);
                            String queryStr = "";
                            CompilerOutputStructure output = null;
                            if(ghq == null){
                                failureCounter++;
                                System.out.println(file.getName() + " FAILED ");
                                addHit(-1, failuresHits);
                                continue;
                            }
                            try{
                                CompilerInputStructure input = new CompilerInputStructure();
                                input.setDBtype(1);
                                queryStr = ghq.toJSON().toString();
                                input.setGhqJSON(ghq.toJSON().toString());
                                output = client1.newCompileCall().call(input);
                                if(ghqFactory.getCurrentRunData().isPerfectConversion()==true){
                                    perfectConversion++;
                                    String dir = queryRoot.getAttributeValue("DWP_FILENAME");
                                    int i =dir.lastIndexOf("/");
                                    dir = dir.substring(0, i);
                                    i=dir.lastIndexOf("/");
                                    int j = dir.lastIndexOf("users/");
                                    String name = dir.substring(j+6, i);
                                    perfectConversionUsers.add(name);
                                }
                                
                                
                            }
                           catch(CompilerServiceException ex){
                               failureCounter++;
                               System.out.println(file.getName() + " FAILED, " +  ex.getMessage());
                               System.out.println(queryStr);
                               addHit(ghq.getAreaId(), failuresHits);
                               continue;
                           }
                            catch(Exception ex){
                                failureCounter++;
                                System.out.println(file.getName() + " FAILED, " +  ex.getMessage());
                                ex.printStackTrace();
                                addHit(ghq.getAreaId(), failuresHits);
                                continue;
                            }
                            addHit(ghq.getAreaId(), usedHits);
                            String newFilename = file.getName().replace(".xml", ".gh");
                            String newFilenameSQL = file.getName().replace(".xml", ".sql");
                            FileWriter outWriter = new FileWriter(outputDirectory + newFilename);
                            FileWriter outWriterSQL = new FileWriter(outputDirectory + newFilenameSQL);
                            BufferedWriter buffWriter = new BufferedWriter(outWriter);
                            BufferedWriter buffWriterSQL = new BufferedWriter(outWriterSQL);
                            try{
                                buffWriter.write(ghq.toJSON().toString());//wrote out as JSON, easy to convert back, easy for website &  compiler
                                buffWriterSQL.write(output.getCompilerOutput());
                                successCounter++;
                                System.out.println(file.getName() + " Succeded.");
                            }
                            catch(JSONException je){
                                failureCounter++;
                                je.printStackTrace();
                            }
                            finally{
                                if(buffWriter != null)
                                    buffWriter.close();
                                if(outWriter != null)
                                    outWriter.close();
                                if(buffWriterSQL != null)
                                    buffWriterSQL.close();
                                if(outWriterSQL != null)
                                    outWriterSQL.close();
                            }
                            
                        }
                    }
                    catch(JDOMException ex){
                        ex.printStackTrace();
                        failureCounter++;
                    }
                    catch(IOException ex){
                        ex.printStackTrace();
                        failureCounter++;
                    }
                    catch(RuntimeException e){
                        if(e.getClass().equals(NumberFormatException.class))
                            e.printStackTrace();
                        //if(e.getLocalizedMessage().equals("Attempting to create a QueryInternalNode with no Filter instances."))
                        System.out.println(file.getName() + " FAILED, " +  e.getMessage());
                        e.printStackTrace();
                        if(unfoundColumns.containsKey(e.getLocalizedMessage()))
                            unfoundColumns.put(e.getLocalizedMessage(), unfoundColumns.get(e.getLocalizedMessage())+1);
                        else{
                            unfoundColumns.put(e.getLocalizedMessage(), 1);
                        }
                        failureCounter++;
                    }  
                }
            }
        

        
        for(java.util.Map.Entry<Long,Long> entry : failuresHits.entrySet()){
            System.out.println("Area "+entry.getKey() + "- " + entry.getValue() + " failures.");
        }
        
        System.out.println("-------Most Missed Attributes--------");
        ghqFactory.printMostMissedAttributes();
        System.out.println("-------------------------------------");
         
        System.out.println("Processing Complete, " + successCounter + "/" + (successCounter + failureCounter) + " succeeded.");
        System.out.println("Perfect conversions:"+ perfectConversion);
        System.out.println("Number of segments: " + ghqFactory.getStatistics().numOfSegments);

        System.out.println("Area use histogram:\n"+usedHits.toString());
        
        System.out.println("Unsupported Universes:\n"+universesCount.toString());
        System.out.println("No Universe:"+noUniverseAttribute);
        System.out.println("Alternative areas histogram:\n"+alternativeAreasCount.toString());
        System.out.println("Users with perfect converted jobs:\n"+perfectConversionUsers.toString());
        
    }

    
    
    private static void addHit(long areaID, HashMap<Long, Long> hits){
        if(hits.containsKey(Long.valueOf(areaID))==false)
            hits.put(Long.valueOf(areaID), Long.valueOf(1));
        else
        {
            
            long curr = hits.get(Long.valueOf(areaID)).longValue();
            hits.put(Long.valueOf(areaID), Long.valueOf(curr+1));
        }
    }
    
    private static String getText(File f){
        FileInputStream str = null;
        try{
            str = new FileInputStream(f);
            List<String> lines = IOUtils.readLines(str);
            StringBuilder builder = new StringBuilder();
            for(String s : lines){
                builder.append(s);
                builder.append("\n");
            }
            return builder.toString();
        }
        catch(FileNotFoundException ex){
            return null;
        }
        catch(RuntimeException ex) {
            return null;
        }
        catch(IOException ex) {
            return null;
        }
        finally{
            if(str!=null)
                try{str.close();} catch(IOException ex){}
        }
    }
}
