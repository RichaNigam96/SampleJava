package com.amazon.dw.grasshopper.tools;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.IOUtils;

import com.amazon.coral.client.ClientBuilder;
import com.amazon.dw.grasshopper.CompilerInputStructure;
import com.amazon.dw.grasshopper.CompilerOutputStructure;
import com.amazon.dw.grasshopper.compilerservice.CompilerServiceClient;


@SuppressWarnings("rawtypes")
public class CompilationRegressionTests extends DirectoryWalker implements Runnable {
    
    CompilerServiceClient client1;
    CompilerServiceClient client2;
    File   file;
    List<String> files = new ArrayList<String> ();
    List<String> failedInFiles = new ArrayList<String>();
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception{
        String ep1 = null;
        String ep2 = null;
        String path = null;
        if(args.length != 3){
            System.out.println("CompilationRegressionTest Bad Arguments");
            System.out.println("Usage: CompilationRegressionTest <EndPoint1> <EndPoint2> <GHQ file/s path>");
            System.out.println("\nExample: https://localhost:20043/CompilerService https://grasshopper-service.integ.amazon.com/CompilerService .");
            return;
            
        }
        else{
            ep1 =  args[0];
            ep2 = args[1];
            path = args[2];
        }
        
        try{
            CompilationRegressionTests regressionTest = new CompilationRegressionTests(ep1, ep2, path);
            regressionTest.run();
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
    }

    
    @SuppressWarnings("unchecked")
    public CompilationRegressionTests(String endPoint1, String endPoint2, String path){
        super();
        String config1 = "CompilerService#Custom : { "
                        + " \"protocols\" : [\"rpc\"], "
                + "  \"httpEndpoint\" : { "
                + "    \"url\" : \"" + endPoint1 + "\""
                + "  } "
                + "}";
        
        String config2 = "CompilerService#Custom : { "
                + " \"protocols\" : [\"rpc\"], "
        + "  \"httpEndpoint\" : { "
        + "    \"url\" : \"" + endPoint2 + "\""
        + "  } "
        + "}";
        ClientBuilder cb1 = new ClientBuilder(new ByteArrayInputStream(config1.getBytes()));
        ClientBuilder cb2 = new ClientBuilder(new ByteArrayInputStream(config2.getBytes()));
        client1 = cb1.remoteOf(CompilerServiceClient.class).withConfiguration("Custom").newClient();       
        client2 = cb2.remoteOf(CompilerServiceClient.class).withConfiguration("Custom").newClient();        
        file = new File(path);
        try{
            if(file.isDirectory())
                walk(file, files);
            else
                files.add(file.getAbsolutePath());
        }catch(Exception ex) {}
    }
    
    @Override
    public void run() {
        CompilerInputStructure input = new CompilerInputStructure();
        for(String s : files){
            String fileContent = getGHQ(s);
            if(fileContent == null){
                this.failedInFiles.add(s);
                continue;
            }
                
            input.setGhqJSON(fileContent);
            input.setDBtype(1);
            CompilerOutputStructure output1 = null, output2 = null;
            boolean errorOccured = false;
            try{
                output1 = client1.newCompileCall().call(input);
            }
            catch(Exception ex){
                System.out.println(s+": Failed running GHQ on EndPoint1");
                System.out.println(ex.getMessage());
                errorOccured = true;
            }
            
            try{
                output2 = client2.newCompileCall().call(input);
            }
            catch(Exception ex){
                System.out.println(s+": Failed running GHQ on EndPoint2");
                System.out.println(ex.getMessage());
                errorOccured = true;
            }
            if(errorOccured){
                this.failedInFiles.add(s);
                continue;
            }
            
            if(output1.getCompilerOutput().equalsIgnoreCase(output2.getCompilerOutput())==false){
                System.out.println(s+":---------------------Output don't match-----------------");
                System.out.println(s+":"+output1.getCompilerOutput() + "\n" + output2.getCompilerOutput());
                System.out.println("-----------------------------------------------------------");
                failedInFiles.add(s);
            }
            else
                System.out.println(s+":Output match");
        }
        
    }
    
    protected String getGHQ(String path) throws RuntimeException{
       
        List<String> ghqs = null;
        try{
            ghqs = IOUtils.readLines(new FileInputStream(path));
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        
        if(ghqs != null ){
            StringBuilder builder = new StringBuilder();
            for(String s : ghqs)
                builder.append(s);
            return builder.toString();
        }
            
        else return null;
    }
    
    
      protected boolean handleDirectory(File directory, int depth, Collection results) {
          return true;
      }

      protected void handleFile(File file, int depth, Collection results) {
          if(file.getName().toLowerCase().endsWith(".ghq") || file.getName().toLowerCase().endsWith(".gh")){
            files.add(file.getAbsolutePath());
        }
         
      }
}
