package com.amazon.dw.grasshopper.tools;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.util.List;

import amazon.odin.OdinLocal;
import amazon.odin.OdinLocalClient;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;
import amazon.platform.config.AppConfig;

import com.amazon.coral.client.ClientBuilder;
import com.amazon.dw.grasshopper.CompilerInputStructure;
import com.amazon.dw.grasshopper.CompilerOutputStructure;
import com.amazon.dw.grasshopper.compilerservice.CompilerServiceClient;
import com.amazon.dw.grasshopper.model.Function;
import com.amazon.dw.grasshopper.util.aws.S3FileRetriever;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class CompileAllS3Files {

    private static CompilerServiceClient client1;
    private static String endPoint1 = null;
   
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        endPoint1 = args[1];
        
        try{
            String cwd= new File(args[0]).getCanonicalPath();
            String[] args2 = new String[] {"--root="+cwd, "--domain=test", "--realm=USAmazon"};
            AppConfig.initialize("GrassHopperService",null,args2);
           
        }
        catch(Exception ex)
        {
            System.out.println(ex.toString());
            ex.printStackTrace();
            return;
        }
        
        
        
        
        String config1 = "CompilerService#Custom : { "
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
            System.exit(1);
        }
        
        // connecting to Odin & AWS
        try{
            
            OdinLocal bim = OdinLocalClient.getService();
            AWSCredentialsProvider provider = new OdinAWSCredentialsProvider("com.amazon.access.GrassHopper-prod-bi-apps-gh-aws-1");
            
            AWSCredentials cred = provider.getCredentials(); 
            AmazonS3 s3 = new AmazonS3Client(cred);
            
            for(Bucket bucket : s3.listBuckets()){
                System.out.println(bucket.getName() + "-" + bucket.getOwner());
                
            }
            ObjectListing listing = null;
            listing = s3.listObjects("queries.prod.grasshopper.amazon.com");
            S3FileRetriever retriever = new S3FileRetriever(s3);
            boolean shouldRun = true;
            do{
                if(listing==null)
                    break;
                String ghq = null;
                for(S3ObjectSummary summary : listing.getObjectSummaries()){
                    if(summary.getSize() == 0)
                        continue;
                    if(summary.getKey().startsWith("job")==false)
                        continue;
                    try{
                        ghq = retriever.getS3FileText("queries.prod.grasshopper.amazon.com", summary.getKey());
                        CompilerInputStructure input = new CompilerInputStructure();
                        input.setDBtype(1);
                        input.setGhqJSON(ghq);
                        CompilerOutputStructure output =  client1.newCompileCall().call(input);
                        System.out.println("Successfully handled file:"+summary.getKey());
                        }
                    catch(Exception ex){
                        System.out.println("Failed to handle file:"+summary.getKey());
                    }
                }
                if(listing.isTruncated()){
                    listing = s3.listNextBatchOfObjects(listing);
                    shouldRun = true;
                }
                else
                    shouldRun = false;
             }while(shouldRun);
        }
        catch(Exception ex){
           ex.printStackTrace();
        }
        
        
        
    }

}
