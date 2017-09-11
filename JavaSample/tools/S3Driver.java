package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.io.ObjectInputStream;
import java.util.List;

import amazon.odin.OdinLocal;
import amazon.odin.OdinLocalClient;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;
import amazon.platform.config.AppConfig;

import com.amazon.dw.grasshopper.model.Function;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;

public class S3Driver {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // Setting up AppConfig - Only necessary when executing this code not within a service/JUnit.
        // for running: pass arg: /apollo/env/GrassHopperService
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
        
        // connecting to Odin & AWS
        try{
            
            OdinLocal bim = OdinLocalClient.getService();
            AWSCredentialsProvider provider = new OdinAWSCredentialsProvider("com.amazon.access.GrassHopper-beta-bi-apps-gh-aws-1");
            
            AWSCredentials cred = provider.getCredentials(); 
            AmazonS3 s3 = new AmazonS3Client(cred);
            
            for(Bucket bucket : s3.listBuckets()){
                System.out.println(bucket.getName() + "-" + bucket.getOwner());
                
            }
            
            //read a file, and immediately close it after use.
            S3Object obj = s3.getObject("metadata.grasshopper.amazon.com", "functions.gh");
            System.out.println("File Length:"+obj.getObjectMetadata().getContentLength());
            ObjectInputStream stream = new ObjectInputStream(obj.getObjectContent());
            @SuppressWarnings("unchecked")
            List<Function> functions = (List<Function>)stream.readObject();
            stream.close();
                        
            for(Function function : functions)
                System.out.println(function.getId() +"-"+  function.getDescription());
            
        }
        catch(Exception ex){
           ex.printStackTrace();
        }
        
        
        
    }

}
