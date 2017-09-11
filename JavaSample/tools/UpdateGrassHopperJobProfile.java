package com.amazon.dw.grasshopper.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import amazon.odin.awsauth.OdinAWSCredentialsProvider;
import amazon.odin.awsauth.OdinRefreshConfiguration;

import com.amazon.coral.spring.Application;
import com.amazon.dw.grasshopper.compilerservice.CompilerConfiguration;
import com.amazon.dw.grasshopper.compilerservice.ghqfactory.GHQFromJSONFactory;
import com.amazon.dw.grasshopper.tools.GHQModifiers.ReplaceAttributesModifier;
import com.amazon.dw.grasshopper.tools.ghqmodifiers.AbstractGHQModifier;
import com.amazon.dw.grasshopper.util.aws.S3FileSaver;
import com.amazonaws.services.s3.AmazonS3Client;

import org.json.JSONException;
import org.json.JSONObject;

public class UpdateGrassHopperJobProfile {

    public static final String UpdateCommand = "/apollo/env/DatanetServiceClientKerberos/bin/datanet --method POST --field jobProfile --uri jobProfile/GRASSHOPPER/<job_profile_id> --inputFile <filename> --endPoint <datanetUrl>";
    public static final String GetCommand =    "/apollo/env/DatanetServiceClientKerberos/bin/datanet --method GET --field jobProfile --uri jobProfile/GRASSHOPPER/<job_profile_id> --endPoint <datanetUrl>" ;
    public static final String SetSudo = "/apollo/env/SDETools/bin/curl --insecure -X POST --header Accept:application/json --header Accept-Charset:ISO-8859-1,utf-8 --header Content-Encoding:amz-1.0 --header Content-Type:application/json;charset=UTF-8 --header X-Amz-Target:com.amazon.dw.security.DWPSecurityService.setSudo  -d @<filename> <securityUrl>";
    private static final int DAY_IN_SECONDS = 82800;
    
    public static void main(String[] args) {
       if(args.length == 0){
            System.out.println("UpdateGrassHopperJobProfile\n-------------------------------");
            System.out.println("You can use a file to update a batch of job-profiles, or update a single profile");
            System.out.println("Arguments:");
            System.out.println("Single-Profile-Update:\t profileID userName GHQ-Modifier-Class-Name datanet-service-url DWPSecurityService-url S3-bucket (optional) AWS-Odin-material-set-name (optional)");
            System.out.println("Batch-Update:\t FileNameContainingListOfProfileIdSeperatedByNewLines GHQ-Modifier-Class-Name datanet-service-url DWPSecurityService-url S3-bucket (optional) AWS-Odin-material-set-name (optional)");
            return;
        }
        
        try{
            
            long jobProfileId = -1;
            String userName = null;
            String jobProfileListFileName = null;
            String className = null;
            String s3BucketName = null;
            String datanetUrl;
            String securityServiceUrl = null;
            String awsMaterialSetName = null;
            
            if(args[0].matches("[0-9]+")){
                jobProfileId = Long.parseLong(args[0]);
                userName = args[1];
                className = args[2].trim();
                datanetUrl = args[3].trim();
                securityServiceUrl = args[4].trim();
                if(args.length>=6)
                    s3BucketName = args[5].trim();
                if(args.length >= 7) {
                    awsMaterialSetName = args[6].trim();
                }
            }
            else{
                jobProfileListFileName = args[0];
                className = args[1].trim();
                datanetUrl = args[2].trim();
                securityServiceUrl = args[3].trim();
                if(args.length>=5) {
                    s3BucketName = args[4].trim();
                }
                
                if(args.length >= 6) {
                    awsMaterialSetName = args[5].trim();
                }
            }
            
                        
            Class<?> ghqModifier = Class.forName(className);
            if(AbstractGHQModifier.class.isAssignableFrom(ghqModifier)==false){
                System.err.println("GHQ Modifier class must extend com.amazon.dw.grasshopper.tools.AbstractGHQModifier");
                return;
            }
            
            UpdateGrassHopperJobProfile updater = new UpdateGrassHopperJobProfile(jobProfileId, userName,
                    jobProfileListFileName, ghqModifier, datanetUrl, securityServiceUrl,  s3BucketName,
                    awsMaterialSetName);
            updater.updateProfiles();
        }
        catch(Exception ex){
            ex.printStackTrace();
            return;
        }
    }
    
    
    ArrayList<Long> profiles  ;
    ArrayList<String> usernames;
    String s3Bucket;
    Class<?> ghqModifierClass;
    String datanetUrl;
    String securityServiceUrl;
    String materialSetName;
    
    public UpdateGrassHopperJobProfile(long profileId,String userName, String profileFiles, Class<?> clazz,
            String datanetUrl, String securityServiceUrl, String s3Bucket, String awsMaterialSetName){
        profiles = new ArrayList<Long>();
        usernames = new ArrayList<String>();
        this.s3Bucket = s3Bucket;
        this.ghqModifierClass = clazz;
        this.securityServiceUrl = securityServiceUrl;
        this.datanetUrl = datanetUrl;
        this.materialSetName = awsMaterialSetName;
        
        if(profileId > 0){
            profiles.add(Long.valueOf(profileId));
            usernames.add(userName);
        }
        else{
            FileInputStream str = null;
            try{
                str = new FileInputStream(profileFiles);
                List<String> lines= IOUtils.readLines(str);
                for(String s : lines){
                    String[] words = s.split(" ");
                    if(words[0].trim().matches("[0-9]+")){
                        profiles.add(Long.valueOf(words[0].trim()));
                        usernames.add(words[1].trim());
                    }
                }
            } catch(FileNotFoundException ex){
                ex.printStackTrace();
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
            finally{
                if(str!=null)
                    try {
                        str.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
            }
        }
    }
    
    public void updateProfiles(){
        ArrayList<Long> failed = new ArrayList<Long>();
        ArrayList<Long> succeeded = new ArrayList<Long>();
        Long l = Long.valueOf(-1);
        for(int i=0;i<profiles.size();i++){
            try{
                l = profiles.get(i);
                String userName = usernames.get(i);
                if(changeUser(userName)){
                    updateSingleProfile(l.longValue());
                    succeeded.add(l);    
                }
                else{
                    failed.add(l);
                }
                
            }
            catch(Exception ex){
                System.out.println(ex);
                failed.add(l);
            }
        }
        System.out.println("Success:"+ succeeded.toString());
        System.out.println("Failed:"+ failed.toString());
    }
    
    private boolean changeUser(String userName) throws JSONException, IOException{
        JSONObject obj = new JSONObject();
        obj.put("user", System.getProperty("user.name"));
        obj.put("sudoUser", userName);
        File f = File.createTempFile("GH_UpdateGHQ_SetSudoInput_", ".tmp");
        FileOutputStream str = new FileOutputStream(f);
        IOUtils.write(obj.toString(), str);
        str.close();
        String command = SetSudo.replace("<filename>", f.getAbsolutePath()).replace("<securityUrl>", securityServiceUrl);
        StringBuilder builder = new StringBuilder();
        try{
        Process p = Runtime.getRuntime().exec(command);
        BufferedReader input =
            new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = null;
        while((line = input.readLine()) != null){
            builder.append(line);
        }
        input.close();
        }
        catch(Exception ex){
            System.out.println("Error changing to user: "+ userName);
            ex.printStackTrace();
            return false;
        }
        obj = new JSONObject(builder.toString());
        if(obj.has("success"))
            return obj.getBoolean("success");
        return false;
    }
    
    private void updateSingleProfile(long profileId) throws Exception{
        String s = getExistingProfile(profileId);
        File originalProfile = new File("/tmp/GH_TEMP_OriginalProfile_"+Long.toString(profileId) + ".dnp");
        writeContentToFile(originalProfile, s);
        JSONObject obj = new JSONObject(s);
        obj.remove("versionAttributes");
        obj.remove("revision");
        JSONObject newGHQ = modifyGHQ(obj.getString("sql"));
        obj.put("sql", newGHQ.toString());
        saveToDatanet(obj, profileId);
    }
    
    private JSONObject modifyGHQ(String sql) throws Exception{
        AbstractGHQModifier modifier = (AbstractGHQModifier)ghqModifierClass.newInstance();
        String newString = modifier.modify(sql);
        JSONObject json = new JSONObject(newString);
        return modifier.modify(json);
    }
    
    private String getExistingProfile(long profileId) throws Exception {
        String command = GetCommand.replace("<job_profile_id>", Long.toString(profileId)).replace("<datanetUrl>", datanetUrl);
        
        StringBuilder builder = new StringBuilder();
        Process p = Runtime.getRuntime().exec(command);
        BufferedReader input =
            new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = null;
        boolean jsonStarted = false;
        while((line = input.readLine()) != null){
            if(line.trim().equalsIgnoreCase("{"))
                jsonStarted = true;
            if(jsonStarted)
                builder.append(line);
        }
        input.close();
        return builder.toString();
    }
    
    private void saveToDatanet(JSONObject obj, long profileId) throws Exception{
        File tempFile = File.createTempFile("GH_TEMP_Update_Profile_"+Long.toString(profileId)+"_", ".dnp");
        writeContentToFile(tempFile, obj.toString());
        String command = UpdateCommand.replace("<job_profile_id>", Long.toString(profileId)).replace("<filename>", tempFile.getAbsolutePath()).replace("<datanetUrl>", datanetUrl);;
        
        Process p = Runtime.getRuntime().exec(command);
        BufferedReader input =
            new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = null;
        boolean jsonStarted = false;
        boolean isSuccess = false;
        StringBuilder builder = new StringBuilder();
        while((line = input.readLine()) != null){
            builder.append(line);
            if(line.trim().equalsIgnoreCase("{"))
                jsonStarted = true;
            if(jsonStarted && line.contains("revision"))
                isSuccess = true;
        }
        input.close();
        if(isSuccess==false){
            System.err.println("Failed updating profile:"+ builder.toString());
            throw new RuntimeException("Failed updating profile:"+ builder.toString());
        }
        
        changeUser("");
        if(s3Bucket!=null && materialSetName != null){
            String fileTargetName = String.format("GH_Updated_Profile_%d_%d.ghq",profileId, System.currentTimeMillis());
            OdinRefreshConfiguration refreshConfig = new OdinRefreshConfiguration(DAY_IN_SECONDS);
            S3FileSaver fileSaver = new S3FileSaver(new AmazonS3Client(new OdinAWSCredentialsProvider(materialSetName, refreshConfig)));
            fileSaver.saveFile(s3Bucket, tempFile, fileTargetName);
        }
    }
    
    public static boolean writeContentToFile(File output, String s){
        FileOutputStream str = null;
        try{
            str = new FileOutputStream(output);
            IOUtils.write(s, str);
            str.flush();
            str.close();
            return true;
        }
        catch(Exception ex){
            return false;
        }
        finally{
            if(str!=null)
                try {
                    str.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
        
    }

}
