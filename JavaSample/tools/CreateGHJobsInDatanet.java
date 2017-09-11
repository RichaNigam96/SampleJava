package com.amazon.dw.grasshopper.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;


import org.apache.commons.io.IOUtils;

import org.json.JSONException;
import org.json.JSONObject;

public class CreateGHJobsInDatanet {

    
    private static final String ADD_PROFILE_COMMAND = "/apollo/env/DatanetServiceClientKerberos/bin/datanet --method POST --field jobProfile --uri jobProfile/GRASSHOPPER --inputFile <filename> --endPoint https://datanet-service-gamma.amazon.com";
    private static final String ADD_JOB_COMMAND = "/apollo/env/DatanetServiceClientKerberos/bin/datanet --method POST --field job --uri job --inputFile <filename> --endPoint https://datanet-service-gamma.amazon.com";
    private static final String securityServiceUrl = "http://datanet-security-gamma.amazon.com";
    public static final String SetSudo = "/apollo/env/SDETools/bin/curl --insecure -X POST --header Accept:application/json --header Accept-Charset:ISO-8859-1,utf-8 --header Content-Encoding:amz-1.0 --header Content-Type:application/json;charset=UTF-8 --header X-Amz-Target:com.amazon.dw.security.DWPSecurityService.setSudo  -d @<filename> <securityUrl>";
    
    private static final String DNP_TEMPLATE = "/home/srebrnik/template1.dnp";
    private static final String DNJ_TEMPLATE = "/home/srebrnik/template.dnj";
    
    
    private static TreeSet<String> getConvertedJob(String jobIdFilePath){
        TreeSet<String> retval = new TreeSet<String>();
        try{
            FileInputStream str = new FileInputStream(new File(jobIdFilePath));
            List<String> lines = IOUtils.readLines(str);
            str.close();
            for(String s : lines){
                retval.add(s.trim());
            }
        }
        catch(Exception ex){System.err.println("Error getConvertedJob:"+ ex.toString());}
        return retval;
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
       
        List<String> lines =  readJobListToCreate(args[0]);
        TreeSet<String> jobIds = getConvertedJob(args[2]);
        String GH_Dir = args[1]; //"/home/srebrnik/gh_files3/";
        for(String input : lines){
            //Example of input line: "TZ=PST  JobID:1804046   Dir:/dss/dss-ui/users/ekauss/results    User:ekauss JobName:Cat 1500 Daily Unit Sales   GHFile:1804046_164628320.gh MarketPlaceIDS:1";
            String[] values = input.split("\t");
            String ghFile = "";
            String timeZone = "";
            String oldJobId = "";
            String desc = "";
            String userName = "";
            int[] mks = null;
            for(String val : values){
                if(val.startsWith("GHFile:"))
                    ghFile = GH_Dir + val.replace("GHFile:", "");
                else if(val.startsWith("TZ"))
                    timeZone = val.replace("TZ=", "");
                else if(val.startsWith("JobID:"))
                    oldJobId = val.replace("JobID:", "");
                else if(val.startsWith("JobName:"))
                    desc = val.replace("JobName:", "");
                else if(val.startsWith("User:"))
                    userName = val.replace("User:", "");
                else if(val.startsWith("MarketPlaceIDS:")){
                    String ids = val.replace("MarketPlaceIDS:", "");
                    String[] idStrArray = ids.split(",");
                    ArrayList<Integer> temp = new ArrayList<Integer>(); 
                    for(int i=0;i<idStrArray.length;i++){
                        try{
                            int newMKid = Integer.parseInt(idStrArray[i]);
                            temp.add(Integer.valueOf(newMKid));
                        }
                        catch(NumberFormatException ex){}
                    }
                    mks = new int[temp.size()];
                    for(int i=0;i<temp.size();i++)
                        mks[i] = temp.get(i).intValue();
                }
            }
            
            if(jobIds.contains(oldJobId)==false)
                continue;
            if(mks!=null && mks.length>0 && ghFile.length() > 0 && oldJobId.length() > 0 && timeZone.length() > 0 && desc.length() > 0){
                Map<String, Long> result = null;
                result = creadeGHJob(ghFile, desc + " (GH conversion of Job:"+oldJobId +")", mks, timeZone, userName);
                if(result != null){
                    String formated = String.format("UserName:%s\tOldJobID:%s\tNewJobID:%d\tNewProfileID:%d", userName, oldJobId, result.get("jobId"), result.get("profileId"));
                    System.out.println(formated);
                }
                else
                    System.out.println("Failed:"+oldJobId);
            }
        }            
    }
    
    private static List<String> readJobListToCreate(String filename){
        FileInputStream str = null;
        try{
            str = new FileInputStream(filename);
            List<String> lines = IOUtils.readLines(str);
            return lines;
        }
        catch(IOException ex){return null;}
        finally{ try{ str.close(); } catch(Exception ex) {ex.printStackTrace();} }
    }
    
    public static Map<String,Long> creadeGHJob(String ghFile, String desc, int[] mkIDS, String timeZone, String userName){
        boolean changedUser = false;
        try{
            changedUser = changeUser(userName);
        }
        catch(Exception ex) { changedUser = false;}
        if(changedUser==false)
            return null;
        int profileID = createProfile(ghFile, desc);
        if(profileID>0){
            int jobId = createJob(profileID, mkIDS, timeZone, desc);
            if(jobId > 0){
                Map<String,Long> retval = new HashMap<String, Long>();
                retval.put("profileId", Long.valueOf(profileID));
                retval.put("jobId", Long.valueOf(jobId));
                return retval;
            }
        }
        return null;
    }
    
    public static String readFileContent(String fileName){
        FileInputStream str = null;
        try{
            str = new FileInputStream(fileName);
            List<String> lines = IOUtils.readLines(str);
            StringBuilder builder = new StringBuilder();
            for(String s2 : lines)
                builder.append(s2);
            return builder.toString();
        }
        catch(FileNotFoundException ex){
            return null;
        }
        catch(IOException ex){
            return null;
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
    
    public static int createProfile(String ghqFilePath, String desc){
        try{
            File outputFile = File.createTempFile("GH_TEMP", ".dnp");
            String dnpTemplate = readFileContent(DNP_TEMPLATE);
            dnpTemplate = dnpTemplate.replace("<desc>", desc);
            String ghq = readFileContent(ghqFilePath);
            dnpTemplate = dnpTemplate.replace("\"<sql>\"", JSONObject.quote(ghq));
            if(writeContentToFile(outputFile, dnpTemplate)==false)
                return -1;
            String command = ADD_PROFILE_COMMAND.replace("<filename>", outputFile.getAbsolutePath());
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader input =
                new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;
           
            int profileId = -1;
            while((line = input.readLine()) != null){
                if(line.contains("\"id\"")){
                   String[] values = line.split(":");
                   profileId = Integer.parseInt(values[1].trim().replace(",", ""));
                }
            }
            input.close();
            return profileId;
        }
        catch(Exception ex){
            ex.printStackTrace();
            return -1;
        }
    }
    
    
    public static int createJob(int profileID, int[] mks, String tz, String desc){
        try{
            int jobId = -1;
            File outputFile = File.createTempFile("GH_TEMP", ".dnj");
            String tz2 = "America/Los_Angeles";
            String pks = generatePKString(mks);
            String jobTemplate = readFileContent(DNJ_TEMPLATE);
            jobTemplate = jobTemplate.replace("<desc>", desc);
            jobTemplate = jobTemplate.replace("<PK>", pks);
            jobTemplate = jobTemplate.replace("<profileID>", Integer.toString(profileID));
            jobTemplate = jobTemplate.replace("<TZ>",tz);
            if(writeContentToFile(outputFile, jobTemplate)==false)
                return -1;
            String command = ADD_JOB_COMMAND.replace("<filename>", outputFile.getAbsolutePath());
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;
            while((line = input.readLine()) != null){
                if(line.contains("\"id\"")){
                   String[] values = line.split(":");
                   jobId = Integer.parseInt(values[1].trim().replace(",", ""));
                }
            }
            input.close();
            return jobId;
        }
        catch(Exception ex){
            ex.printStackTrace();
            return -1;
        }
    }
    
    /*
     * [
        {
            "partitionTypeId": "MARKETPLACE_ID", 
            "partitionValue": 1
        }
    ]
     */
    private static String generatePKString(int[] mks){
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        int counter = 0;
        for(int mk : mks){
            if(counter > 0){
                builder.append(",");
            }
            builder.append("\n{ \"partitionTypeId\": \"MARKETPLACE_ID\",\n\"partitionValue\": "+mk);
            builder.append("\n}");
            counter++;
        }
        builder.append("]");
        
        return builder.toString();
    }
    
    private static boolean changeUser(String userName) throws JSONException, IOException{
        JSONObject obj = new JSONObject();
        obj.put("user", System.getProperty("user.name"));
        obj.put("sudoUser", userName);
        File f = File.createTempFile("GH_UpdateGHQ_SetSudoInput_", ".tmp");
        FileOutputStream str = null;
        try{
            str = new FileOutputStream(f);
            IOUtils.write(obj.toString(), str);
        }
        catch(Exception ex){
            return false;
        }
        finally{
        if(str!=null) 
            try{str.close();} catch(Exception ex){ex.printStackTrace();}
        }
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

}
