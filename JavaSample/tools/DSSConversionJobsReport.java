package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class DSSConversionJobsReport {

    /**
     * @param args
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
       
       
        String xmlDirectory = args[0];
        String ghDirectory = args[1];
        SAXBuilder builder = new SAXBuilder();
        File directory = new File(xmlDirectory);
        File ghdirectoryFile = new File(ghDirectory);
        
        HashMap<Long, String> convertedFile = new HashMap<Long, String>();
        
        for(File file : ghdirectoryFile.listFiles()){
            if(file.getName().contains(".gh")){
                try{
                    String[] namePart = file.getName().split("_");
                    long jobId = Long.parseLong(namePart[0]);
                    convertedFile.put(Long.valueOf(jobId), file.getName());
                }
                catch(Exception ex){
                    System.err.println("Error handling file:" + file.getName() +"-"+ex.toString());
                }
            }
        }
        
        HashMap<Long, String> jobsDetails = new HashMap<Long, String>();
        HashSet<String> tzOptions = new HashSet<String>();
        HashSet<String> users = new HashSet<String>();
        HashSet<Long> failedConversions = new HashSet<Long>();
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        HashSet<String> universes = new HashSet<String>();
        
        for(File file : directory.listFiles()){
            if(file.getName().contains(".xml")){
                try{
                    Document document = (Document) builder.build(file);
                    Element queryRoot = document.getRootElement().getChild("QUERY");
                    String areaName = queryRoot.getAttributeValue("DESC");
                    if(document.getRootElement().getAttributeValue("UNIVERSE") == null ||
                            document.getRootElement().getAttributeValue("UNIVERSE").equals("Consolidated ASIN")==false)
                    {
                        String key = document.getRootElement().getAttributeValue("UNIVERSE") + "-" + areaName;
                        universes.add(key);
                        continue;
                    }
                    
                    String[] namePart = file.getName().split("_");
                    long jobId = Long.parseLong(namePart[0]);
                    if(convertedFile.containsKey(Long.valueOf(jobId))==false){
                        failedConversions.add(Long.valueOf(jobId));
                        if(map.containsKey(areaName)==false)
                            map.put(areaName, 1);
                        else{
                            Integer oldVal = map.get(areaName);
                            oldVal++;
                            map.put(areaName, oldVal);
                        }
                        continue;
                    }
                       
                    //Parse query file
                    String ghFile = convertedFile.get(Long.valueOf(jobId));
                    String timeZone = queryRoot.getAttribute("TZ").getValue();
                    String dwpJobDescription = queryRoot.getAttribute("DWP_JOB_DESCRIPTION").getValue();
                    String dir = queryRoot.getAttributeValue("DWP_FILENAME");
                    int i =dir.lastIndexOf("/");
                    dir = dir.substring(0, i);
                    i=dir.lastIndexOf("/");
                    int j = dir.lastIndexOf("users/");
                    String name = dir.substring(j+6, i);
                    users.add(name);
                    tzOptions.add(timeZone);
                    String values = "";
                    List<Element> restirctions = new ArrayList<Element>();
                    if(queryRoot.getChild("AND")!=null)
                        restirctions.addAll(queryRoot.getChild("AND").getChildren("RESTRICTION"));
                    if(queryRoot.getChild("OR")!=null && queryRoot.getChild("OR").getChild("AND")!=null){
                        restirctions.addAll(queryRoot.getChild("OR").getChild("AND").getChildren("RESTRICTION"));
                    }
                    for(Element elem : restirctions){
                        if(elem.getAttribute("COL_ID")!=null && elem.getAttribute("COL_ID").getValue().equalsIgnoreCase("marketplace_id") && elem.getAttributeValue("VALUES")!=null){
                            values = elem.getAttributeValue("VALUES");
                            values = values.replace("\'", "");
                        }
                    }
                    timeZone = translateTZ(timeZone);
                    String desciption = "TZ="+timeZone +"\tJobID:"+jobId +"\tDir:"+dir+"\tUser:"+name+"\tJobName:"+dwpJobDescription+"\tGHFile:"+ghFile;
                    if(values.length() > 0)
                        desciption+="\tMarketPlaceIDS:"+values;
                    jobsDetails.put(Long.valueOf(jobId), desciption);
                    //System.out.println(desciption);
                    
                }
                catch(Exception ex){
                    System.err.println("Error handling file:" + file.getName() +"-"+ex.toString());
                    
                }
            }
        }
        System.out.println("Total Number of Jobs:"+jobsDetails.size());
        System.out.println("Total Number of converted Jobs:"+convertedFile.size());
        System.out.println("Total Number of failed Jobs:"+failedConversions.size());
        System.out.println("Total Number of users:"+users.size());
        System.out.println("Total Number of TZ:"+tzOptions.size() + "-" + tzOptions.toString());
        //System.out.println("Total Number of TZ:"+tzOptions.size());
        for(Entry<String, Integer> entry : map.entrySet()){
            System.out.println(entry.getKey() + "-" + entry.getValue().toString());
        }
        System.out.println(failedConversions.toString());
        System.out.println("Other Universes:" + universes.toString());
        System.out.println("-----------------------------------------------------------------------");
        for(Entry<Long, String> entry : jobsDetails.entrySet()){
            System.out.println(entry.getValue());
       }
    }

    private static String translateTZ(String timeZone) {
        // TODO Auto-generated method stub
        if(timeZone.equals("CET") || timeZone.equals("MEZ"))
            return "Europe/Paris";
        if(timeZone.equals("JST") || timeZone.equalsIgnoreCase("GMT+09:00"))
            return "Asia/Tokyo";
        if(timeZone.equals("GMT"))
            return "Europe/London";
        else
            return "America/Los_Angeles";
    }

}
