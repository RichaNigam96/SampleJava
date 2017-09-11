package com.amazon.dw.grasshopper.tools;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataStorageType;
import com.amazon.dw.grasshopper.model.*;

/**
 * This tool can be use to generate an XML repository dump containing fake data
 * @author shererr
 */
public class MDSizeTest
{
    // The number of each metadata type to generate
    private static final int MAX_AREAS = 300;
    private static final int MAX_CATS = 10000;
    private static final int MAX_TABLES = 50000;
    private static final int MAX_ATTRIBUTES = 250000;
    private static final int MAX_CIS = 5000000;
    
    public static void main(String[] args)
    {
        InMemoryRepository repo = new InMemoryRepository();
        Random rand = new Random();
        
        List<String> tableNames = new ArrayList<String>();
        
        for(int i=0; i<MAX_ATTRIBUTES; i++) {
            repo.putAttribute(new Attribute(i, randomString(), randomString(), false));
        }
        
        for(int i=0; i<MAX_TABLES; i++) {
            String tableName = randomString();
            tableNames.add(tableName);
            Table t = new Table(tableName);
            t.setPhysicalName(randomString());
            t.setFactTable(false);
            repo.putTable(t);
        }
        
        for(int i=0; i<MAX_CATS; i++) {
            Category cat = new Category(i, randomString());
            int num = rand.nextInt(25) + 5;
            for(int j=0; j<num; j++) {
                cat.addAttribute(repo.getAttribute(rand.nextInt(MAX_ATTRIBUTES)));
            }
            repo.putCategory(cat);
        }
        
        for(int i=0; i<MAX_AREAS; i++) {
            Area area = new Area(i, randomString(), randomString());
            int num = rand.nextInt(30) + 15;
            for(int j=0; j<num; j++) {
                area.addCategory(repo.getCategory(rand.nextInt(MAX_CATS)));
            }
            repo.putArea(area);
        }
        
        for(int i=0; i<MAX_CIS; i++) {
            Attribute a = repo.getAttribute(i % MAX_ATTRIBUTES);
            a.addColumnInstance(repo.getTable(tableNames.get(i / 100)),
                new ColumnInstance(randomString(), ColumnType.UNKNOWN), false);
        }
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        String date = dateFormat.format(new Date());
        String filename = "metadata-content/repo-" + date + ".xml";
        repo.saveTo(MetaDataStorageType.XML, filename);
        System.out.println("Repository Backed Up To: " + filename);
        
        System.out.println("DONE");
    }
    
    
    public static String randomString() {
        return RandomStringUtils.random(32, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    }
}
