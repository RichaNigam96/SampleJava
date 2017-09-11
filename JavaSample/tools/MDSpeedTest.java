package com.amazon.dw.grasshopper.tools;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataSerialObject;

public class MDSpeedTest
{
    public static void main(String args[]) {
        InMemoryRepository repo = null;
        long startTime = 0;
        
        startTime = System.currentTimeMillis();
        repo = new InMemoryRepository();
        long endTime = System.currentTimeMillis();
        System.out.println("Init Time: " + (endTime - startTime));
                
        startTime = System.currentTimeMillis();
        
        MetaDataSerialObject metadata = repo.createSerializeObject();
        try {
            FileOutputStream fileOut = new FileOutputStream("metadata-content/metadata.gh");
            ObjectOutputStream outfile = new ObjectOutputStream(new BufferedOutputStream(fileOut));
            outfile.writeObject(metadata);
            outfile.close();
            fileOut.close();
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
        
        long saveTime = System.currentTimeMillis();
        System.out.println("Save Time: " + (saveTime - startTime));
    }
}
