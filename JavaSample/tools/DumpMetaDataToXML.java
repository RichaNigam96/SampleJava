package com.amazon.dw.grasshopper.tools;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataStorageType;

/**
 * This tool will dump the entire contents of the metadata out to
 * an XML file named repo-<today's date>.xml
 * 
 * @author shererr
 * Dec 06, 2012
 */
public class DumpMetaDataToXML
{
    public static void main(String[] args)
    {
        InMemoryRepository repo = new InMemoryRepository();
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        String date = dateFormat.format(new Date());
        String filename = "metadata-content/repo-" + date + ".xml";
        repo.saveTo(MetaDataStorageType.XML, filename);
        System.out.println("Repository Backed Up To: " + filename);
    }
}
