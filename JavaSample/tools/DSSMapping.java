package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class DSSMapping
{
    private static final String MAPPING_FILENAME = "metadata-content/dssAttributeMap.xml";
    
    private static DSSMapping instance = null;
    
    private Map<String, Long> attributeMapping;
    
    private DSSMapping() {
        attributeMapping = new HashMap<String, Long>();
        
        SAXBuilder builder = new SAXBuilder();
        File mappingFile = new File(MAPPING_FILENAME);
        
        try {
            Document document = (Document) builder.build(mappingFile);
            Element rootNode = document.getRootElement();
            @SuppressWarnings("unchecked")
            List<Element> mappings = (List<Element>)rootNode.getChildren("Mapping");
            for(Element mapping : mappings) {
                try {
                    String dssid = mapping.getAttributeValue("dssid");
                    long attrid = Long.parseLong(mapping.getAttributeValue("attrid"));
                    
                    attributeMapping.put(dssid, attrid);
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = mapping.getAttributes();
                    StringBuilder stringBuilder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       stringBuilder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + stringBuilder.toString() + mapping.getText());
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(JDOMException e) {
            e.printStackTrace();
        }
    }
    
    
    public static DSSMapping getInstance() {
        if(instance == null) {
            instance = new DSSMapping();
        }
        return instance;
    }
    
    
    public long getAttributeId(String dssId) throws NoSuchElementException {
        
        if(attributeMapping.containsKey(dssId)) {
            return attributeMapping.get(dssId);
        }
        
        throw new NoSuchElementException("There is no mapping for that DSS Id");
    }
}
