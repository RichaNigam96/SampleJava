package com.amazon.dw.grasshopper.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;

import org.apache.commons.lang.StringEscapeUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataConfiguration;
import com.amazon.dw.grasshopper.metadata.MetaDataSerialObject;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.Table;

public class InjectMetaDataJSON2 {

    private static boolean changesMade = false;
    private static InMemoryRepository repo = new InMemoryRepository();
    
    private static int columnsAdded = 0;
    private static int areasAdded = 0;
    private static int categoriesAdded = 0;
    private static int tablesAdded = 0;
    private static int attributesAdded = 0;
    private static int areaCatsAdded = 0;
    private static int categoryAttrsAdded = 0;
    
    public static void main(String args[]) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            String filename = "metadata-content/injectMD.xml";
            if(args.length > 0)
                filename = args[0];
            
            BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            JSONObject json = (JSONObject)JSONValue.parse(input);
            JSONObject additions = (JSONObject)json.get("Additions");
//            JSONObject edits = (JSONObject)json.get("Edits");
//            JSONObject removals = (JSONObject)json.get("Removals");
            
            if(additions != null) {
                JSONArray attributes = (JSONArray)additions.get("Attributes");
                for(Object attr : attributes) {
                    addAttribute((JSONObject) attr);
                }
                
                JSONArray tables = (JSONArray)additions.get("Tables");
                for(Object table : tables) {
                    addTable((JSONObject) table);
                }
                
                JSONArray areas = (JSONArray)additions.get("Areas");
                for(Object area : areas) {
                    addArea((JSONObject) area);
                }
                
                JSONArray categories = (JSONArray)additions.get("Categories");
                for(Object cat : categories) {
                    addCategory((JSONObject) cat);
                }
                
                JSONArray columnInstances = (JSONArray)additions.get("ColumnInstances");
                for(Object ci : columnInstances) {
                    addCI((JSONObject) ci);
                }
                
                JSONArray areaCats = (JSONArray)additions.get("AreaCats");
                for(Object areaCat : areaCats) {
                    addAreaCat((JSONObject) areaCat);
                }
                
                JSONArray categoryAttrs = (JSONArray)additions.get("CategoryAttrs");
                for(Object categoryAttr : categoryAttrs) {
                    addCategoryAttr((JSONObject) categoryAttr);
                }
                
//            } else if(edits != null) {
//                JSONArray attributes = (JSONArray)additions.get("Attributes");
//                JSONArray tables = (JSONArray)additions.get("Tables");
//                JSONArray areas = (JSONArray)additions.get("Areas");
//                JSONArray categories = (JSONArray)additions.get("Categories");
//            } else if(removals != null) {
//                JSONArray attributes = (JSONArray)additions.get("Attributes");
//                JSONArray tables = (JSONArray)additions.get("Tables");
//                JSONArray areas = (JSONArray)additions.get("Areas");
//                JSONArray categories = (JSONArray)additions.get("Categories");
//                JSONArray columnInstances = (JSONArray)additions.get("ColumnInstances");
//                JSONArray areaCats = (JSONArray)additions.get("AreaCats");
//                JSONArray categoryAttrs = (JSONArray)additions.get("CategoryAttrs");
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Import Time: " + (endTime - startTime));
        
        System.out.println("----------");
        System.out.println(columnsAdded + " column instances added to MD");
        System.out.println(areasAdded + " areas added to MD");
        System.out.println(categoriesAdded + " categories added to MD");
        System.out.println(tablesAdded + " tables added to MD");
        System.out.println(attributesAdded + " attributes added to MD");
        System.out.println(areaCatsAdded + " areaCats added to MD");
        System.out.println(categoryAttrsAdded + " categoryAttrs added to MD");
        System.out.println("----------");
//        System.out.println(columnsAdded + " column instances edited");
//        System.out.println(areasAdded + " areas edited");
//        System.out.println(categoriesAdded + " categories edited");
//        System.out.println(tablesAdded + " tables edited");
//        System.out.println(attributesAdded + " attributes edited");
//        System.out.println(areaCatsAdded + " areaCats edited");
//        System.out.println(categoryAttrsAdded + " categoryAttrs edited");
//        System.out.println("----------");
//        System.out.println(columnsAdded + " column instances removed from MD");
//        System.out.println(areasAdded + " areas removed from MD");
//        System.out.println(categoriesAdded + " categories removed from MD");
//        System.out.println(tablesAdded + " tables removed from MD");
//        System.out.println(attributesAdded + " attributes removed from MD");
//        System.out.println(areaCatsAdded + " areaCats removed from MD");
//        System.out.println(categoryAttrsAdded + " categoryAttrs removed from MD");
//        System.out.println("----------");
        
//        if(changesMade) {
//            save(repo);
//            System.out.println("Metadata updated!! Don't forget to check-in");
//        } else {
//            System.out.println("No Metadata Updated.");
//        }
    }


    private static void save(InMemoryRepository repository) {
        MetaDataSerialObject metadata = repository.createSerializeObject();
        try {
            MetaDataConfiguration config = MetaDataConfiguration.getInstance();
            FileOutputStream fileOut = new FileOutputStream(config.getMetaDataFilename());
            ObjectOutputStream outfile = new ObjectOutputStream(new BufferedOutputStream(fileOut)); 
            outfile.writeObject(metadata);
            outfile.close();
            fileOut.close();
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    
    
    private static void addCI(JSONObject ciJSON) {
        try {
            String columnName = StringEscapeUtils.unescapeJavaScript((String)ciJSON.get("columnName"));
            int colType = ((Number) ciJSON.get("ColumnType")).intValue();
            String tableName = StringEscapeUtils.unescapeJavaScript((String)ciJSON.get("Table"));
            long attrId = ((Number) ciJSON.get("AttrId")).longValue();
            boolean isPartKey = ((Boolean) ciJSON.get("IsPartitionKey")).booleanValue();
            
            Table table = repo.getTable(tableName);
            Attribute attr = repo.getAttribute(attrId);

            if(table != null && attr != null) {

                ColumnInstance ci = new ColumnInstance(columnName, ColumnType.get(colType));
                table.addColumnInstance(attr, ci, isPartKey);
                attr.addColumnInstance(table, ci, isPartKey);

                columnsAdded++;
                changesMade = true;
            }
        } catch(RuntimeException e) {
            System.out.println(ciJSON);
            e.printStackTrace();
        }
    }


    private static void addTable(JSONObject tableJSON) {
        try {
            String name = StringEscapeUtils.unescapeJavaScript((String)tableJSON.get("name"));
            String physicalName = StringEscapeUtils.unescapeJavaScript((String)tableJSON.get("physicalName"));
            boolean isFactTable = ((Boolean)tableJSON.get("isFactTable")).booleanValue();

            Table table = new Table(name);
            table.setPhysicalName(physicalName);
            table.setFactTable(isFactTable);

            if(!repo.putTable(table)) {
                tablesAdded++;
                changesMade = true;
            }
        } catch(RuntimeException e) {
            System.out.println(tableJSON);
            e.printStackTrace();
        }
    }


    private static void addAttribute(JSONObject attrJSON) {
        try {
            String name = StringEscapeUtils.unescapeJavaScript((String)attrJSON.get("name"));
            String desc = StringEscapeUtils.unescapeJavaScript((String)attrJSON.get("description"));
            boolean isJoinable = ((Boolean)attrJSON.get("isJoinable")).booleanValue();
            long id = ((Number)attrJSON.get("id")).longValue();
            
            Attribute attr = new Attribute(id, name, desc, isJoinable);
            
            if(!repo.putAttribute(attr)) {
                attributesAdded++;
                changesMade = true;
            }
        } catch(RuntimeException e) {
            System.out.println(attrJSON);
            e.printStackTrace();
        }
    }


    private static void addArea(JSONObject areaJSON) {
        try {
            long id = ((Number)areaJSON.get("id")).longValue();
            String name = StringEscapeUtils.unescapeJavaScript((String)areaJSON.get("name"));
            String desc = StringEscapeUtils.unescapeJavaScript((String)areaJSON.get("description"));

            Area area = new Area(id, name, desc);
            if(!repo.putArea(area)) {
                areasAdded++;
                changesMade = true;
            }
        } catch(RuntimeException e) {
            System.out.println(areaJSON);
            e.printStackTrace();
        }
    }
    
    
    private static void addAreaCat(JSONObject areaCatJSON) {
        try {
            long areaId = ((Number)areaCatJSON.get("areaId")).longValue();
            long catId = ((Number)areaCatJSON.get("catId")).longValue();
            
            Area area = repo.getArea(areaId);
            if(area != null) {
                area.addCategory(repo.getCategory(catId));
            }

            areaCatsAdded++;
            changesMade = true;
        } catch(RuntimeException e) {
            System.out.println(areaCatJSON);
            e.printStackTrace();
        }
    }
    
    
    private static void addCategory(JSONObject catJSON) {
        try {
            long id = ((Number)catJSON.get("catId")).longValue();
            String name = StringEscapeUtils.unescapeJavaScript((String)catJSON.get("name"));
            
            Category category = new Category(id, name);
            
            if(!repo.putCategory(category)) {
                categoriesAdded++;
                changesMade = true;
            }
        } catch(RuntimeException e) {
            System.out.println(catJSON);
            e.printStackTrace();
        }
    }
    
    
    private static void addCategoryAttr(JSONObject categoryAttrJSON) {
        try {
            long catId = ((Number)categoryAttrJSON.get("catId")).longValue();
            long attrId = ((Number)categoryAttrJSON.get("attrId")).longValue();
            
            Category cat = repo.getCategory(catId);
            cat.addAttribute(repo.getAttribute(attrId));
            
            categoryAttrsAdded++;
            changesMade = true;
        } catch(RuntimeException e) {
            System.out.println(categoryAttrJSON);
            e.printStackTrace();
        }
    }
        
        
//        private void editArea() {
//            try {
//                long id = Long.parseLong(attributes.getValue("id"));
//                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
//                String desc = StringEscapeUtils.unescapeXml(attributes.getValue("description"));
//                
//                Area area = repo.getArea(id);
//                
//                if(area != null) {
//                    if(name != null) {
//                        area.setName(name);
//                    }
//                    
//                    if(desc != null) {
//                        area.setDescription(desc);
//                    }
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to edit Area: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void editAttribute() {
//            try {
//                long id = Long.parseLong(attributes.getValue("id"));
//                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
//                String desc = StringEscapeUtils.unescapeXml(attributes.getValue("description"));
//                String joinableString = attributes.getValue("isJoinable");
//                String availabilityString = attributes.getValue("availability");
//                
//                Attribute attr = repo.getAttribute(id);
//                
//                if(attr != null) {
//                    if(name != null) {
//                        attr.setName(name);
//                    }
//                    
//                    if(desc != null) {
//                        attr.setDescription(desc);
//                    }
//                    
//                    if(joinableString != null) {
//                        attr.setJoinable("true".equalsIgnoreCase(joinableString));
//                    }
//                    
//                    if(availabilityString != null) {
//                        attr.setAvailability(Long.parseLong(availabilityString));
//                    }
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to edit Attribute: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void editCategory() {
//            try {
//                long id = Long.parseLong(attributes.getValue("catId"));
//                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
//                
//                Category cat = repo.getCategory(id);
//                
//                if(cat != null) {
//                    if(name != null) {
//                        cat.setName(name);
//                    }
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to edit Category: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void editTable() {
//            try {
//                String tableName = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
//                String physicalName = StringEscapeUtils.unescapeXml(attributes.getValue("physicalName"));
//                String dbTypeString = attributes.getValue("dbType");
//                String factTableString = attributes.getValue("isFactTable");
//                
//                Table table = repo.getTable(tableName);
//                
//                if(table != null) {
//                    if(physicalName != null) {
//                        table.setPhysicalName(physicalName);
//                    }
//                    
//                    if(dbTypeString != null) {
//                        table.setDBType(DBType.get(Long.parseLong(dbTypeString)));
//                    }
//                    
//                    if(factTableString != null) {
//                        table.setFactTable("true".equalsIgnoreCase(factTableString));
//                    }
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to edit Table: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeCategoryAttr() {
//            try {
//                long catId = Long.parseLong(attributes.getValue("catId"));
//                long attrId = Long.parseLong(attributes.getValue("attrId"));
//                
//                Category category = repo.getCategory(catId);
//                Attribute attribute = repo.getAttribute(attrId);
//                
//                if(category != null && attribute != null) {
//                    category.removeAttribute(attribute);
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove CategoryAttr: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeAreaCat() {
//            try {
//                long areaId = Long.parseLong(attributes.getValue("areaId"));
//                long catId = Long.parseLong(attributes.getValue("catId"));
//                
//                Area area = repo.getArea(areaId);
//                Category category = repo.getCategory(catId);
//                
//                if(area != null && category != null) {
//                    area.removeCategory(category);
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove AreaCat: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeCI() {
//            try {
//                long attrId = Long.parseLong(attributes.getValue("AttrId"));
//                String tableName = StringEscapeUtils.unescapeXml(attributes.getValue("Table"));
//                
//                Attribute attr = repo.getAttribute(attrId);
//                Table table = repo.getTable(tableName);
//                
//                if(attr != null && table != null) {
//                    table.removeColumnInstance(attr);
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove ColumnInstance: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeArea() {
//            try {
//                long id = Long.parseLong(attributes.getValue("Area"));
//                
//                if(repo.removeArea(id)) {
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove Area: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeCategory() {
//            try {
//                long id = Long.parseLong(attributes.getValue("id"));
//                
//                if(repo.removeCategory(id)) {
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove Category: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeTable() {
//            try {
//                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
//                
//                if(repo.removeTable(name)) {
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove Table: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
//        
//        
//        private void removeAttribute() {
//            try {
//                long id = Long.parseLong(attributes.getValue("id"));
//                
//                if(repo.removeAttribute(id)) {
//                    changesMade = true;
//                }
//            } catch(RuntimeException e) {
//                int len = attributes.getLength();
//                StringBuilder builder = new StringBuilder();
//                for(int i=0; i<len; i++) {
//                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
//                }
//                System.out.println("Failed to remove Attribute: " + builder.toString());
//                e.printStackTrace();
//            }
//        }
}
