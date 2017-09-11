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

public class InjectMetaDataJSON {

    private static boolean changesMade = false;

    public static void main(String args[]) {

        InMemoryRepository repo = new InMemoryRepository();
        JSONParser parser = new JSONParser();
        MDHandler handler = new MDHandler(repo);
        
        long startTime = System.currentTimeMillis();
        
        try {
            String filename = "metadata-content/injectMD.xml";
            if(args.length > 0)
                filename = args[0];
            
            BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
            while(!handler.isEnd()) {
                parser.parse(input, handler, true);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Import Time: " + (endTime - startTime));
        
        if(changesMade) {
            save(repo);
            System.out.println("Metadata updated!! Don't forget to check-in");
        } else {
            System.out.println("No Metadata Updated.");
        }
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


    private static final class MDHandler implements ContentHandler {

        boolean adding = false;
//        boolean editing = false;
//        boolean removing = false;
        boolean end = false;
        boolean parsingTable = false;
        boolean parsingAttr = false;
        boolean parsingArea = false;
        boolean parsingCI = false;
        boolean parsingCat = false;
        boolean parsingAreaCat = false;
        boolean parsingCategoryAttr = false;
        boolean isBool = false;
        boolean isPartKey = false;
        
        String desc = null;
        String name = null;
        
        long id = 0;
        long catId = 0;
        long attrId = 0;
        
        int numObj = 0;
        int columnsAdded = 0;
        int areasAdded = 0;
        int categoriesAdded = 0;
        int tablesAdded = 0;
        int attributesAdded = 0;
        int areaCatsAdded = 0;
        int categoryAttrsAdded = 0;
        
        InMemoryRepository repo = null;
        String key = "";
        
        
        public MDHandler(InMemoryRepository repo) {
            this.repo = repo;
        }
        
        
        public boolean isEnd() {
            return end;
        }
        
        
        public void startJSON() throws ParseException, IOException {
            end = false;
        }
        
        
        public void endJSON() throws ParseException, IOException {
            System.out.println("----------");
            System.out.println(columnsAdded + " column instances added to MD");
            System.out.println(areasAdded + " areas added to MD");
            System.out.println(categoriesAdded + " categories added to MD");
            System.out.println(tablesAdded + " tables added to MD");
            System.out.println(attributesAdded + " attributes added to MD");
            System.out.println(areaCatsAdded + " areaCats added to MD");
            System.out.println(categoryAttrsAdded + " categoryAttrs added to MD");
            System.out.println("----------");
//            System.out.println(columnsAdded + " column instances edited");
//            System.out.println(areasAdded + " areas edited");
//            System.out.println(categoriesAdded + " categories edited");
//            System.out.println(tablesAdded + " tables edited");
//            System.out.println(attributesAdded + " attributes edited");
//            System.out.println(areaCatsAdded + " areaCats edited");
//            System.out.println(categoryAttrsAdded + " categoryAttrs edited");
//            System.out.println("----------");
//            System.out.println(columnsAdded + " column instances removed from MD");
//            System.out.println(areasAdded + " areas removed from MD");
//            System.out.println(categoriesAdded + " categories removed from MD");
//            System.out.println(tablesAdded + " tables removed from MD");
//            System.out.println(attributesAdded + " attributes removed from MD");
//            System.out.println(areaCatsAdded + " areaCats removed from MD");
//            System.out.println(categoryAttrsAdded + " categoryAttrs removed from MD");
//            System.out.println("----------");
            end = true;
        }
        
        
        public boolean startArray() { return true; }
        
        
        public boolean endArray() {
            parsingTable = false;
            parsingAttr = false;
            parsingArea = false;
            parsingCI = false;
            parsingCat = false;
            parsingAreaCat = false;
            parsingCategoryAttr = false;
            return true;
        }
        
        
        public boolean startObject() { return true; }
        
        
        public boolean endObject() {
            if(adding) {
                if(parsingAttr) {
                    addAttribute();
                } else if(parsingTable) {
                    addTable();
                } else if(parsingCat) {
                    addCategory();
                } else if(parsingArea) {
                    addArea();
                } else if(parsingCI) {
                    addCI();
                } else if(parsingCategoryAttr) {
                    addCategoryAttr();
                } else if(parsingAreaCat) {
                    addAreaCat();
                }
//            } else if(editing) {
//                if(parsingAttr) {
//                    editAttribute();
//                } else if(parsingTable) {
//                    editTable();
//                } else if(parsingCat) {
//                    editCategory();
//                } else if(parsingArea) {
//                    editArea();
//                }
//            } else if(removing) {
//                if(parsingAttr) {
//                    removeAttribute();
//                } else if(parsingTable) {
//                    removeTable();
//                } else if(parsingCat) {
//                    removeCategory();
//                } else if(parsingArea) {
//                    removeArea();
//                } else if(parsingCI) {
//                    removeCI();
//                } else if(parsingCategoryAttr) {
//                    removeCategoryAttr();
//                } else if(parsingAreaCat) {
//                    removeAreaCat();
//                }
            }
            
            return true;
        }
        
        
        public boolean startObjectEntry(String key) {
            
            if("additions".equalsIgnoreCase(key)) {
                adding = true;
//                editing = false;
//                removing = false;
//            } else if("edits".equalsIgnoreCase(key)) {
//                editing = true;
//                adding = false;
//                removing = false;
//            } else if("removals".equalsIgnoreCase(key)) {
//                removing = true;
//                adding = false;
//                removing = false;
            } else if("tables".equalsIgnoreCase(key)) {
                parsingTable = true;
            } else if("attributes".equalsIgnoreCase(key)) {
                parsingAttr = true;
            } else if("areas".equalsIgnoreCase(key)) {
                parsingArea = true;
            } else if("columninstances".equalsIgnoreCase(key)) {
                parsingCI = true;
            } else if("categories".equalsIgnoreCase(key)) {
                parsingCat = true;
            } else if("areacats".equalsIgnoreCase(key)) {
                parsingAreaCat = true;
            } else if("categoryattrs".equalsIgnoreCase(key)) {
                parsingCategoryAttr = true;
            } else {
                this.key = key;
            }
            
            return true;
        }
        
        
        public boolean endObjectEntry() { return true; }
        
        
        public boolean primitive(Object value) {
            
            if("columnname".equalsIgnoreCase(key) ||
               "description".equalsIgnoreCase(key) ||
               "physicalName".equalsIgnoreCase(key)) {
                
                desc = (String) value;
            } else if("columnType".equalsIgnoreCase(key) ||
                      "availability".equalsIgnoreCase(key) ||
                      "dbType".equalsIgnoreCase(key)) {
                
                numObj = ((Number) value).intValue();
            } else if("name".equalsIgnoreCase(key) || "table".equalsIgnoreCase(key)) {
                
                name = (String) value;
            } else if("isJoinable".equalsIgnoreCase(key) ||
                      "isFactTable".equalsIgnoreCase(key)) {
                
                isBool = ((Boolean) value).booleanValue();
            } else if("attrId".equalsIgnoreCase(key)) {
                attrId = ((Long) value).longValue();
            } else if("catId".equalsIgnoreCase(key)) {
                catId = ((Long) value).longValue();
            } else if("id".equalsIgnoreCase(key) || "areaId".equalsIgnoreCase(key)) {
                id = ((Long) value).longValue();
            } else if("isPartitionKey".equalsIgnoreCase(key)){
                isPartKey = ((Boolean) value).booleanValue();
            }
            
            return true;
        }
        
        
        private void addCI() {
            String columnName = "";
            int dbType = -1;
            String tableName = "";
            boolean isPKey = false;
            
            try {
                columnName = StringEscapeUtils.unescapeJavaScript(desc);
                dbType = numObj;
                tableName = StringEscapeUtils.unescapeJavaScript(name);
                isPKey = isPartKey;

                Table table = repo.getTable(tableName);
                Attribute attr = repo.getAttribute(attrId);

                if(table != null && attr != null) {

                    ColumnInstance ci = new ColumnInstance(columnName, ColumnType.get(dbType));
                    table.addColumnInstance(attr, ci, isPKey);
                    attr.addColumnInstance(table, ci, isPKey);

                    columnsAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                System.out.printf("Failed to add CI: {'columnName' : %s, " +
                    "'attrId' : %d, 'dbType' : %d, 'tableName' : %s}%n",
                    columnName, attrId, dbType, tableName);
                e.printStackTrace();
            }
        }


        private void addTable() {
            String name = "";
            String physicalName = "";
            boolean isFactTable = isBool;
            
            try {
                name = StringEscapeUtils.unescapeJavaScript(this.name);
                physicalName = StringEscapeUtils.unescapeJavaScript(desc);

                Table table = new Table(name);
                table.setPhysicalName(physicalName);
                table.setFactTable(isFactTable);

                if(!repo.putTable(table)) {
                    tablesAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                System.out.printf("Failed to add Table: {'name' : %s, " +
                    "'physicalName' : %s, 'dbType' : %d, 'isFactTable' : %b}%n",
                    name, physicalName, numObj, isFactTable);
                e.printStackTrace();
            }
        }


        private void addAttribute() {
            int availability = numObj;
            boolean isJoinable = isBool;
            String name = "";
            String desc = "";
            
            try {
                name = StringEscapeUtils.unescapeJavaScript(this.name);
                desc = StringEscapeUtils.unescapeJavaScript(this.desc);
                
                Attribute attr = new Attribute(id, name, desc, isJoinable);
                
                if(!repo.putAttribute(attr)) {
                    attributesAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                System.out.printf("Failed to add Attribute: {'id' : %d," +
                    "'name' : %s, 'description' : %s, 'isJoinable' : %b, " +
                    "'availability' : %d}%n", id, name, desc, isJoinable,
                    availability);
                e.printStackTrace();
            }
        }


        private void addArea() {
            String name = "";
            String desc = "";
            try {
                name = StringEscapeUtils.unescapeJavaScript(this.name);
                desc = StringEscapeUtils.unescapeJavaScript(this.desc);

                Area area = new Area(id, name, desc);
                if(!repo.putArea(area)) {
                    areasAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                System.out.printf("Failed to add Area: {'id' : %d," +
                    "'name' : %s, 'description' : %s}%n", id, name, desc);
                e.printStackTrace();
            }
        }


        private void addAreaCat() {
            long areaId = id;
            
            try {
                Area area = repo.getArea(areaId);
                if(area != null) {
                    area.addCategory(repo.getCategory(catId));
                }

                areaCatsAdded++;
                changesMade = true;
            } catch(RuntimeException e) {
                System.out.printf("Failed to add AreaCat: {'areaId' : %d,"
                    + " 'catId' : %d}%n", areaId, catId);
                e.printStackTrace();
            }
        }
        
        
        private void addCategory() {
            long id = catId;
            String name = "";
            
            try {
                name = StringEscapeUtils.unescapeJavaScript(this.name);
                
                Category category = new Category(id, name);
                
                if(!repo.putCategory(category)) {
                    categoriesAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                System.out.printf("Failed to add Category: {'catId' : %d, " +
                    "'name' : %s}%n", id, name);
                e.printStackTrace();
            }
        }
        
        
        private void addCategoryAttr() {
            try {
                Category cat = repo.getCategory(catId);
                cat.addAttribute(repo.getAttribute(attrId));
                
                categoryAttrsAdded++;
                changesMade = true;
            } catch(RuntimeException e) {
                System.out.printf("Failed to add CategoryAttr: {'catId' : %d" +
                    ", 'attrId' : %d}%n", catId, attrId);
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
}
