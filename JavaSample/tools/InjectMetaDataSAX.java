package com.amazon.dw.grasshopper.tools;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataConfiguration;
import com.amazon.dw.grasshopper.metadata.MetaDataSerialObject;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.Table;

public class InjectMetaDataSAX {

    private static boolean changesMade = false;

    public static void main(String args[]) {

        InMemoryRepository repo = new InMemoryRepository();

        long startTime = System.currentTimeMillis();

        try {
            String filename = "metadata-content/injectMD.xml";
            if(args.length > 0)
                filename = args[0];
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            MDHandler handler = new MDHandler(repo);
            FileInputStream fis = new FileInputStream(filename);
            saxParser.parse(fis, handler);
            fis.close();
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


    private static final class MDHandler extends DefaultHandler {

        boolean adding = false;
        boolean editing = false;
        boolean removing = false;
        int columnsAdded = 0;
        int areasAdded = 0;
        int categoriesAdded = 0;
        int tablesAdded = 0;
        int attributesAdded = 0;
        int areaCatsAdded = 0;
        int categoryAttrsAdded = 0;
        InMemoryRepository repo = null;

        public MDHandler(InMemoryRepository repo) {
            this.repo = repo;
        }

        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

            if("additions".equalsIgnoreCase(qName)) {
                adding = true;
            } else if("edits".equalsIgnoreCase(qName)) {
                editing = true;
            } else if("removals".equalsIgnoreCase(qName)) {
                removing = true;
            } else if(adding) {
                if("attribute".equalsIgnoreCase(qName)) {
                    addAttribute(attributes);
                } else if("table".equalsIgnoreCase(qName)) {
                    addTable(attributes);
                } else if("category".equalsIgnoreCase(qName)) {
                    addCategory(attributes);
                } else if("area".equalsIgnoreCase(qName)) {
                    addArea(attributes);
                } else if("columninstance".equalsIgnoreCase(qName)) {
                    addCI(attributes);
                } else if("categoryattr".equalsIgnoreCase(qName)) {
                    addCategoryAttr(attributes);
                } else if("areacat".equalsIgnoreCase(qName)) {
                    addAreaCat(attributes);
                }
            } else if(editing) {
                if("attribute".equalsIgnoreCase(qName)) {
                    editAttribute(attributes);
                } else if("table".equalsIgnoreCase(qName)) {
                    editTable(attributes);
                } else if("category".equalsIgnoreCase(qName)) {
                    editCategory(attributes);
                } else if("area".equalsIgnoreCase(qName)) {
                    editArea(attributes);
                }
            } else if(removing) {
                if("categoryattr".equalsIgnoreCase(qName)) {
                    removeCategoryAttr(attributes);
                } else if("areacat".equalsIgnoreCase(qName)) {
                    removeAreaCat(attributes);
                } else if("columninstance".equalsIgnoreCase(qName)) {
                    removeCI(attributes);
                } else if("area".equalsIgnoreCase(qName)) {
                    removeArea(attributes);
                } else if("category".equalsIgnoreCase(qName)) {
                    removeCategory(attributes);
                } else if("table".equalsIgnoreCase(qName)) {
                    removeTable(attributes);
                } else if("attribute".equalsIgnoreCase(qName)) {
                    removeAttribute(attributes);
                }
            }
        }


        public void endElement(String uri, String localName, String qName) throws SAXException {

            if("additions".equalsIgnoreCase(qName)) {
                adding = false;
                System.out.println("----------");
                System.out.println(columnsAdded + " column instances added to MD");
                System.out.println(areasAdded + " areas added to MD");
                System.out.println(categoriesAdded + " categories added to MD");
                System.out.println(tablesAdded + " tables added to MD");
                System.out.println(attributesAdded + " attributes added to MD");
                System.out.println(areaCatsAdded + " areaCats added to MD");
                System.out.println(categoryAttrsAdded + " categoryAttrs added to MD");
            } else if("edits".equalsIgnoreCase(qName)) {
                editing = false;
                System.out.println("----------");
                System.out.println(columnsAdded + " column instances edited");
                System.out.println(areasAdded + " areas edited");
                System.out.println(categoriesAdded + " categories edited");
                System.out.println(tablesAdded + " tables edited");
                System.out.println(attributesAdded + " attributes edited");
                System.out.println(areaCatsAdded + " areaCats edited");
                System.out.println(categoryAttrsAdded + " categoryAttrs edited");
            } else if("removals".equalsIgnoreCase(qName)) {
                removing = false;
                System.out.println("----------");
                System.out.println(columnsAdded + " column instances removed from MD");
                System.out.println(areasAdded + " areas removed from MD");
                System.out.println(categoriesAdded + " categories removed from MD");
                System.out.println(tablesAdded + " tables removed from MD");
                System.out.println(attributesAdded + " attributes removed from MD");
                System.out.println(areaCatsAdded + " areaCats removed from MD");
                System.out.println(categoryAttrsAdded + " categoryAttrs removed from MD");
                System.out.println("----------");
            }
        }


        private void addCI(Attributes attributes) {
            try {
                String columnName = StringEscapeUtils.unescapeXml(attributes.getValue("columnName"));
                int attrId = Integer.parseInt(attributes.getValue("AttrId"));
                int dbType = Integer.parseInt(attributes.getValue("ColumnType"));
                String tableName = StringEscapeUtils.unescapeXml(attributes.getValue("Table"));
                boolean isPartKey = Boolean.parseBoolean(attributes.getValue("IsPartitionKey"));

                Table table = repo.getTable(tableName);
                Attribute attr = repo.getAttribute(attrId);

                if(table != null && attr != null) {

                    ColumnInstance ci = new ColumnInstance(columnName, ColumnType.get(dbType));
                    table.addColumnInstance(attr, ci, isPartKey);
                    attr.addColumnInstance(table, ci, isPartKey);

                    columnsAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add CI: " + builder.toString());
                e.printStackTrace();
            }
        }


        private void addTable(Attributes attributes) {
            try {
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                String physicalName = StringEscapeUtils.unescapeXml(attributes.getValue("physicalName"));
                boolean isFactTable = false;
                if("true".equalsIgnoreCase(attributes.getValue("isFactTable"))) {
                    isFactTable = true;
                }

                Table table = new Table(name);
                table.setPhysicalName(physicalName);
                table.setFactTable(isFactTable);

                if(!repo.putTable(table)) {
                    tablesAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add Table: " + builder.toString());
                e.printStackTrace();
            }
        }


        private void addAttribute(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("id"));
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                String desc = StringEscapeUtils.unescapeXml(attributes.getValue("description"));
                boolean isJoinable = false;

                if("true".equalsIgnoreCase(attributes.getValue("isJoinable"))) {
                    isJoinable = true;
                }

                Attribute attr = new Attribute(id, name, desc, isJoinable);

                if(!repo.putAttribute(attr)) {
                    attributesAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add Attribute: " + builder.toString());
                e.printStackTrace();
            }
        }


        private void addArea(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("id"));
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                String desc = StringEscapeUtils.unescapeXml(attributes.getValue("description"));

                Area area = new Area(id, name, desc);
                if(!repo.putArea(area)) {
                    areasAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add Area: " + builder.toString());
                e.printStackTrace();
            }
        }


        private void addAreaCat(Attributes attributes) {
            try {
                long areaId = Long.parseLong(attributes.getValue("areaId"));
                long catId = Long.parseLong(attributes.getValue("catId"));

                Area area = repo.getArea(areaId);
                if(area != null) {
                    area.addCategory(repo.getCategory(catId));
                }

                areaCatsAdded++;
                changesMade = true;
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add AreaCat: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void addCategory(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("catId"));
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                
                Category category = new Category(id, name);
                
                if(!repo.putCategory(category)) {
                    categoriesAdded++;
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add Category: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void addCategoryAttr(Attributes attributes) {
            try {
                long catId = Long.parseLong(attributes.getValue("catId"));
                long attrId = Long.parseLong(attributes.getValue("attrId"));
                
                Category cat = repo.getCategory(catId);
                cat.addAttribute(repo.getAttribute(attrId));
                
                categoryAttrsAdded++;
                changesMade = true;
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to add CategoryAttr: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void editArea(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("id"));
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                String desc = StringEscapeUtils.unescapeXml(attributes.getValue("description"));
                
                Area area = repo.getArea(id);
                
                if(area != null) {
                    if(name != null) {
                        area.setName(name);
                    }
                    
                    if(desc != null) {
                        area.setDescription(desc);
                    }
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to edit Area: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void editAttribute(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("id"));
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                String desc = StringEscapeUtils.unescapeXml(attributes.getValue("description"));
                String joinableString = attributes.getValue("isJoinable");
                
                Attribute attr = repo.getAttribute(id);
                
                if(attr != null) {
                    if(name != null) {
                        attr.setName(name);
                    }
                    
                    if(desc != null) {
                        attr.setDescription(desc);
                    }
                    
                    if(joinableString != null) {
                        attr.setJoinable("true".equalsIgnoreCase(joinableString));
                    }
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to edit Attribute: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void editCategory(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("catId"));
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                
                Category cat = repo.getCategory(id);
                
                if(cat != null) {
                    if(name != null) {
                        cat.setName(name);
                    }
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to edit Category: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void editTable(Attributes attributes) {
            try {
                String tableName = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                String physicalName = StringEscapeUtils.unescapeXml(attributes.getValue("physicalName"));
                String factTableString = attributes.getValue("isFactTable");
                
                Table table = repo.getTable(tableName);
                
                if(table != null) {
                    if(physicalName != null) {
                        table.setPhysicalName(physicalName);
                    }
                    
                    if(factTableString != null) {
                        table.setFactTable("true".equalsIgnoreCase(factTableString));
                    }
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to edit Table: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeCategoryAttr(Attributes attributes) {
            try {
                long catId = Long.parseLong(attributes.getValue("catId"));
                long attrId = Long.parseLong(attributes.getValue("attrId"));
                
                Category category = repo.getCategory(catId);
                Attribute attribute = repo.getAttribute(attrId);
                
                if(category != null && attribute != null) {
                    category.removeAttribute(attribute);
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove CategoryAttr: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeAreaCat(Attributes attributes) {
            try {
                long areaId = Long.parseLong(attributes.getValue("areaId"));
                long catId = Long.parseLong(attributes.getValue("catId"));
                
                Area area = repo.getArea(areaId);
                Category category = repo.getCategory(catId);
                
                if(area != null && category != null) {
                    area.removeCategory(category);
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove AreaCat: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeCI(Attributes attributes) {
            try {
                long attrId = Long.parseLong(attributes.getValue("AttrId"));
                String tableName = StringEscapeUtils.unescapeXml(attributes.getValue("Table"));
                
                Attribute attr = repo.getAttribute(attrId);
                Table table = repo.getTable(tableName);
                
                if(attr != null && table != null) {
                    table.removeColumnInstance(attr);
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove ColumnInstance: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeArea(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("Area"));
                
                if(repo.removeArea(id)) {
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove Area: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeCategory(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("id"));
                
                if(repo.removeCategory(id)) {
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove Category: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeTable(Attributes attributes) {
            try {
                String name = StringEscapeUtils.unescapeXml(attributes.getValue("name"));
                
                if(repo.removeTable(name)) {
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove Table: " + builder.toString());
                e.printStackTrace();
            }
        }
        
        
        private void removeAttribute(Attributes attributes) {
            try {
                long id = Long.parseLong(attributes.getValue("id"));
                
                if(repo.removeAttribute(id)) {
                    changesMade = true;
                }
            } catch(RuntimeException e) {
                int len = attributes.getLength();
                StringBuilder builder = new StringBuilder();
                for(int i=0; i<len; i++) {
                    builder.append(attributes.getQName(i) + " : " + attributes.getValue(i) + " , ");
                }
                System.out.println("Failed to remove Attribute: " + builder.toString());
                e.printStackTrace();
            }
        }
    }
}
