package com.amazon.dw.grasshopper.tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataConfiguration;
import com.amazon.dw.grasshopper.metadata.MetaDataSerialObject;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.Table;

/**
 * This tool is used to insert items into the metadata file.
 * It reads elements from a provided XML file (default injectMD.xml)
 * and inserts them into a repository, then saves that repository to disk
 * Don't forget to check-in the metadata.gh file after using this tool
 * 
 * @author srebrnik, shererr
 * Dec 06, 2012
 */
public class InjectMetaData {

    public static void main(String[] args) {

        InMemoryRepository repos = new InMemoryRepository();
        InjectMetaData imd = new InjectMetaData();
        try{
            String fileName = "metadata-content/injectMD.xml";
            if(args.length > 0)
                fileName = args[0];
            imd.injectMetaData(repos, fileName);
        }
        catch(Exception ex){
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }
    
    
    protected void injectMetaData(InMemoryRepository repository, String filename) throws Exception{
        long startTime = System.currentTimeMillis();
        SAXBuilder builder = new SAXBuilder();
        File xmlFile = new File(filename);
        
        Document document = (Document) builder.build(xmlFile);
        Element rootNode = document.getRootElement();
        Element addNode = rootNode.getChild("Additions");
        Element editNode = rootNode.getChild("Edits");
        Element removeNode = rootNode.getChild("Removals");
        
        boolean changesMade = false;
        
        if(addNode != null) {
            changesMade |= addNodes(repository, addNode);
        }
        
        if(editNode != null) {
            changesMade |= editNodes(repository, editNode);
        }
        
        if(removeNode != null) {
            changesMade |= removeNodes(repository, removeNode);
        }
        
        long endTime = System.currentTimeMillis();
        
        System.out.println("Import Time: " + (endTime - startTime));
        
        if(changesMade) {
            save(repository);
            long saveTime = System.currentTimeMillis();
            System.out.println("Save Time: " + (saveTime - endTime));
            System.out.println("Metadata updated!! Don't forget to Check-in.");
        }
        else
            System.out.println("No Metadata updated.");
    }
    
    
    protected void save(InMemoryRepository repository){
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
    
    
    protected boolean addNodes(InMemoryRepository repository, Element addNode) {
        
        int columnsAdded = 0;
        int areasAdded = 0;
        int categoriesAdded = 0;
        int tablesAdded = 0;
        int attributesAdded = 0;
        
        boolean changes = false;
        
        Element attributesNode = addNode.getChild("Attributes");
        if(attributesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> attributesToAdd = attributesNode.getChildren("Attribute");
            for(Element attributeToAdd : attributesToAdd) {
                if(addAttribute(repository, attributeToAdd)) {
                    attributesAdded++;
                    changes = true;
                }
            }
        }
        attributesNode = null;
        
        Element tablesNode = addNode.getChild("Tables");
        if(tablesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> tablesToAdd = tablesNode.getChildren("Table");
            for(Element tableToAdd : tablesToAdd) {
                if(addTable(repository, tableToAdd)) {
                    tablesAdded++;
                    changes = true;
                }
            }
        }
        tablesNode = null;
        
        Element categoriesNode = addNode.getChild("Categories");
        if(categoriesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> categoriesToAdd = categoriesNode.getChildren("Category");
            for(Element categoryToAdd : categoriesToAdd) {
                if(addCategory(repository, categoryToAdd)) {
                    categoriesAdded++;
                    changes = true;
                }
            }
        }
        categoriesNode = null;
        
        Element areasNode = addNode.getChild("Areas");
        if(areasNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> areasToAdd = areasNode.getChildren("Area");
            for(Element areaToAdd : areasToAdd) {
                if(addArea(repository, areaToAdd)) {
                    areasAdded++;
                    changes = true;
                }
            }
        }
        areasNode = null;
        
        Element columnsNode = addNode.getChild("ColumnInstances");
        if(columnsNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> columnInstancesToAdd =  columnsNode.getChildren("ColumnInstance");
            for(Element columnToAdd : columnInstancesToAdd) {
                if(addCI(repository, columnToAdd)) {
                    columnsAdded++;
                    changes = true;
                }
            }
        }
        columnsNode = null;
        
        Element areaCatsNode = addNode.getChild("AreaCats");
        if(areaCatsNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> areaCatsToAdd = areaCatsNode.getChildren("AreaCat");
            for(Element areaCatToAdd : areaCatsToAdd) {
                addAreaCat(repository, areaCatToAdd);
                changes = true;
            }
        }
        areaCatsNode = null;
        
        Element categoryAttrsNode = addNode.getChild("CategoryAttrs");
        if(categoryAttrsNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> categoryAttrsToAdd = categoryAttrsNode.getChildren("CategoryAttr");
            for(Element categoryAttrToAdd : categoryAttrsToAdd) {
                addCategoryAttr(repository, categoryAttrToAdd);
                changes = true;
            }
        }
        categoryAttrsNode = null;
        
        System.out.println(columnsAdded + " column instances added to MD");
        System.out.println(areasAdded + " areas added to MD");
        System.out.println(categoriesAdded + " categories added to MD");
        System.out.println(tablesAdded + " tables added to MD");
        System.out.println(attributesAdded + " attributes added to MD");
        
        return changes;
    }
    
    
    protected boolean addCI(InMemoryRepository repo, Element columnToAdd) {
        try {
            String columnName = StringEscapeUtils.unescapeXml(columnToAdd.getAttribute("columnName").getValue());
            int attrId = Integer.parseInt(columnToAdd.getAttribute("AttrId").getValue());
            int dbType = Integer.parseInt(columnToAdd.getAttribute("ColumnType").getValue());
            boolean isPartKey = Boolean.parseBoolean(columnToAdd.getAttributeValue("IsPartitionKey"));

            String tableName = StringEscapeUtils.unescapeXml(columnToAdd.getAttribute("Table").getValue());
            
            Table t = repo.getTable(tableName);
            Attribute attr = repo.getAttribute(attrId);
            
            if(t==null || attr==null)
                return false;
            
            ColumnInstance ci = new ColumnInstance(columnName, ColumnType.get(dbType));
            t.addColumnInstance(attr, ci, isPartKey);
            attr.addColumnInstance(t, ci, isPartKey);

            return true;
        }
        catch(RuntimeException ex) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = columnToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + columnToAdd.getText());
            ex.printStackTrace();
            return false;
        }
    }
    
    
    protected boolean addTable(InMemoryRepository repo, Element tableToAdd) {
        try {
            String name = StringEscapeUtils.unescapeXml(tableToAdd.getAttributeValue("name"));
            String physicalName = StringEscapeUtils.unescapeXml(tableToAdd.getAttributeValue("physicalName"));
            boolean isFactTable = false;
            if("true".equalsIgnoreCase(tableToAdd.getAttributeValue("isFactTable"))) {
                isFactTable = true;
            }
            
            Table table = new Table(name);
            table.setPhysicalName(physicalName);
            table.setFactTable(isFactTable);
            
            return !repo.putTable(table);
        } catch(RuntimeException ex) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = tableToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + tableToAdd.getText());
            return false;
        }
    }
    
    
    protected boolean addAttribute(InMemoryRepository repo, Element attrToAdd) {
        try {
            int id = Integer.parseInt(attrToAdd.getAttributeValue("id"));
            String name = StringEscapeUtils.unescapeXml(attrToAdd.getAttributeValue("name"));
            String desc = StringEscapeUtils.unescapeXml(attrToAdd.getAttributeValue("description"));
            boolean isJoinable = false;            
            
            if("true".equalsIgnoreCase(attrToAdd.getAttributeValue("isJoinable"))) {
                isJoinable = true;
            }
            
            Attribute attribute = new Attribute(id, name, desc, isJoinable);
            return !repo.putAttribute(attribute);
        } catch(RuntimeException ex) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = attrToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + attrToAdd.getText());
            return false;
        }
    }
    
    
    protected boolean addCategory(InMemoryRepository repo, Element catToAdd) {
        
        try {
            int id = Integer.parseInt(catToAdd.getAttributeValue("catId"));
            String name = StringEscapeUtils.unescapeXml(catToAdd.getAttributeValue("name"));
            
            Category category = new Category(id, name);
            return !repo.putCategory(category);
        } catch(RuntimeException ex) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = catToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + catToAdd.getText());
            return false;
        }
    }
    
    
    protected boolean addArea(InMemoryRepository repo, Element areaToAdd) {
        
        try {
            int id = Integer.parseInt(areaToAdd.getAttributeValue("id"));
            String name = StringEscapeUtils.unescapeXml(areaToAdd.getAttributeValue("name"));
            String desc = StringEscapeUtils.unescapeXml(areaToAdd.getAttributeValue("description"));
            
            Area area = new Area(id, name, desc);
            return !repo.putArea(area);
        } catch(RuntimeException ex) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = areaToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + areaToAdd.getText());
            return false;
        }
    }
    
    
    protected void addAreaCat(InMemoryRepository repo, Element areaCatToAdd) {
        try {
            long areaId = Long.parseLong(areaCatToAdd.getAttributeValue("areaId"));
            long catId = Long.parseLong(areaCatToAdd.getAttributeValue("catId"));
            
            Area area = repo.getArea(areaId);
            area.addCategory(repo.getCategory(catId));
        } catch(RuntimeException e) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = areaCatToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + areaCatToAdd.getText());
        }
    }
    
    
    protected void addCategoryAttr(InMemoryRepository repo, Element categoryAttrToAdd) {
        try {
            long catId = Long.parseLong(categoryAttrToAdd.getAttributeValue("catId"));
            long attrId = Long.parseLong(categoryAttrToAdd.getAttributeValue("attrId"));
            
            Category cat = repo.getCategory(catId);
            cat.addAttribute(repo.getAttribute(attrId));
        } catch(RuntimeException e) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = categoryAttrToAdd.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + categoryAttrToAdd.getText());
        }
    }
    
    
    protected boolean editNodes(InMemoryRepository repo, Element editNode) {
        boolean changesMade = false;
        
        Element areasNode = editNode.getChild("Areas");
        Element categoriesNode = editNode.getChild("Categories");
        Element tablesNode = editNode.getChild("Tables");
        Element attributesNode = editNode.getChild("Attributes");
        
        if(attributesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> attributesToEdit = attributesNode.getChildren("Attribute");
            for(Element attributeToEdit : attributesToEdit) {
                editAttribute(repo, attributeToEdit);
            }
            changesMade = true;
        }
        
        if(tablesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> tablesToEdit = tablesNode.getChildren("Table");
            for(Element tableToEdit : tablesToEdit) {
                editTable(repo, tableToEdit);
            }
            changesMade = true;
        }
        
        if(categoriesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> categoriesToEdit = categoriesNode.getChildren("Category");
            for(Element categoryToEdit : categoriesToEdit) {
                editCategory(repo, categoryToEdit);
            }
            changesMade = true;
        }
        
        if(areasNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> areasToEdit = areasNode.getChildren("Area");
            for(Element areaToEdit : areasToEdit) {
                editArea(repo, areaToEdit);
            }
            changesMade = true;
        }
        
        return changesMade;
    }
    
    
    protected void editArea(InMemoryRepository repo, Element areaToEdit) {
        try {
            long id = Long.parseLong(areaToEdit.getAttributeValue("id"));
            String name = StringEscapeUtils.unescapeXml(areaToEdit.getAttributeValue("name"));
            String desc = StringEscapeUtils.unescapeXml(areaToEdit.getAttributeValue("description"));
            
            Area area = repo.getArea(id);
            
            if(area == null) {
                return;
            }
            
            if(name != null) {
                area.setName(name);
            }
            
            if(desc != null) {
                area.setDescription(desc);
            }
        } catch(RuntimeException e) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = areaToEdit.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + areaToEdit.getText());
        }
    }
    
    
    protected void editAttribute(InMemoryRepository repo, Element attributeToEdit) {
        try {
            long id = Long.parseLong(attributeToEdit.getAttributeValue("id"));
            String name = StringEscapeUtils.unescapeXml(attributeToEdit.getAttributeValue("name"));
            String desc = StringEscapeUtils.unescapeXml(attributeToEdit.getAttributeValue("description"));
            String joinableString = attributeToEdit.getAttributeValue("isJoinable");
            
            Attribute attr = repo.getAttribute(id);
            
            if(attr == null) {
                return;
            }
            
            if(name != null) {
                attr.setName(name);
            }
            
            if(desc != null) {
                attr.setDescription(desc);
            }
            
            if(joinableString != null) {
                boolean joinable = joinableString.equalsIgnoreCase("true");
                attr.setJoinable(joinable);
            }
        } catch(RuntimeException e) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = attributeToEdit.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + attributeToEdit.getText());
        }
    }
    
    
    protected void editCategory(InMemoryRepository repo, Element catToEdit) {
        try {
            long id = Long.parseLong(catToEdit.getAttributeValue("catId"));
            String name = StringEscapeUtils.unescapeXml(catToEdit.getAttributeValue("name"));
            
            Category cat = repo.getCategory(id);
            
            if(cat == null) {
                return;
            }
            
            if(name != null) {
                cat.setName(name);
            }
        } catch(RuntimeException e) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = catToEdit.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + catToEdit.getText());
        }
    }
    
    
    protected void editTable(InMemoryRepository repo, Element tableToEdit) {
        try {
            String tableName = tableToEdit.getAttributeValue("name");
            String physicalName = tableToEdit.getAttributeValue("physicalName");
            String factTableString = tableToEdit.getAttributeValue("isFactTable");
            
            Table table = repo.getTable(tableName);
            
            if(table == null) {
                return;
            }
            
            if(physicalName != null) {
                table.setPhysicalName(physicalName);
            }
            
            if(factTableString != null) {
                boolean isFactTable = factTableString.equalsIgnoreCase("true");
                table.setFactTable(isFactTable);
            }
        } catch(RuntimeException e) {
            @SuppressWarnings("unchecked")
            List<org.jdom.Attribute> attrs = tableToEdit.getAttributes();
            StringBuilder builder = new StringBuilder();
            for(org.jdom.Attribute attr : attrs)
               builder.append(attr.getName() + ":" + attr.getValue() + "\t");
               
            System.out.println("Failed on: " + builder.toString() + tableToEdit.getText());
        }
    }
    
    
    protected boolean removeNodes(InMemoryRepository repo, Element removeNode) {
        boolean changesMade = false;
        
        Element columnsNode = removeNode.getChild("ColumnInstances");
        Element areasNode = removeNode.getChild("Areas");
        Element categoriesNode = removeNode.getChild("Categories");
        Element tablesNode = removeNode.getChild("Tables");
        Element attributesNode = removeNode.getChild("Attributes");
        Element areaCatsNode = removeNode.getChild("AreaCats");
        Element categoryAttrsNode = removeNode.getChild("CategoryAttrs");
        
        int areasRemoved = 0;
        int categoriesRemoved = 0;
        int tablesRemoved = 0;
        int attributesRemoved = 0;
        
        if(categoryAttrsNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> categoryAttrsToRemove = categoryAttrsNode.getChildren("CategoryAttr");
            for(Element categoryAttrToRemove : categoryAttrsToRemove) {
                try {
                    long catId = Long.parseLong(categoryAttrToRemove.getAttributeValue("catId"));
                    long attrId = Long.parseLong(categoryAttrToRemove.getAttributeValue("attrId"));
                    
                    Category category = repo.getCategory(catId);
                    Attribute attribute = repo.getAttribute(attrId);
                    
                    if(category == null || attribute == null) {
                        continue;
                    }
                    
                    category.removeAttribute(attribute);
                    changesMade = true;
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = categoryAttrToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + categoryAttrToRemove.getText());
                }
            }
        }
        
        if(areaCatsNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> areaCatsToRemove = areaCatsNode.getChildren("AreaCat");
            for(Element areaCatToRemove : areaCatsToRemove) {
                try {
                    long areaId = Long.parseLong(areaCatToRemove.getAttributeValue("areaId"));
                    long catId = Long.parseLong(areaCatToRemove.getAttributeValue("catId"));
                    
                    Area area = repo.getArea(areaId);
                    Category category = repo.getCategory(catId);
                    
                    if(area == null || category == null) {
                        continue;
                    }
                    
                    area.removeCategory(category);
                    changesMade = true;
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = areaCatToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + areaCatToRemove.getText());
                }
            }
        }
        
        if(columnsNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> columnInstancesToRemove =  columnsNode.getChildren("ColumnInstance");
            for(Element columnToRemove : columnInstancesToRemove) {
                try {
                    long attrId = Long.parseLong(columnToRemove.getAttributeValue("AttrId"));
                    String tableName = columnToRemove.getAttributeValue("Table");
                    
                    Table table = repo.getTable(tableName);
                    Attribute attr = repo.getAttribute(attrId);
                    
                    if(table == null || attr == null) {
                        continue;
                    }
                    
                    table.removeColumnInstance(attr);
                    changesMade = true;
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = columnToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + columnToRemove.getText());
                }
            }
        }
        
        if(areasNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> areasToRemove = areasNode.getChildren("Area");
            for(Element areaToRemove : areasToRemove) {
                try {
                    long id = Long.parseLong(areaToRemove.getAttributeValue("id"));
                    if(repo.removeArea(id)) {
                        areasRemoved++;
                        changesMade = true;
                    }
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = areaToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + areaToRemove.getText());
                }
            }
        }
        
        if(categoriesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> categoriesToRemove = categoriesNode.getChildren("Category");
            for(Element categoryToRemove : categoriesToRemove) {
                try {
                    long id = Long.parseLong(categoryToRemove.getAttributeValue("id"));
                    if(repo.removeCategory(id)) {
                        categoriesRemoved++;
                        changesMade = true;
                    }
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = categoryToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + categoryToRemove.getText());
                }
            }
        }
        
        if(tablesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> tablesToRemove = tablesNode.getChildren("Table");
            for(Element tableToRemove : tablesToRemove) {
                try {
                    String name = StringEscapeUtils.unescapeXml(tableToRemove.getAttributeValue("name"));
                    if(repo.removeTable(name)) {
                        tablesRemoved++;
                        changesMade = true;
                    }
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = tableToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + tableToRemove.getText());
                }
            }
        }
        
        if(attributesNode != null) {
            @SuppressWarnings("unchecked")
            List<Element> attributesToRemove = attributesNode.getChildren("Attribute");
            for(Element attributeToRemove : attributesToRemove) {
                try {
                    long id = Long.parseLong(attributeToRemove.getAttributeValue("id"));
                    if(repo.removeAttribute(id)) {
                        attributesRemoved++;
                        changesMade = true;
                    }
                } catch(RuntimeException e) {
                    @SuppressWarnings("unchecked")
                    List<org.jdom.Attribute> attrs = attributeToRemove.getAttributes();
                    StringBuilder builder = new StringBuilder();
                    for(org.jdom.Attribute attr : attrs)
                       builder.append(attr.getName() + ":" + attr.getValue() + "\t");
                       
                    System.out.println("Failed on: " + builder.toString() + attributeToRemove.getText() + " - " + e.toString());
                }
            }
        }
        
        System.out.println(areasRemoved + " areas removed to MD");
        System.out.println(categoriesRemoved + " categories removed to MD");
        System.out.println(tablesRemoved + " tables removed to MD");
        System.out.println(attributesRemoved + " attributes removed to MD");
        
        return changesMade;
    }
}
