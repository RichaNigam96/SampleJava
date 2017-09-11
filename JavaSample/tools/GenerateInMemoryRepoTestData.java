/**
 * 
 */
package com.amazon.dw.grasshopper.tools;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataStorageType;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.Table;

/**
 * This tool is used to generate fake metadata to use with JUnit testing the
 * InMemoryRepository
 *  
 * @author shererr
 */
public class GenerateInMemoryRepoTestData {
    
    
    /**
     * @param args ignored
     */
    public static void main(String[] args) {
        System.out.println("Generating Data...");
        InMemoryRepository repo = new InMemoryRepository(null, null);
        
        Attribute attributes[] = new Attribute[] {
            new Attribute(1, "A", "Desc", false),
            new Attribute(2, "B", "Desc", false),
            new Attribute(3, "C", "Desc", true),
            new Attribute(4, "D", "Desc", false),
            new Attribute(5, "E", "Desc", false),
        };
        
        Table tables[] = new Table[] {
            new Table("Table A"),
            new Table("Table B"),
            new Table("Table C")
        };
        
        Area areas[] = new Area[] {
            new Area(1, "Area A", "Desc"),
            new Area(2, "Area B", "Desc")
        };
        
        Category cats[] = new Category[] {
            new Category(1, "Category A"),
            new Category(2, "Category B"),
            new Category(3, "Category C"),
        };
        
        areas[0].addCategory(cats[0]);
        areas[0].addCategory(cats[1]);
        areas[1].addCategory(cats[2]);
        cats[0].addAttribute(attributes[0]);
        cats[1].addAttribute(attributes[1]);
        cats[2].addAttribute(attributes[2]);
        cats[2].addAttribute(attributes[3]);
        cats[2].addAttribute(attributes[4]);
        
        tables[0].addColumnInstance(attributes[0], new ColumnInstance("ATTR_A", ColumnType.CHAR), false);
        tables[0].addColumnInstance(attributes[1], new ColumnInstance("ATTR_B", ColumnType.CHAR), false);
        tables[1].addColumnInstance(attributes[1], new ColumnInstance("ATTR_A_TWO", ColumnType.CHAR), false);
        tables[1].addColumnInstance(attributes[2], new ColumnInstance("ATTR_C", ColumnType.CHAR), false);
        tables[2].addColumnInstance(attributes[3], new ColumnInstance("ATTR_D", ColumnType.NUMERIC), false);
        tables[2].addColumnInstance(attributes[4], new ColumnInstance("ATTR_E", ColumnType.NUMERIC), false);
        
        for(Area area : areas) {
            repo.putArea(area);
        }
        
        for(Attribute attr : attributes) {
            repo.putAttribute(attr);
        }
        
        for(Category cat : cats) {
            repo.putCategory(cat);
        }
        
        for(Table table : tables) {
            repo.putTable(table);
        }
        
        System.out.println("Saving Java File...");
        repo.saveTo(MetaDataStorageType.JAVA, "tst-resources/repoTestMD.gh");
        
        System.out.println("Saving XML File...");
        repo.saveTo(MetaDataStorageType.XML, "tst-resources/repoTestMD.xml");
        
        System.out.println("Saving JSON File...");
        repo.saveTo(MetaDataStorageType.JSON, "tst-resources/repoTestMD.json");
        
        System.out.println("DONE!");
    }
}
