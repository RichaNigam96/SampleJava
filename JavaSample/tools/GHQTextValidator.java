package com.amazon.dw.grasshopper.tools;

import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;

import com.amazon.dw.grasshopper.compilerservice.GHQValidationConfiguration;
import com.amazon.dw.grasshopper.compilerservice.GHQValidatorVisitor;
import com.amazon.dw.grasshopper.compilerservice.ghqfactory.GHQFromJSONFactory;
import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.GHQuery.GHQuery;
import org.json.*;

public class GHQTextValidator {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        GHQValidationConfiguration config = new GHQValidationConfiguration();
        config.setConstStringRegEx("^[a-zA-Z0-9\\\\/_-]+$|^'[a-zA-Z0-9\\s\\\\/_-]+'$");
        config.setSynonymStringRegEx("^[a-zA-Z]+[a-zA-Z0-9_\\s]+$");
        String filename = "queryToValidate.ghq";
        if(args.length > 0)
            filename = args[0];
        FileInputStream fis = new FileInputStream(filename);
        String ghqText = IOUtils.toString(fis);
        fis.close();
        InMemoryRepository repository = new InMemoryRepository();
        JSONObject obj = new JSONObject(ghqText);
        GHQValidatorVisitor visitor = new GHQValidatorVisitor();
        GHQFromJSONFactory factory = new GHQFromJSONFactory(repository);
        GHQuery ghquery = factory.generateQuery(obj, DBType.SQL.getId());
        String validationStr = visitor.visit(ghquery);
        if(validationStr == null || validationStr.length()==0)
            System.out.println("Legal GHQ");
        else
            System.out.println("Illegal GHQ: " + validationStr);
        
    }

}
