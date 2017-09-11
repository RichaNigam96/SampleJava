package com.amazon.dw.grasshopper.tools;

import java.util.Scanner;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.InjectJSONMetaData;

public class InjectJSONMetaDataLauncher {

    /**
     * Injects metadata changes into the repository and allows the user to
     * save these changes
     * @param args use a command line argument to specify the json file to inject    
     */
    public static void main(String[] args) {
        InMemoryRepository repo = new InMemoryRepository();
        InjectJSONMetaData injector = new InjectJSONMetaData(repo);
        String filename = "metadata-content/injectMD.json";
        if(args.length > 0)
            filename = args[0];
        
        injector.inject(filename);
        
        if(injector.isChangesMade()) {
            String userInput = "";
            Scanner scanner = new Scanner(System.in);
            
            while("".equals(userInput)) {
                System.out.print("MetaData changes were made. Save changes? (y/n): ");
                userInput = scanner.next();
                if("y".equalsIgnoreCase(userInput) || "yes".equalsIgnoreCase(userInput)) {
                    repo.save();
                    System.out.println("------------");
                    System.out.println("Changes saved. Don't forget to commit changes!");
                    System.out.println("------------");
                } else if("n".equalsIgnoreCase(userInput) || "no".equalsIgnoreCase(userInput)) {
                    System.out.println("------------");
                    System.out.println("Changes not saved.");
                    System.out.println("------------");
                } else {
                    userInput = "";
                }
            }
        } else {
            System.out.println("No Metadata Updated.");
        }

         
    }

}
