package com.amazon.dw.grasshopper.orgchart;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.log4j.Logger;

import com.amazon.coral.metrics.Metrics;
import com.amazon.dw.grasshopper.tools.ImmutablePair;
import com.amazon.dw.grasshopper.util.file.AbstractFileContentFactory;
import com.amazon.dw.grasshopper.util.file.DelimitedFileReader;

public class OrgChartFactory {
    private static final Logger LOGGER = Logger.getLogger(OrgChartFactory.class);
    private static final String FILE_PROCESSING_TIME_METRIC = "OrgChartProcessingTime";
    private static final String FILE_SIZE_METRIC = "NumOrgChartRecordsSuccessfullyProcessed";
    
    
    /**
     * Creates an OrgChart based on data from the given file. If there is an error processing the file,
     * the OrgChart will contain no hierarchy.
     * 
     * @param orgChartDataFile a TSV file with 2 columns, user and manager, that is parsed to obtain the OrgChart
     * @return the OrgChart based on data from the given file
     * @throws NullPointerException if the given File is null
     */
    public static OrgChart createOrgChart(File orgChartDataFile, Metrics metrics) throws NullPointerException {
        long startTime = System.currentTimeMillis();
        
        Objects.requireNonNull(orgChartDataFile);
        
        OrgChart orgChart = new OrgChart();
        DelimitedFileReader fileReader = new DelimitedFileReader();
        
        // Read the orgChart data from the given file
        List<ImmutablePair<String, String>> userMgrPairs;
        
        try {
            userMgrPairs = fileReader.parseFile(orgChartDataFile,
                new AbstractFileContentFactory<ImmutablePair<String, String>>() {
                    
                    @Override
                    public ImmutablePair<String, String> createFileContentObject(String[] splits) throws IllegalArgumentException {
                        if(splits.length != 2) {
                            throw new IllegalArgumentException("The OrgChart file is expected to contain rows with 2 tab separated values");
                        }
                        
                        return new ImmutablePair<String, String>(splits[0], splits[1]);
                    }
                }, true);
        } catch(IOException e) {
            LOGGER.error("Error processing orgChart file:" + e, e);
            userMgrPairs = new LinkedList<ImmutablePair<String, String>>();
        }
        
        // Fill in the OrgChart with the hierarchy data from the file
        for(ImmutablePair<String, String> userMgrPair : userMgrPairs) {
            orgChart.setDirectManagerForUser(userMgrPair.getLeft(), userMgrPair.getRight());
        }
        
        long stopTime = System.currentTimeMillis();
        if(metrics != null) {
            metrics.addTime(FILE_PROCESSING_TIME_METRIC, stopTime - startTime, SI.MILLI(SI.SECOND));
            metrics.addCount(FILE_SIZE_METRIC, userMgrPairs.size(), Unit.ONE);
        }
        
        return orgChart;
    }
}