package com.amazon.dw.grasshopper.orgchart;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.log4j.Logger;

import com.amazon.coral.metrics.Metrics;
import com.amazon.dw.grasshopper.tools.ImmutablePair;
import com.amazon.dw.grasshopper.util.file.AbstractFileContentFactory;
import com.amazon.dw.grasshopper.util.file.DelimitedFileReader;

public class OrgChartPopularAreaManager {
    private static final Logger LOGGER = Logger.getLogger(OrgChartPopularAreaManager.class);
    private static final String FILE_PROCESSING_TIME_METRIC = "OrgPopularAreasProcessingTime";
    private static final String FILE_SIZE_METRIC = "NumOrgPopularAreasRecordsSuccessfullyProcessed";
    private static final long DEFAULT_LIMIT = Long.MAX_VALUE;
    private static final Set<String> DEFAULT_GROUPS = null;
    
    private final Map<String, SortedSet<ImmutablePair<String, Long>>> popularAreasMap;
    
    
    private OrgChartPopularAreaManager() {
        popularAreasMap = new HashMap<String, SortedSet<ImmutablePair<String, Long>>>();
    }
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="WMI_WRONG_MAP_ITERATOR", justification="Actually using the key, so don't want values() iterator")
    public static OrgChartPopularAreaManager fromFileAndOrg(File orgChartDataFile, OrgChart orgChart, Metrics metrics) {
        long startTime = System.currentTimeMillis();
        
        Objects.requireNonNull(orgChartDataFile);
        Objects.requireNonNull(orgChart);
        
        DelimitedFileReader fileReader = new DelimitedFileReader();
        Map<String, Map<ImmutablePair<String, Long>, Long>> countsByManagerMap = new HashMap<String, Map<ImmutablePair<String, Long>, Long>>();
        
        List<AreaByUserCountContent> areaCounts;
        
        try {
            areaCounts = fileReader.parseFile(orgChartDataFile, new AbstractFileContentFactory<AreaByUserCountContent> () {
                    @Override
                    public AreaByUserCountContent createFileContentObject(String[] splits) throws IllegalArgumentException {
                        if(splits.length != 4) {
                            throw new IllegalArgumentException("Expected 4 values per line in the data file");
                        }
                        
                        try {
                            return new AreaByUserCountContent(splits[0], Long.parseLong(splits[1]), splits[2], Long.parseLong(splits[3]));
                        } catch(NumberFormatException e) {
                            throw new IllegalArgumentException("Expected areaId and count to be numeric values");
                        }
                    }
                }, true);
        } catch(IOException e) {
            LOGGER.error("Error processing orgChart file:" + e, e);
            areaCounts = new LinkedList<AreaByUserCountContent>();
        }
        
        for(AreaByUserCountContent areaCount : areaCounts) {
            for(String mgr : orgChart.getManagersForUser(areaCount.getUser())) {
                Map<ImmutablePair<String, Long>, Long> countsForManager = countsByManagerMap.get(mgr);
                if(countsForManager == null) {
                    countsForManager = new HashMap<ImmutablePair<String, Long>, Long>();
                    countsByManagerMap.put(mgr, countsForManager);
                }
                
                ImmutablePair<String, Long> versionAreaPair = new ImmutablePair<String, Long>(areaCount.getMdVersion(), areaCount.getAreaId());
                Long count = countsForManager.get(versionAreaPair);
                if(count == null) {
                    count = 0L;
                }
                
                countsForManager.put(versionAreaPair, count + areaCount.getCount());
            }
        }
        
        
        OrgChartPopularAreaManager areaMgr = new OrgChartPopularAreaManager();
        
        // Sort mdVersion-areaId pairs for each manager by how frequently they are used
        for(String manager : countsByManagerMap.keySet()) {
            if(countsByManagerMap.get(manager) != null) {
                SortedSet<ImmutablePair<String, Long>> sortedAreaVersions =
                        new TreeSet<ImmutablePair<String, Long>>(new PairComparator(countsByManagerMap.get(manager)));
                
                areaMgr.popularAreasMap.put(manager, sortedAreaVersions);
                
                for(ImmutablePair<String, Long> areaVersion : countsByManagerMap.get(manager).keySet()) {
                    sortedAreaVersions.add(areaVersion);
                }
            }
        }
        
        long stopTime = System.currentTimeMillis();
        if(metrics != null) {
            metrics.addTime(FILE_PROCESSING_TIME_METRIC, stopTime - startTime, SI.MILLI(SI.SECOND));
            
            int numRecords = 0;
            for(SortedSet<ImmutablePair<String, Long>> areaVersions : areaMgr.popularAreasMap.values()) {
                numRecords += areaVersions.size();
            }
            
            metrics.addCount(FILE_SIZE_METRIC, numRecords, Unit.ONE);
        }
        
        return areaMgr;
    }
    
    
    public List<ImmutablePair<String, Long>> getPopularAreasForManager(String mgrLogin) {
        return getPopularAreasForManager(mgrLogin, DEFAULT_LIMIT, DEFAULT_GROUPS);
    }
    
    
    public List<ImmutablePair<String, Long>> getPopularAreasForManager(String mgrLogin, long limit) {
        return getPopularAreasForManager(mgrLogin, limit, DEFAULT_GROUPS);
    }
    
    
    public List<ImmutablePair<String, Long>> getPopularAreasForManager(String mgrLogin, Set<String> groups) {
        return getPopularAreasForManager(mgrLogin, DEFAULT_LIMIT, groups);
    }
    
    
    public List<ImmutablePair<String, Long>> getPopularAreasForManager(String mgrLogin, long limit, Set<String> groups) {
        SortedSet<ImmutablePair<String, Long>> sortedAreaVersions = popularAreasMap.get(mgrLogin);
        List<ImmutablePair<String, Long>> popularAreas = new LinkedList<ImmutablePair<String, Long>>();
        if(sortedAreaVersions != null) {
            Iterator<ImmutablePair<String, Long>> iterator = sortedAreaVersions.iterator();
            
            while(popularAreas.size() < limit && iterator.hasNext()) {
                ImmutablePair<String, Long> key = iterator.next();
                
                if(groups == null || groups.contains(key.getLeft())) {
                    popularAreas.add(key);
                }
            }
        }
        
        return popularAreas;
    }
    
    private static final class PairComparator implements Comparator<ImmutablePair<String, Long>> {
        Map<ImmutablePair<String, Long>, Long> countMap;
        
        public PairComparator(Map<ImmutablePair<String, Long>, Long> countMap) {
            this.countMap = countMap;
        }
        
        @Override
        public int compare(ImmutablePair<String, Long> pairOne, ImmutablePair<String, Long> pairTwo) {
            Long valueOne = countMap.get(pairOne);
            Long valueTwo = countMap.get(pairTwo);
            
            return valueTwo.compareTo(valueOne);
        }
    }
    
    
    private static final class AreaByUserCountContent {
        private String mdVersion, user;
        private long areaId, count;
        
        public AreaByUserCountContent(String mdVersion, long areaId, String user, long count) {
            this.mdVersion = mdVersion;
            this.areaId = areaId;
            this.user = user;
            this.count = count;
        }
        
        
        /**
         * @return the mdVersion
         */
        public String getMdVersion() {
            return mdVersion;
        }
        
        
        /**
         * @return the user
         */
        public String getUser() {
            return user;
        }
        
        
        /**
         * @return the areaId
         */
        public long getAreaId() {
            return areaId;
        }
        
        
        /**
         * @return the count
         */
        public long getCount() {
            return count;
        }
    }
}
