package com.amazon.dw.grasshopper.orgchart;

import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.validate.Validated;
import com.amazon.coral.service.Activity;
import com.amazon.dw.grasshopper.GetManagementChainForUserInput;
import com.amazon.dw.grasshopper.GetManagementChainForUserOutput;

@Service("OrgChartService")
public class OrgChartService extends Activity {
    
    // The organizational chart used to look up managers for a user
    private OrgChart orgChart;
    
    
    @Operation("getManagementChainForUser")
    @Validated
    public GetManagementChainForUserOutput getManagementChainForUser(GetManagementChainForUserInput input) {
        GetManagementChainForUserOutput output = new GetManagementChainForUserOutput();
        output.setManagers(orgChart.getManagersForUser(input.getUsername()));
        return output;
    }
    
    
    public void setOrgChart(OrgChart orgChart) {
        this.orgChart = orgChart;
    }
}
