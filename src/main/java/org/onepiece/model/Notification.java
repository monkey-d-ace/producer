package org.onepiece.model;

import lombok.Data;

import java.util.Date;

@Data
public class Notification {
    private Long scenarioId;
    private Date asOfDate;
    private Long datasetId;
    private String positionId;
}
