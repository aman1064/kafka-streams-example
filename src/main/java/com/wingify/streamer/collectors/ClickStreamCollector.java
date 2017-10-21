package com.wingify.streamer.collectors;

import com.wingify.streamer.models.ClickStream;
import com.wingify.streamer.utils.ApplicationUtils;

public class ClickStreamCollector {

    private String sessionId;
    private String uuid;
    private Integer clickCount = 0;
    private Integer pageView = 0;
    private Integer totalTimeSpent = 0;

    public ClickStreamCollector add(ClickStream hit) {
        ApplicationUtils.getClickMeter().mark();
        if (this.sessionId == null) {
            this.sessionId = hit.getSessionId();
        }
        if (this.uuid == null) {
            this.uuid = hit.getUuid();
        }
        if (hit.getEvent().equalsIgnoreCase("click"))
            this.clickCount++;
        if (hit.getEvent().equalsIgnoreCase("page_view"))
            this.pageView++;
        if (this.totalTimeSpent < hit.getTimeSpent())
            this.totalTimeSpent = hit.getTimeSpent();
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Integer getClickCount() {
        return clickCount;
    }

    public void setClickCount(Integer clickCount) {
        this.clickCount = clickCount;
    }

    public Integer getPageView() {
        return pageView;
    }

    public void setPageView(Integer pageView) {
        this.pageView = pageView;
    }

    public Integer getTotalTimeSpent() {
        return totalTimeSpent;
    }

    public void setTotalTimeSpent(Integer totalTimeSpent) {
        this.totalTimeSpent = totalTimeSpent;
    }
}
