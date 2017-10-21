package org.streams.demo.models;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "uuid",
        "session_id",
        "event",
        "time_spent"
})
public class ClickStream {

    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("session_id")
    private String sessionId;
    @JsonProperty("event")
    private String event;
    @JsonProperty("time_spent")
    private Integer timeSpent;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("uuid")
    public String getUuid() {
        return uuid;
    }

    @JsonProperty("uuid")
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @JsonProperty("session_id")
    public String getSessionId() {
        return sessionId;
    }

    @JsonProperty("session_id")
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @JsonProperty("event")
    public String getEvent() {
        return event;
    }

    @JsonProperty("event")
    public void setEvent(String event) {
        this.event = event;
    }

    @JsonProperty("time_spent")
    public Integer getTimeSpent() {
        return timeSpent;
    }

    @JsonProperty("time_spent")
    public void setTimeSpent(Integer timeSpent) {
        this.timeSpent = timeSpent;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}