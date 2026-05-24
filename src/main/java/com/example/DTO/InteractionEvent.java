package com.example.DTO;

public class InteractionEvent {
    private String eventId;
    private String interactionType; // "COMMENT", "DIRECT_MESSAGE", "LIKE"
    private String username;
    private String content;
    private long timestamp;

    // Jackson kütüphanesinin çalışabilmesi için boş bir constructor zorunludur
    public InteractionEvent() {
    }

    public InteractionEvent(String eventId, String interactionType, String username, String content) {
        this.eventId = eventId;
        this.interactionType = interactionType;
        this.username = username;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    // Getter ve Setter metodları Jackson için zorunlu
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getInteractionType() {
        return interactionType;
    }

    public void setInteractionType(String interactionType) {
        this.interactionType = interactionType;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "InteractionEvent{" +
                "type='" + interactionType + '\'' +
                ", user='" + username + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}