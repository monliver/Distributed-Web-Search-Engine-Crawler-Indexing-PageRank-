package webserver;

import java.util.*;

class SessionImpl implements Session {
    private final String id;
    private final long creationTime;
    private long lastAccessed;
    private int maxInterval = 300; 
    private final Map<String,Object> attrs = new HashMap<>();
    private boolean valid = true;

    SessionImpl(String id) {
        this.id = id;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessed = creationTime;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessed;
    }

    // Update access time
    public void access() {
        this.lastAccessed = System.currentTimeMillis();
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxInterval = seconds;
    }

    public int maxActiveInterval() {
        return maxInterval;
    }

    @Override
    public void invalidate() {
        valid = false;
        attrs.clear();
    }

    @Override
    public Object attribute(String name) {
        return attrs.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        attrs.put(name, value);
    }

    // ---- Extra helper methods for Server ----
    public boolean isValid() {
        return valid;
    }

    public boolean expired() {
        if (!valid) return true;
        return (System.currentTimeMillis() - lastAccessed) > (maxInterval * 1000L);
    }
}
