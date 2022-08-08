package com.meesho.prismdatabricksapi.configs;

import java.util.Map;

public class ApplicationProperties {
    Map<String, String> env;

    public ApplicationProperties() {
        env = System.getenv();
    }

    public String getValue(String key) {
        return env.get(key);
    }
}

