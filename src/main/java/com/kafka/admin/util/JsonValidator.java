package com.kafka.admin.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class JsonValidator {

    private static final Logger logger = LoggerFactory.getLogger(JsonValidator.class);
    private final ObjectMapper objectMapper;

    public JsonValidator() {
        this.objectMapper = new ObjectMapper();
        logger.debug("JsonValidator initialized");
    }

    public boolean isValidJson(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return false;
        }

        try {
            objectMapper.readTree(jsonString);
            logger.debug("JSON validation successful");
            return true;
        } catch (Exception e) {
            logger.debug("JSON validation failed: {}", e.getMessage());
            return false;
        }
    }

    public String validateJsonAndGetError(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return "JSON string is empty";
        }

        try {
            objectMapper.readTree(jsonString);
            return null; // No error
        } catch (Exception e) {
            logger.debug("JSON validation error: {}", e.getMessage());
            return e.getMessage();
        }
    }

    public String prettyPrint(String jsonString) throws Exception {
        Object json = objectMapper.readValue(jsonString, Object.class);
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    }

    public String minify(String jsonString) throws Exception {
        Object json = objectMapper.readValue(jsonString, Object.class);
        return objectMapper.writeValueAsString(json);
    }
}