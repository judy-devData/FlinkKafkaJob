package org.example;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class CustomSchema implements DeserializationSchema {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String,Object> deserialize(byte[] bytes) throws IOException {
        Map<String,Object> t = null;
        t = mapper.readValue(bytes, Map.class);
        return t;
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation<Map> getProducedType() {
        return TypeInformation.of(new TypeHint<Map>() {
        });
    }
}

