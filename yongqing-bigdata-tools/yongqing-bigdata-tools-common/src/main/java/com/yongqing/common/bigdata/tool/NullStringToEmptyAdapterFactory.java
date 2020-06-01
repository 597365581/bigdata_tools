package com.yongqing.common.bigdata.tool;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;


@SuppressWarnings("unchecked")
public class NullStringToEmptyAdapterFactory implements TypeAdapterFactory {
    public TypeAdapter create(Gson gson, TypeToken type) {
        Class rawType = (Class) type.getRawType();
        if (rawType != String.class) {
            return null;
        }
        return (TypeAdapter) new StringNullAdapter();
    }
}

// StringNullAdapter.java
 class StringNullAdapter extends TypeAdapter<String> {

    @Override
    public String read(JsonReader reader) throws IOException {
        if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return "";
        }
        return reader.nextString();
    }

    @Override
    public void write(JsonWriter writer, String value) throws IOException {
        if (value == null) {
            writer.value("");
            return;
        }
        writer.value(value);
    }

}
