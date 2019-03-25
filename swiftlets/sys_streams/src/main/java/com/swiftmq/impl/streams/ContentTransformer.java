/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.impl.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ContentTransformer {

    ReadContext readContext = null;

    public String XMLtoJSON(String xml, boolean pretty) throws Exception {
        JSONObject jObject = XML.toJSONObject(xml);
        ObjectMapper mapper = new ObjectMapper();
        if (pretty)
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
        Object json = mapper.readValue(jObject.toString(), Object.class);
        return mapper.writeValueAsString(json);
    }

    public void setBody(String body, boolean isXML) throws Exception {
        String json = isXML ? XMLtoJSON(body, false) : body;
        readContext = JsonPath.parse(json);
    }

    public List<Object> selectJSON(String jsonPath) throws Exception {
        Object object = readContext.read(jsonPath);
        if (object instanceof List)
            return (List<Object>) object;
        List<Object> list = new ArrayList<Object>();
        list.add(object);
        return list;
    }

    public List<String> getJsonPaths(String json) throws Exception {
        return new JsonParser(json).getPathList();
    }

    private class JsonParser {

        private List<String> pathList;
        private String json;

        public JsonParser(String json) {
            this.json = json;
            this.pathList = new ArrayList<String>();
            setJsonPaths(json);
        }

        public List<String> getPathList() {
            return this.pathList;
        }

        private void setJsonPaths(String json) {
            this.pathList = new ArrayList<String>();
            JSONObject object = new JSONObject(json);
            String jsonPath = "$";
            if (json != JSONObject.NULL) {
                readObject(object, jsonPath);
            }
        }

        private void readObject(JSONObject object, String jsonPath) {
            Iterator<String> keysItr = object.keys();
            String parentPath = jsonPath;
            while (keysItr.hasNext()) {
                String key = keysItr.next();
                Object value = object.get(key);
                jsonPath = parentPath + "." + key;

                if (value instanceof JSONArray) {
                    readArray((JSONArray) value, jsonPath);
                } else if (value instanceof JSONObject) {
                    readObject((JSONObject) value, jsonPath);
                } else { // is a value
                    this.pathList.add(jsonPath);
                }
            }
        }

        private void readArray(JSONArray array, String jsonPath) {
            String parentPath = jsonPath;
            jsonPath = parentPath + "[*]";
            this.pathList.add(jsonPath);
            for (int i = 0; i < array.length(); i++) {
                Object value = array.get(i);
                if (value instanceof JSONArray) {
                    readArray((JSONArray) value, jsonPath);
                } else if (value instanceof JSONObject) {
                    readObject((JSONObject) value, jsonPath);
                }
            }
        }

    }
}
