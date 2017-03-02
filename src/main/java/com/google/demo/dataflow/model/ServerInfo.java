/*
    Copyright 2017, Google, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
package com.google.demo.dataflow.model;

import java.io.Serializable;

public class ServerInfo implements Serializable {

    private String region;
    private String server;
    private Integer value;

    public ServerInfo(String region, String server, Integer value) {
        this.region = region;
        this.server = server;
        this.value = value;
    }

    public String getRegion() {
        return region;
    }

    public String getServer() {
        return server;
    }

    public Integer getValue() {
        return value;
    }

    public String getKey(String key) {
        if("region".equals(key)) {
            return region;
        }
        return server;
    }
}
