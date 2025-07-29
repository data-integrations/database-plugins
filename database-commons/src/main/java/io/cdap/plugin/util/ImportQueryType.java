/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.util;

/**
 * Enum to specify the import query type used.
 */
public enum ImportQueryType {
    IMPORT_QUERY("nativeQuery"),
    TABLE_NAME("namedTable");

    private String value;

    ImportQueryType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ImportQueryType fromString(String value) {
        if (value == null) {
            return ImportQueryType.IMPORT_QUERY;
        }

        for (ImportQueryType type : ImportQueryType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        return ImportQueryType.IMPORT_QUERY;
    }
}
