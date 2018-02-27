/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingjdbc.core.parsing.parser.token;

import io.shardingjdbc.core.util.SQLUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Schema token.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@ToString
public final class SchemaToken implements SQLToken {
    
    @Getter
    private final int beginPosition;
    
    @Getter
    private final String originalLiterals;
    
    private final String tableName;
    
    /**
     * Get schema name.
     * 
     * @return schema name
     */
    public String getSchemaName() {
        return SQLUtil.getExactlyValue(originalLiterals);
    }
    
    /**
     * Get table name.
     *
     * @return table name
     */
    public String getTableName() {
        return SQLUtil.getExactlyValue(tableName);
    }
}
