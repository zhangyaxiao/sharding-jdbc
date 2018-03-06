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

package io.shardingjdbc.core.parsing.parser.sql;

import io.shardingjdbc.core.constant.DatabaseType;
import io.shardingjdbc.core.parsing.lexer.LexerEngine;
import io.shardingjdbc.core.parsing.lexer.dialect.mysql.MySQLKeyword;
import io.shardingjdbc.core.parsing.lexer.token.DefaultKeyword;
import io.shardingjdbc.core.parsing.lexer.token.Keyword;
import io.shardingjdbc.core.parsing.lexer.token.TokenType;
import io.shardingjdbc.core.parsing.parser.exception.SQLParsingUnsupportedException;
import io.shardingjdbc.core.parsing.parser.sql.dal.describe.DescribeParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.dal.show.ShowParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.dal.use.UseParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.ddl.alter.AlterParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.ddl.create.CreateParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.ddl.drop.DropParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.ddl.truncate.TruncateParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.dml.delete.DeleteParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.dml.insert.InsertParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.dml.update.UpdateParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.dql.select.SelectParserFactory;
import io.shardingjdbc.core.parsing.parser.sql.tcl.TCLParserFactory;
import io.shardingjdbc.core.rule.ShardingRule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * SQL parser factory.
 *
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SQLParserFactory {
    
    /**
     * Create SQL parser.
     *
     * @param dbType database type
     * @param tokenType token type
     * @param shardingRule databases and tables sharding rule
     * @param lexerEngine lexical analysis engine
     * @return SQL parser
     */
    public static SQLParser newInstance(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        if (isDQL(tokenType)) {
            return getDQLParser(dbType, shardingRule, lexerEngine);
        }
        if (isDML(tokenType)) {
            return getDMLParser(dbType, tokenType, shardingRule, lexerEngine);
        }
        if (isDDL(tokenType)) {
            return getDDLParser(dbType, tokenType, shardingRule, lexerEngine);
        }
        if (isTCL(tokenType)) {
            return getTCLParser(dbType, shardingRule, lexerEngine);
        }
        if (isDAL(tokenType)) {
            return getDALParser(dbType, (Keyword) tokenType, shardingRule, lexerEngine);
        }
        throw new SQLParsingUnsupportedException(tokenType);
    }
    
    private static boolean isDQL(final TokenType tokenType) {
        return DefaultKeyword.SELECT == tokenType;
    }
    
    private static boolean isDML(final TokenType tokenType) {
        return DefaultKeyword.INSERT == tokenType || DefaultKeyword.UPDATE == tokenType || DefaultKeyword.DELETE == tokenType;
    }
    
    private static boolean isDDL(final TokenType tokenType) {
        return DefaultKeyword.CREATE == tokenType || DefaultKeyword.ALTER == tokenType || DefaultKeyword.DROP == tokenType || DefaultKeyword.TRUNCATE == tokenType;
    }
    
    private static boolean isTCL(final TokenType tokenType) {
        return DefaultKeyword.SET == tokenType || DefaultKeyword.COMMIT == tokenType || DefaultKeyword.ROLLBACK == tokenType
                || DefaultKeyword.SAVEPOINT == tokenType || DefaultKeyword.BEGIN == tokenType;
    }
    
    private static boolean isDAL(final TokenType tokenType) {
        return DefaultKeyword.USE == tokenType || DefaultKeyword.DESC == tokenType || MySQLKeyword.DESCRIBE == tokenType || MySQLKeyword.SHOW == tokenType;
    }
    
    private static SQLParser getDQLParser(final DatabaseType dbType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        return SelectParserFactory.newInstance(dbType, shardingRule, lexerEngine);
    }
    
    private static SQLParser getDMLParser(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        switch ((DefaultKeyword) tokenType) {
            case INSERT:
                return InsertParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case UPDATE:
                return UpdateParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DELETE:
                return DeleteParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            default:
                throw new SQLParsingUnsupportedException(tokenType);
        }
    }
    
    private static SQLParser getDDLParser(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        switch ((DefaultKeyword) tokenType) {
            case CREATE:
                return CreateParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case ALTER:
                return AlterParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DROP:
                return DropParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case TRUNCATE:
                return TruncateParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            default:
                throw new SQLParsingUnsupportedException(tokenType);
        }
    }
    
    private static SQLParser getTCLParser(final DatabaseType dbType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        return TCLParserFactory.newInstance(dbType, shardingRule, lexerEngine);
    }
    
    private static SQLParser getDALParser(final DatabaseType dbType, final Keyword tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        if (DefaultKeyword.USE == tokenType) {
            return UseParserFactory.newInstance(dbType, shardingRule, lexerEngine);
        }
        if (DefaultKeyword.DESC == tokenType || MySQLKeyword.DESCRIBE == tokenType) {
            return DescribeParserFactory.newInstance(dbType, shardingRule, lexerEngine);
        }
        if (MySQLKeyword.SHOW == tokenType) {
            return ShowParserFactory.newInstance(dbType, shardingRule, lexerEngine);
        }
        throw new SQLParsingUnsupportedException(tokenType);
    }
}
