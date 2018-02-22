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

package io.shardingjdbc.core.parsing.lexer;

import com.google.common.collect.Sets;
import io.shardingjdbc.core.constant.DatabaseType;
import io.shardingjdbc.core.parsing.lexer.dialect.mysql.MySQLLexer;
import io.shardingjdbc.core.parsing.lexer.dialect.oracle.OracleLexer;
import io.shardingjdbc.core.parsing.lexer.dialect.postgresql.PostgreSQLLexer;
import io.shardingjdbc.core.parsing.lexer.dialect.sqlserver.SQLServerLexer;
import io.shardingjdbc.core.parsing.lexer.token.Assist;
import io.shardingjdbc.core.parsing.lexer.token.Symbol;
import io.shardingjdbc.core.parsing.lexer.token.Token;
import io.shardingjdbc.core.parsing.lexer.token.TokenType;
import io.shardingjdbc.core.parsing.parser.exception.SQLParsingException;
import io.shardingjdbc.core.parsing.parser.exception.SQLParsingUnsupportedException;
import io.shardingjdbc.core.parsing.parser.sql.SQLStatement;
import lombok.RequiredArgsConstructor;

import java.util.Set;

/**
 * 分词引擎
 * Lexical analysis engine.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class LexerEngine {

    /**
     * 词法解析器
     */
    private final Lexer lexer;
    
    /**
     * 获得 SQL
     * Get input string.
     * 
     * @return inputted string
     */
    public String getInput() {
        return lexer.getInput();
    }
    
    /**
     * 下一个词法标记
     * Analyse next token.
     */
    public void nextToken() {
        lexer.nextToken();
    }
    
    /**
     * 当前的词法标记
     * Get current token.
     * 
     * @return current token
     */
    public Token getCurrentToken() {
        return lexer.getCurrentToken();
    }
    
    /**
     * 返回
     * skip all tokens that inside parentheses.
     *
     * @param sqlStatement SQL statement
     * @return skipped string
     */
    public String skipParentheses(final SQLStatement sqlStatement) {
        StringBuilder result = new StringBuilder("");
        int count = 0;
        if (Symbol.LEFT_PAREN == lexer.getCurrentToken().getType()) {
            final int beginPosition = lexer.getCurrentToken().getEndPosition();
            result.append(Symbol.LEFT_PAREN.getLiterals());
            lexer.nextToken();
            while (true) {
                if (equalAny(Symbol.QUESTION)) {
                    sqlStatement.increaseParametersIndex();
                }
                if (Assist.END == lexer.getCurrentToken().getType() || (Symbol.RIGHT_PAREN == lexer.getCurrentToken().getType() && 0 == count)) {
                    break;
                }
                if (Symbol.LEFT_PAREN == lexer.getCurrentToken().getType()) {
                    count++;
                } else if (Symbol.RIGHT_PAREN == lexer.getCurrentToken().getType()) {
                    count--;
                }
                lexer.nextToken();
            }
            result.append(lexer.getInput().substring(beginPosition, lexer.getCurrentToken().getEndPosition()));
            lexer.nextToken();
        }
        return result.toString();
    }
    
    /**
     * Assert current token type should equals input token and go to next token type.
     *
     * @param tokenType token type
     */
    public void accept(final TokenType tokenType) {
        if (lexer.getCurrentToken().getType() != tokenType) {
            throw new SQLParsingException(lexer, tokenType);
        }
        lexer.nextToken();
    }
    
    /**
     * 判断当前令牌是否等于输入的令牌其中一个
     * Adjust current token equals one of input tokens or not.
     *
     * @param tokenTypes to be adjusted token types
     * @return current token equals one of input tokens or not
     */
    public boolean equalAny(final TokenType... tokenTypes) {
        for (TokenType each : tokenTypes) {
            if (each == lexer.getCurrentToken().getType()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 判断当前令牌是否等于输入的令牌其中一个
     * 如果等于跳过参数中的token，并且返回true
     * Skip current token if equals one of input tokens.
     *
     * @param tokenTypes to be adjusted token types
     * @return skipped current token or not
     */
    public boolean skipIfEqual(final TokenType... tokenTypes) {
        if (equalAny(tokenTypes)) {
            lexer.nextToken();
            return true;
        }
        return false;
    }
    
    /**
     * 跳过参数中的token
     * Skip all input tokens.
     *
     * @param tokenTypes to be skipped token types
     */
    public void skipAll(final TokenType... tokenTypes) {
        Set<TokenType> tokenTypeSet = Sets.newHashSet(tokenTypes);
        while (tokenTypeSet.contains(lexer.getCurrentToken().getType())) {
            lexer.nextToken();
        }
    }
    
    /**
     * Skip until one of input tokens.
     *
     * @param tokenTypes to be skipped untiled token types
     */
    public void skipUntil(final TokenType... tokenTypes) {
        Set<TokenType> tokenTypeSet = Sets.newHashSet(tokenTypes);
        tokenTypeSet.add(Assist.END);
        while (!tokenTypeSet.contains(lexer.getCurrentToken().getType())) {
            lexer.nextToken();
        }
    }
    
    /**
     * 如果当前令牌等于输入令牌的其中一个，则抛出不支持的异常
     * Throw unsupported exception if current token equals one of input tokens.
     * 
     * @param tokenTypes to be adjusted token types
     */
    public void unsupportedIfEqual(final TokenType... tokenTypes) {
        if (equalAny(tokenTypes)) {
            throw new SQLParsingUnsupportedException(lexer.getCurrentToken().getType());
        }
    }
    
    /**
     * Throw unsupported exception if current token not equals one of input tokens.
     *
     * @param tokenTypes to be adjusted token types
     */
    public void unsupportedIfNotSkip(final TokenType... tokenTypes) {
        if (!skipIfEqual(tokenTypes)) {
            throw new SQLParsingUnsupportedException(lexer.getCurrentToken().getType());
        }
    }
    
    /**
     * Get database type.
     * 
     * @return database type
     */
    public DatabaseType getDatabaseType() {
        if (lexer instanceof MySQLLexer) {
            return DatabaseType.MySQL;
        }
        if (lexer instanceof OracleLexer) {
            return DatabaseType.Oracle;
        }
        if (lexer instanceof SQLServerLexer) {
            return DatabaseType.SQLServer;
        }
        if (lexer instanceof PostgreSQLLexer) {
            return DatabaseType.PostgreSQL;
        }
        throw new UnsupportedOperationException(String.format("Cannot support lexer class: %s", lexer.getClass().getCanonicalName()));
    }
}
