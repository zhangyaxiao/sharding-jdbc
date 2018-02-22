package io.shardingjdbc.core.parsing.parser.clause.expression;

import io.shardingjdbc.core.parsing.lexer.LexerEngine;
import io.shardingjdbc.core.parsing.lexer.token.Literals;
import io.shardingjdbc.core.parsing.lexer.token.Symbol;
import io.shardingjdbc.core.parsing.parser.expression.SQLExpression;
import io.shardingjdbc.core.parsing.parser.expression.SQLIdentifierExpression;
import io.shardingjdbc.core.parsing.parser.expression.SQLIgnoreExpression;
import io.shardingjdbc.core.parsing.parser.expression.SQLNumberExpression;
import io.shardingjdbc.core.parsing.parser.expression.SQLPlaceholderExpression;
import io.shardingjdbc.core.parsing.parser.expression.SQLPropertyExpression;
import io.shardingjdbc.core.parsing.parser.expression.SQLTextExpression;
import io.shardingjdbc.core.parsing.parser.sql.SQLStatement;
import io.shardingjdbc.core.parsing.parser.token.TableToken;
import io.shardingjdbc.core.util.NumberUtil;
import io.shardingjdbc.core.util.SQLUtil;
import lombok.RequiredArgsConstructor;

/**
 * Basic expression parser.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class BasicExpressionParser {
    
    private final LexerEngine lexerEngine;
    
    /**
     * 解析当前的sql表达式
     * Parse expression.
     *
     * @param sqlStatement SQL statement
     * @return expression
     */
    public SQLExpression parse(final SQLStatement sqlStatement) {
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition();
        SQLExpression result = parseExpression(sqlStatement);//解析当前sql表达式
        if (result instanceof SQLPropertyExpression) {//sql表达式 中使用了 表的别名
            setTableToken(sqlStatement, beginPosition, (SQLPropertyExpression) result);
        }
        return result;
    }

    //解析当前sql表达式
    // TODO complete more expression parse
    private SQLExpression parseExpression(final SQLStatement sqlStatement) {
        String literals = lexerEngine.getCurrentToken().getLiterals();
        final int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - literals.length();
        final SQLExpression expression = getExpression(literals, sqlStatement);//根据literals类型转成 SQLExpression对象
        lexerEngine.nextToken();
        if (lexerEngine.skipIfEqual(Symbol.DOT)) {// 符号 .
            String property = lexerEngine.getCurrentToken().getLiterals();
            lexerEngine.nextToken();
            return skipIfCompositeExpression(sqlStatement)//如果是复合表达式，比如存在运算符，则直接取全部的sql表达式返回
                    ? new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition()))
                    : new SQLPropertyExpression(new SQLIdentifierExpression(literals), property);//返回 属性表达式。如 t.userid
        }
        //如果存在() ,则直接返回括号中的全部内容
        if (lexerEngine.equalAny(Symbol.LEFT_PAREN)) {// 符号 (
            lexerEngine.skipParentheses(sqlStatement);//跳过 ()
            skipRestCompositeExpression(sqlStatement);//跳过 复合表达式
            //截取 () 中的 表达式 返回
            return new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition,
                    lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length()).trim());
        }
        return skipIfCompositeExpression(sqlStatement) //如果是复合表达式，比如存在运算符，则直接取全部的sql表达式返回
                ? new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition())) : expression;
    }

    /**
     * 根据类型获得表达式
     * @param literals
     * @param sqlStatement
     * @return
     */
    private SQLExpression getExpression(final String literals, final SQLStatement sqlStatement) {
        if (lexerEngine.equalAny(Symbol.QUESTION)) {// 符号 ?
            sqlStatement.increaseParametersIndex();
            return new SQLPlaceholderExpression(sqlStatement.getParametersIndex() - 1);
        }
        if (lexerEngine.equalAny(Literals.CHARS)) {// char
            return new SQLTextExpression(literals);
        }
        if (lexerEngine.equalAny(Literals.INT)) {//整数
            return new SQLNumberExpression(NumberUtil.getExactlyNumber(literals, 10));
        }
        if (lexerEngine.equalAny(Literals.FLOAT)) {//小数
            return new SQLNumberExpression(Double.parseDouble(literals));
        }
        if (lexerEngine.equalAny(Literals.HEX)) {//十六进制
            return new SQLNumberExpression(NumberUtil.getExactlyNumber(literals, 16));
        }
        if (lexerEngine.equalAny(Literals.IDENTIFIER)) {//标识符
            return new SQLIdentifierExpression(SQLUtil.getExactlyValue(literals));
        }
        return new SQLIgnoreExpression(literals);
    }

    /**
     * 如果是复合表达式，跳过，并返回true
     * @param sqlStatement
     * @return
     */
    private boolean skipIfCompositeExpression(final SQLStatement sqlStatement) {
        if (lexerEngine.equalAny(
                Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT, Symbol.LEFT_PAREN)) {
            lexerEngine.skipParentheses(sqlStatement);
            skipRestCompositeExpression(sqlStatement);
            return true;
        }
        return false;
    }
    
    private void skipRestCompositeExpression(final SQLStatement sqlStatement) {
        while (lexerEngine.skipIfEqual(Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT)) {
            if (lexerEngine.equalAny(Symbol.QUESTION)) {
                sqlStatement.increaseParametersIndex();
            }
            lexerEngine.nextToken();
            lexerEngine.skipParentheses(sqlStatement);
        }
    }
    
    private void setTableToken(final SQLStatement sqlStatement, final int beginPosition, final SQLPropertyExpression propertyExpr) {
        String owner = propertyExpr.getOwner().getName();//表的别名
        if (sqlStatement.getTables().getTableNames().contains(SQLUtil.getExactlyValue(propertyExpr.getOwner().getName()))) {
            //别名保存到 tokens中
            sqlStatement.getSqlTokens().add(new TableToken(beginPosition - owner.length(), owner));
        }
    }
}
