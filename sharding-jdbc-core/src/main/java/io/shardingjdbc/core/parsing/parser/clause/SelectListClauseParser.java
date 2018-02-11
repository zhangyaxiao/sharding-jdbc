package io.shardingjdbc.core.parsing.parser.clause;

import io.shardingjdbc.core.parsing.parser.clause.expression.AliasExpressionParser;
import io.shardingjdbc.core.parsing.parser.dialect.ExpressionParserFactory;
import io.shardingjdbc.core.rule.ShardingRule;
import io.shardingjdbc.core.constant.AggregationType;
import io.shardingjdbc.core.parsing.lexer.LexerEngine;
import io.shardingjdbc.core.parsing.lexer.token.DefaultKeyword;
import io.shardingjdbc.core.parsing.lexer.token.Keyword;
import io.shardingjdbc.core.parsing.lexer.token.Symbol;
import io.shardingjdbc.core.parsing.parser.context.selectitem.AggregationSelectItem;
import io.shardingjdbc.core.parsing.parser.context.selectitem.CommonSelectItem;
import io.shardingjdbc.core.parsing.parser.context.selectitem.SelectItem;
import io.shardingjdbc.core.parsing.parser.context.selectitem.StarSelectItem;
import io.shardingjdbc.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingjdbc.core.parsing.parser.token.TableToken;
import io.shardingjdbc.core.util.SQLUtil;
import com.google.common.base.Optional;
import lombok.Getter;

import java.util.List;

/**
 * Select list clause parser.
 *
 * @author zhangliang
 */
@Getter
public class SelectListClauseParser implements SQLClauseParser {
    
    private final ShardingRule shardingRule;
    
    private final LexerEngine lexerEngine;
    
    private final AliasExpressionParser aliasExpressionParser;
    
    public SelectListClauseParser(final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        this.shardingRule = shardingRule;
        this.lexerEngine = lexerEngine;
        aliasExpressionParser = ExpressionParserFactory.createAliasExpressionParser(lexerEngine);
    }
    
    /**
     * Parse select list.
     * 
     * @param selectStatement select statement
     * @param items select items
     */
    public void parse(final SelectStatement selectStatement, final List<SelectItem> items) {
        do {
            selectStatement.getItems().add(parseSelectItem(selectStatement));
        } while (lexerEngine.skipIfEqual(Symbol.COMMA));
        //计算 最后一个查询项下一个token的开始位置
        selectStatement.setSelectListLastPosition(lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length());
        //将 查询项解析结果 放入select分析器中
        items.addAll(selectStatement.getItems());
    }
    
    private SelectItem parseSelectItem(final SelectStatement selectStatement) {
        lexerEngine.skipIfEqual(getSkippedKeywordsBeforeSelectItem());//跳过部分词法
        SelectItem result;
        if (isRowNumberSelectItem()) {//sqlserver关键字 ROW_NUMBER
            result = parseRowNumberSelectItem(selectStatement);
        } else if (isStarSelectItem()) {//是否查询 *
            selectStatement.setContainStar(true);
            result = parseStarSelectItem();//selectItem是空的
        } else if (isAggregationSelectItem()) {//集合项 MAX MIN SUM AVG COUNT
            //TODO zyx 查询项解析
            result = parseAggregationSelectItem(selectStatement);
            parseRestSelectItem(selectStatement);//
        } else {
            //创建CommonSelectItem，包含查询项表达语句，和别名
            result = new CommonSelectItem(SQLUtil.getExactlyValue(parseCommonSelectItem(selectStatement) + parseRestSelectItem(selectStatement)), aliasExpressionParser.parseSelectItemAlias());
        }
        return result;
    }
    
    protected Keyword[] getSkippedKeywordsBeforeSelectItem() {
        return new Keyword[0];
    }
    
    protected boolean isRowNumberSelectItem() {
        return false;
    }
    
    protected SelectItem parseRowNumberSelectItem(final SelectStatement selectStatement) {
        throw new UnsupportedOperationException("Cannot support special select item.");
    }
    
    private boolean isStarSelectItem() {
        return Symbol.STAR.getLiterals().equals(SQLUtil.getExactlyValue(lexerEngine.getCurrentToken().getLiterals()));
    }
    
    private SelectItem parseStarSelectItem() {
        lexerEngine.nextToken();
        aliasExpressionParser.parseSelectItemAlias();//跳过一部分词法
        return new StarSelectItem(Optional.<String>absent());//返回一个空的selectItem
    }
    
    private boolean isAggregationSelectItem() {
        return lexerEngine.equalAny(DefaultKeyword.MAX, DefaultKeyword.MIN, DefaultKeyword.SUM, DefaultKeyword.AVG, DefaultKeyword.COUNT);
    }

    private SelectItem parseAggregationSelectItem(final SelectStatement selectStatement) {
        AggregationType aggregationType = AggregationType.valueOf(lexerEngine.getCurrentToken().getLiterals().toUpperCase());
        lexerEngine.nextToken();
        return new AggregationSelectItem(aggregationType, lexerEngine.skipParentheses(selectStatement), aliasExpressionParser.parseSelectItemAlias());
    }

    private String parseCommonSelectItem(final SelectStatement selectStatement) {
        String literals = lexerEngine.getCurrentToken().getLiterals();
        int position = lexerEngine.getCurrentToken().getEndPosition() - literals.length();
        StringBuilder result = new StringBuilder();
        result.append(literals);
        lexerEngine.nextToken();
        if (lexerEngine.equalAny(Symbol.LEFT_PAREN)) {
            result.append(lexerEngine.skipParentheses(selectStatement));
        } else if (lexerEngine.equalAny(Symbol.DOT)) {
            String tableName = SQLUtil.getExactlyValue(literals);
            if (shardingRule.tryFindTableRule(tableName).isPresent() || shardingRule.findBindingTableRule(tableName).isPresent()) {
                selectStatement.getSqlTokens().add(new TableToken(position, literals));
            }
            result.append(lexerEngine.getCurrentToken().getLiterals());
            lexerEngine.nextToken();
            result.append(lexerEngine.getCurrentToken().getLiterals());
            lexerEngine.nextToken();
        }
        return result.toString();
    }

    private String parseRestSelectItem(final SelectStatement selectStatement) {
        StringBuilder result = new StringBuilder();
        while (lexerEngine.equalAny(Symbol.getOperators())) {//如果是符号
            result.append(lexerEngine.getCurrentToken().getLiterals());
            lexerEngine.nextToken();
            result.append(parseCommonSelectItem(selectStatement));
        }
        return result.toString();
    }
}
