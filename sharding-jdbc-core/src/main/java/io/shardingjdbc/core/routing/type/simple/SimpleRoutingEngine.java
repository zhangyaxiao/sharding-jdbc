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

package io.shardingjdbc.core.routing.type.simple;

import io.shardingjdbc.core.api.algorithm.sharding.ShardingValue;
import io.shardingjdbc.core.hint.HintManagerHolder;
import io.shardingjdbc.core.hint.ShardingKey;
import io.shardingjdbc.core.parsing.parser.context.condition.Column;
import io.shardingjdbc.core.parsing.parser.context.condition.Condition;
import io.shardingjdbc.core.parsing.parser.sql.SQLStatement;
import io.shardingjdbc.core.routing.strategy.ShardingStrategy;
import io.shardingjdbc.core.routing.type.RoutingEngine;
import io.shardingjdbc.core.routing.type.RoutingResult;
import io.shardingjdbc.core.routing.type.TableUnit;
import io.shardingjdbc.core.rule.DataNode;
import io.shardingjdbc.core.rule.ShardingRule;
import io.shardingjdbc.core.rule.TableRule;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple routing engine.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class SimpleRoutingEngine implements RoutingEngine {

    /**
     * 分表策略
     */
    private final ShardingRule shardingRule;

    /**
     * 参数
     */
    private final List<Object> parameters;

    /**
     * 逻辑表名
     */
    private final String logicTableName;

    /**
     * sql 解析结果
     */
    private final SQLStatement sqlStatement;
    
    @Override
    public RoutingResult route() {
        TableRule tableRule = shardingRule.getTableRule(logicTableName);//根据逻辑表名 获得分库分表策略
        List<ShardingValue> databaseShardingValues = getDatabaseShardingValues(tableRule);//根据策略得到分库规则
        List<ShardingValue> tableShardingValues = getTableShardingValues(tableRule);//根据策略得到分表规则
        Collection<String> routedDataSources = routeDataSources(tableRule, databaseShardingValues);//得到需要执行sql的库名
        Collection<DataNode> routedDataNodes = new LinkedList<>();//实际表名
        for (String each : routedDataSources) {
            //根据数据库名称和分表规则，得到全部的 DataNode
            routedDataNodes.addAll(routeTables(tableRule, each, tableShardingValues));
        }
        return generateRoutingResult(routedDataNodes);
    }

    /**
     * 根据策略得到分库规则
     * @param tableRule
     * @return
     */
    private List<ShardingValue> getDatabaseShardingValues(final TableRule tableRule) {
        ShardingStrategy strategy = shardingRule.getDatabaseShardingStrategy(tableRule);//根据策略得到分库规则
        // sql中的查询条件中 是否包含分片字段 ；如果包含 返回 ShardingValue
        return HintManagerHolder.isUseShardingHint() ? getDatabaseShardingValuesFromHint(strategy.getShardingColumns()) : getShardingValues(strategy.getShardingColumns());
    }

    /**
     * 根据策略得到分表规则
     */
    private List<ShardingValue> getTableShardingValues(final TableRule tableRule) {
        ShardingStrategy strategy = shardingRule.getTableShardingStrategy(tableRule);
        // sql中的查询条件中 是否包含分片字段 ；如果包含 返回 ShardingValue
        return HintManagerHolder.isUseShardingHint() ? getTableShardingValuesFromHint(strategy.getShardingColumns()) : getShardingValues(strategy.getShardingColumns());
    }
    
    private List<ShardingValue> getDatabaseShardingValuesFromHint(final Collection<String> shardingColumns) {
        List<ShardingValue> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<ShardingValue> shardingValue = HintManagerHolder.getDatabaseShardingValue(new ShardingKey(logicTableName, each));
            if (shardingValue.isPresent()) {
                result.add(shardingValue.get());
            }
        }
        return result;
    }
    
    private List<ShardingValue> getTableShardingValuesFromHint(final Collection<String> shardingColumns) {
        List<ShardingValue> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<ShardingValue> shardingValue = HintManagerHolder.getTableShardingValue(new ShardingKey(logicTableName, each));
            if (shardingValue.isPresent()) {
                result.add(shardingValue.get());
            }
        }
        return result;
    }

    /**
     * sql中的查询条件中 是否包含分片字段
     * 如果包含 返回 ShardingValue
     * @param shardingColumns 分片字段。用于将数据库(表)水平拆分的关键字段
     * @return
     */
    private List<ShardingValue> getShardingValues(final Collection<String> shardingColumns) {
        List<ShardingValue> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<Condition> condition = sqlStatement.getConditions().find(new Column(each, logicTableName));
            if (condition.isPresent()) {
                result.add(condition.get().getShardingValue(parameters));
            }
        }
        return result;
    }

    /**
     * 路由 得到需要执行sql的 数据库名称
     * @param tableRule
     * @param databaseShardingValues
     * @return
     */
    private Collection<String> routeDataSources(final TableRule tableRule, final List<ShardingValue> databaseShardingValues) {
        //获取实际的 全部数据库名称
        Collection<String> availableTargetDatabases = tableRule.getActualDatasourceNames();
        //如果没有分库规则，返回所有的数据库
        if (databaseShardingValues.isEmpty()) {
            return availableTargetDatabases;
        }
        //根据shardingValue 中的值，计算出对应的数据库名称
        Collection<String> result = shardingRule.getDatabaseShardingStrategy(tableRule).doSharding(availableTargetDatabases, databaseShardingValues);
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        return result;
    }

    /**
     * 根据数据库名称 和分表规则 得到 DataNode
     * @param tableRule 分片策略
     * @param routedDataSource 数据库名称
     * @param tableShardingValues 分表规则
     * @return
     */
    private Collection<DataNode> routeTables(final TableRule tableRule, final String routedDataSource, final List<ShardingValue> tableShardingValues) {
        Collection<String> availableTargetTables = tableRule.getActualTableNames(routedDataSource);
        Collection<String> routedTables = tableShardingValues.isEmpty() ? availableTargetTables
                : shardingRule.getTableShardingStrategy(tableRule).doSharding(availableTargetTables, tableShardingValues);
        Preconditions.checkState(!routedTables.isEmpty(), "no table route info");
        Collection<DataNode> result = new LinkedList<>();
        for (String each : routedTables) {
            result.add(new DataNode(routedDataSource, each));
        }
        return result;
    }

    /**
     * 生成路由结果
     * @param routedDataNodes
     * @return
     */
    private RoutingResult generateRoutingResult(final Collection<DataNode> routedDataNodes) {
        RoutingResult result = new RoutingResult();
        for (DataNode each : routedDataNodes) {
            result.getTableUnits().getTableUnits().add(new TableUnit(each.getDataSourceName(), logicTableName, each.getTableName()));
        }
        return result;
    }
}
