/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
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
 */

package com.alibaba.nacos.plugin.datasource.impl.postgresql;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.dialect.DatabaseDialect;
import com.alibaba.nacos.plugin.datasource.enums.postgre.TrustedPostgreFunctionEnum;
import com.alibaba.nacos.plugin.datasource.impl.mysql.ConfigInfoBetaMapperByMySql;
import com.alibaba.nacos.plugin.datasource.manager.DatabaseDialectManager;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: nacos
 * @ClassName BaseConfigInfoBetaMapper
 * @description:
 * @author: zhang wenchao
 * @create: 2024-07-24 15:43
 * @Version 1.0
 **/
public abstract class BasePostgreMapper extends AbstractMapper {

    public BasePostgreMapper() {
    }

    /**
     * return the table name
     * @author zhang wenchao
     * 2024/7/24 10:19
     * @return java.lang.String table name
     */
    @Override
    public abstract String getTableName();
    @Override
    public String getDataSource() {
        return DataSourceConstant.POSTGRESQL;
    }

    @Override
    public String getFunction(String functionName) {
        return TrustedPostgreFunctionEnum.getFunctionByName(functionName);
    }

    @Override
    public String select(List<String> columns, List<String> where) {
        StringBuilder sql = new StringBuilder("SELECT ");

        for (int i = 0; i < columns.size(); i++) {
            sql.append(columns.get(i));
            if (i == columns.size() - 1) {
                sql.append(" ");
            }
            else {
                sql.append(",");
            }
        }
        sql.append("FROM ");
        sql.append(getTableName());
        sql.append(" ");

        if (where.size() == 0) {
            return sql.toString();
        }

        sql.append("WHERE ");
        for (int i = 0; i < where.size(); i++) {
            String column = where.get(i);

            // 租户列特殊处理 避免前端传空字符串是Oracle查询不到数据
            if ("tenant_id".equalsIgnoreCase(column)) {
                sql.append("(");
                sql.append(column).append(" = ").append("?");
                sql.append(" OR ");
                sql.append(column).append(" IS NULL ");
                sql.append(")");
            }
            else {
                sql.append(column).append(" = ").append("?");
            }

            if (i != where.size() - 1) {
                sql.append(" AND ");
            }
        }
        return sql.toString();
    }

    @Override
    public String update(List<String> columns, List<String> where) {
        StringBuilder sql = new StringBuilder();
        String method = "UPDATE ";
        sql.append(method);
        sql.append(getTableName()).append(" ").append("SET ");

        for (int i = 0; i < columns.size(); i++) {
            sql.append(columns.get(i)).append(" = ").append("?");
            if (i != columns.size() - 1) {
                sql.append(",");
            }
        }

        if (where.size() == 0) {
            return sql.toString();
        }

        sql.append(" WHERE ");

        for (int i = 0; i < where.size(); i++) {
            String column = where.get(i);
            if ("tenant_id".equalsIgnoreCase(column)) {
                sql.append("(");
                sql.append(column).append(" = ").append("?");
                sql.append(" OR ");
                sql.append(column).append(" IS NULL ");
                sql.append(")");
            }
            else {
                sql.append(column).append(" = ").append("?");
            }
            if (i != where.size() - 1) {
                sql.append(" AND ");
            }
        }
        return sql.toString();
    }

    @Override
    public String delete(List<String> params) {
        StringBuilder sql = new StringBuilder();
        String method = "DELETE ";
        sql.append(method).append("FROM ").append(getTableName()).append(" ").append("WHERE ");
        for (int i = 0; i < params.size(); i++) {
            String column = params.get(i);
            if ("tenant_id".equalsIgnoreCase(column)) {
                sql.append("(");
                sql.append(column).append(" = ").append("?");
                sql.append(" OR ");
                sql.append(column).append(" IS NULL ");
                sql.append(")");
            }
            else {
                sql.append(column).append(" = ").append("?");
            }
            if (i != params.size() - 1) {
                sql.append("AND ");
            }
        }

        return sql.toString();
    }

    @Override
    public String count(List<String> where) {
        StringBuilder sql = new StringBuilder();
        String method = "SELECT ";
        sql.append(method);
        sql.append("COUNT(*) FROM ");
        sql.append(getTableName());
        sql.append(" ");

        if (null == where || where.size() == 0) {
            return sql.toString();
        }

        sql.append("WHERE ");
        for (int i = 0; i < where.size(); i++) {
            String column = where.get(i);
            if ("tenant_id".equalsIgnoreCase(column)) {
                sql.append("(");
                sql.append(column).append(" = ").append("?");
                sql.append(" OR ");
                sql.append(column).append(" IS NULL ");
                sql.append(")");
            }
            else {
                sql.append(column).append(" = ").append("?");
            }
            if (i != where.size() - 1) {
                sql.append(" AND ");
            }
        }
        return sql.toString();
    }

    public String getLimitTopSqlWithMark(String sql) {
        return sql + " LIMIT ? ";
    }
    public String getLimitTopSqlWithMark(String sql,int pageSize) {
        return sql + " LIMIT ? ";
    }

    public String getLimitPageSqlWithMark(String sql) {
        return sql + "  LIMIT ?  OFFSET ? ";
    }

    public String getLimitPageSql(String sql, int pageNo, int pageSize) {
        return sql  + " LIMIT " + pageSize + "  OFFSET " + pageNo;
    }

    public String getLimitPageSqlWithOffset(String sql, int startOffset, int pageSize){
        return sql  + " LIMIT " + pageSize + "  OFFSET " + startOffset;
    }

    public int getPagePrevNum(int pageNo, int pageSize) {
        if (pageNo == 0 ){ return 0;}
        return pageNo;
    }
}