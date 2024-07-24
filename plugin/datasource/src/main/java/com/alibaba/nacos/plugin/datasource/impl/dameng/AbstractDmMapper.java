/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.plugin.datasource.impl.dameng;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.enums.dameng.TrustedDamengFunctionEnum;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;
import java.util.List;

/**
 * The abstract dameng mapper contains CRUD methods.
 * @author zhang wenchao
 *  2024/7/24 15:11
 */
public abstract class AbstractDmMapper extends AbstractMapper {


	@Override
	public String getFunction(String functionName) {
		return TrustedDamengFunctionEnum.getFunctionByName(functionName);
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

	public String buildPaginationSql(String originalSql, int startRow, int pageSize) {
		return "SELECT * FROM ( SELECT TMP2.* FROM (SELECT TMP.*, ROWNUM ROW_ID FROM ( " + originalSql
				+ " ) TMP) TMP2 WHERE ROWNUM <=" + (startRow + pageSize) + ") WHERE ROW_ID > " + startRow;
	}

	/**
	java.lang.String
	 * @author zhang wenchao
	 *  2024/7/24 15:28
	 * @return  string table name
	 */
	@Override
	public abstract String getTableName();

	@Override
	public  String getDataSource() {
		return DataSourceConstant.DM;
	}

}
