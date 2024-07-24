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

package com.alibaba.nacos.plugin.datasource.impl.oracle;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoAggrMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.List;

/**
 *  The Oracle implementation of TenantInfoMapper
 *  
 * @author zhang wenchao
 * 
 */
public class ConfigInfoAggrMapperByBaseOracle extends BaseOracleAbstractMapper implements ConfigInfoAggrMapper {



	@Override
	public MapperResult findConfigInfoAggrByPageFetchRows(MapperContext context) {
		final Integer startRow = context.getStartRow();
		final Integer pageSize = context.getPageSize();
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String groupId = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);

		String sql = findConfigInfoAggrByPageFetchRowsSql(startRow, pageSize);
		List<Object> paramList = CollectionUtils.list(dataId, groupId, tenantId);
		return new MapperResult(sql, paramList);
	}

	@Override
	public String getTableName() {
		return TableConstant.CONFIG_INFO_AGGR;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String aggrConfigInfoCount(int size, boolean isIn) {
		StringBuilder sql = new StringBuilder(
				"SELECT count(*) FROM CONFIG_INFO_AGGR WHERE data_id = ? AND group_id = ? AND (tenant_id = ? OR tenant_id IS NULL) AND datum_id");
		if (isIn) {
			sql.append(" IN (");
		}
		else {
			sql.append(" NOT IN (");
		}
		for (int i = 0; i < size; i++) {
			if (i > 0) {
				sql.append(", ");
			}
			sql.append('?');
		}
		sql.append(')');

		return sql.toString();
	}


	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoAggrIsOrdered() {
		return "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM "
				+ "CONFIG_INFO_AGGR WHERE data_id = ? AND group_id = ? AND (tenant_id = ? OR tenant_id IS NULL) ORDER BY datum_id";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoAggrByPageFetchRowsSql(int startRow, int pageSize) {
		String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM CONFIG_INFO_AGGR WHERE data_id= ? AND "
				+ "group_id= ? AND (tenant_id= ? OR tenant_id IS NULL) ORDER BY datum_id";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllAggrGroupByDistinct() {
		return "SELECT DISTINCT data_id, group_id, tenant_id FROM CONFIG_INFO_AGGR";
	}

}
