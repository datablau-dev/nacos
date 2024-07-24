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
import com.alibaba.nacos.plugin.datasource.mapper.HistoryConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
/**
 * @Description: TODO
 * @author zhang wenchao
 * @date 2024/7/24 10:14
 */
public class HistoryConfigInfoMapperByBaseOracle extends BaseOracleAbstractMapper implements HistoryConfigInfoMapper {




	@Override
	public MapperResult removeConfigHistory(MapperContext context) {
		String sql = removeConfigHistorySql();
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.START_TIME),
				context.getWhereParameter(FieldConstant.LIMIT_SIZE)));
	}
	@Override
	public MapperResult pageFindConfigHistoryFetchRows(MapperContext context) {
		String sql = pageFindConfigHistoryFetchRows(context.getStartRow(), context.getPageSize());
			/*	"SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info "
						+ "WHERE data_id = ? AND group_id = ? AND tenant_id = ? ORDER BY nid DESC  OFFSET "
						+ context.getStartRow() + " ROWS FETCH NEXT " + context.getPageSize() + " ROWS ONLY";*/
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.DATA_ID),
				context.getWhereParameter(FieldConstant.GROUP_ID), context.getWhereParameter(FieldConstant.TENANT_ID)));
	}

	@Override
	public String getTableName() {
		return TableConstant.HIS_CONFIG_INFO;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String removeConfigHistorySql() {
		return "DELETE FROM HIS_CONFIG_INFO WHERE gmt_modified < ? AND ROWNUM > ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigHistoryCountByTime() {
		return "SELECT count(*) FROM HIS_CONFIG_INFO WHERE gmt_modified < ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findDeletedConfig() {
		return "SELECT DISTINCT data_id, group_id, tenant_id FROM HIS_CONFIG_INFO WHERE op_type = 'D' AND gmt_modified >= ? AND gmt_modified <= ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigHistoryFetchRows() {
		return "SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM HIS_CONFIG_INFO "
				+ "WHERE data_id = ? AND group_id = ? AND (tenant_id = ? OR tenant_id IS NULL) ORDER BY nid DESC";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String pageFindConfigHistoryFetchRows(int i, int i1) {
		return buildPaginationSql(this.findConfigHistoryFetchRows(),i ,i1);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String detailPreviousConfigHistory() {
		return "SELECT nid,data_id,group_id,tenant_id,app_name,content,md5,src_user,src_ip,op_type,gmt_create,gmt_modified "
				+ "FROM HIS_CONFIG_INFO WHERE nid = (SELECT max(nid) FROM HIS_CONFIG_INFO WHERE id = ?) ";
	}


}
