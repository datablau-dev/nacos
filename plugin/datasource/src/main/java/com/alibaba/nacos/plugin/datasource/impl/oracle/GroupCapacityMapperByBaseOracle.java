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
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
/**
 * @Description: TODO
 * @author zhang wenchao
 * @date 2024/7/24 10:13
 */
public class GroupCapacityMapperByBaseOracle extends BaseOracleAbstractMapper implements GroupCapacityMapper {


	@Override
	public MapperResult selectGroupInfoBySize(MapperContext context) {
		String sql = "SELECT id, group_id FROM group_capacity WHERE id > ?";
		return new MapperResult(buildPaginationSql(sql,0,context.getPageSize()),
				CollectionUtils.list(context.getWhereParameter(FieldConstant.ID), context.getPageSize()));
	}

	@Override
	public String getTableName() {
		return TableConstant.GROUP_CAPACITY;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String insertIntoSelect() {
		return "INSERT INTO GROUP_CAPACITY (group_id, quota, `usage`, `max_size`, max_aggr_count, max_aggr_size,gmt_create,"
				+ " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM CONFIG_INFO";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String insertIntoSelectByWhere() {
		return "INSERT INTO GROUP_CAPACITY (group_id, quota,`usage`, `max_size`, max_aggr_count, max_aggr_size, gmt_create,"
				+ " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM CONFIG_INFO WHERE group_id=? AND tenant_id = ''";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String incrementUsageByWhereQuotaEqualZero() {
		return "UPDATE GROUP_CAPACITY SET `usage` = `usage` + 1, gmt_modified = ? WHERE group_id = ? AND `usage` < ? AND quota = 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String incrementUsageByWhereQuotaNotEqualZero() {
		return "UPDATE GROUP_CAPACITY SET `usage` = `usage` + 1, gmt_modified = ? WHERE group_id = ? AND `usage` < quota AND quota != 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String incrementUsageByWhere() {
		return "UPDATE GROUP_CAPACITY SET `usage` = `usage` + 1, gmt_modified = ? WHERE group_id = ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String decrementUsageByWhere() {
		return "UPDATE GROUP_CAPACITY SET `usage` = `usage` - 1, gmt_modified = ? WHERE group_id = ? AND `usage` > 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String updateUsage() {
		return "UPDATE GROUP_CAPACITY SET `usage` = (SELECT count(*) FROM CONFIG_INFO), gmt_modified = ? WHERE group_id = ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String updateUsageByWhere() {
		return "UPDATE GROUP_CAPACITY SET `usage` = (SELECT count(*) FROM CONFIG_INFO WHERE group_id=? AND tenant_id = ''),"
				+ " gmt_modified = ? WHERE group_id= ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String selectGroupInfoBySize() {
		return "SELECT id, group_id FROM GROUP_CAPACITY WHERE id > ? ROWNUM > ?";
	}


}
