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

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
/**
 * The dameng implementation of GroupCapacityMapper.
 * @author zhang wenchao
 *  2024/7/24 15:11
 */
public class GroupCapacityMapperByDm extends AbstractDmMapper implements GroupCapacityMapper {




	@Override
	public MapperResult selectGroupInfoBySize(MapperContext context) {
		String sql = selectGroupInfoBySize();
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID), context.getPageSize()));
	}

	@Override
	public String getTableName() {
		return TableConstant.GROUP_CAPACITY;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String insertIntoSelect() {
		return "INSERT INTO group_capacity (group_id, quota, `usage`, `max_size`, max_aggr_count, max_aggr_size,gmt_create,"
				+ " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String insertIntoSelectByWhere() {
		return "INSERT INTO group_capacity (group_id, quota,`usage`, `max_size`, max_aggr_count, max_aggr_size, gmt_create,"
				+ " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE group_id=? AND tenant_id = ''";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String incrementUsageByWhereQuotaEqualZero() {
		return "UPDATE group_capacity SET `usage` = `usage` + 1, gmt_modified = ? WHERE group_id = ? AND `usage` < ? AND quota = 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String incrementUsageByWhereQuotaNotEqualZero() {
		return "UPDATE group_capacity SET `usage` = `usage` + 1, gmt_modified = ? WHERE group_id = ? AND `usage` < quota AND quota != 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String incrementUsageByWhere() {
		return "UPDATE group_capacity SET `usage` = `usage` + 1, gmt_modified = ? WHERE group_id = ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String decrementUsageByWhere() {
		return "UPDATE group_capacity SET `usage` = `usage` - 1, gmt_modified = ? WHERE group_id = ? AND `usage` > 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String updateUsage() {
		return "UPDATE group_capacity SET `usage` = (SELECT count(*) FROM config_info), gmt_modified = ? WHERE group_id = ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String updateUsageByWhere() {
		return "UPDATE group_capacity SET `usage` = (SELECT count(*) FROM config_info WHERE group_id=? AND tenant_id = ''),"
				+ " gmt_modified = ? WHERE group_id= ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String selectGroupInfoBySize() {
		return "SELECT id, group_id FROM group_capacity WHERE id > ? ROWNUM > ?";
	}


}
