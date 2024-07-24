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
import com.alibaba.nacos.plugin.datasource.mapper.TenantCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
/**
 * The Oracle implementation of TenantCapacityMapperByOracle
 *
 * @author zhang wenchao
 *
 */
public class TenantCapacityMapperByBaseOracle extends BaseOracleAbstractMapper implements TenantCapacityMapper {




	@Override
	public MapperResult getCapacityList4CorrectUsage(MapperContext context) {
		String sql = getCapacityList4CorrectUsageSql();
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID),
				context.getWhereParameter(FieldConstant.LIMIT_SIZE)));
	}
	@Override
	public String getTableName() {
		return TableConstant.TENANT_CAPACITY;
	}


	/**
	 *
	 *
	 * @author zhang wenchao
	 *
	 */
	public String incrementUsageWithDefaultQuotaLimit() {
		return "UPDATE TENANT_CAPACITY SET `usage` = `usage` + 1, gmt_modified = ? WHERE ((tenant_id = ? OR tenant_id IS NULL) OR tenant_id IS NULL) AND `usage` <"
				+ " ? AND quota = 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *
	 */
	public String incrementUsageWithQuotaLimit() {
		return "UPDATE TENANT_CAPACITY SET `usage` = `usage` + 1, gmt_modified = ? WHERE (tenant_id = ? OR tenant_id IS NULL) AND `usage` < "
				+ "quota AND quota != 0";
	}

	/**
	 *
	 * @author zhang wenchao
	 *
	 */
	public String incrementUsage() {
		return "UPDATE TENANT_CAPACITY SET `usage` = `usage` + 1, gmt_modified = ? WHERE (tenant_id = ? OR tenant_id IS NULL)";
	}

	/**
	 *
	 * @author zhang wenchao
	 *
	 */
	public String decrementUsage() {
		return "UPDATE TENANT_CAPACITY SET `usage` = `usage` - 1, gmt_modified = ? WHERE (tenant_id = ? OR tenant_id IS NULL) AND `usage` > 0";
	}

	/**
	java.lang.String
	 * @author zhang wenchao
	 *
	 *  2024/7/24 10:18
	 */
	public String correctUsage() {
		return "UPDATE TENANT_CAPACITY SET `usage` = (SELECT count(*) FROM config_info WHERE (tenant_id = ? OR tenant_id IS NULL)), "
				+ "gmt_modified = ? WHERE (tenant_id = ? OR tenant_id IS NULL)";
	}

	/**
	java.lang.String
	 * @author zhang wenchao
	 *
	 *  2024/7/24 10:18
	 */
	public String getCapacityList4CorrectUsageSql() {
		return "SELECT id, tenant_id FROM TENANT_CAPACITY WHERE id> AND  ROWNUM > ?";
	}

	/**
	java.lang.String
	 * @author zhang wenchao
	 *
	 *  2024/7/24 10:18
	 */
	public String insertTenantCapacity() {
		return "INSERT INTO TENANT_CAPACITY (tenant_id, quota, `usage`, `max_size`, max_aggr_count, max_aggr_size, "
				+ "gmt_create, gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE tenant_id=? OR tenant_id IS NULL;";
	}


}
