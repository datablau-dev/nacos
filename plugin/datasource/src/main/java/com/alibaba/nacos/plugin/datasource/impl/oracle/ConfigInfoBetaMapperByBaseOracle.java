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

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoBetaMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;
/**
 * @Description: TODO
 * @author zhang wenchao
 * @date 2024/7/24 10:13
 */
public class ConfigInfoBetaMapperByBaseOracle extends BaseOracleAbstractMapper implements ConfigInfoBetaMapper {


	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	@Override
	public MapperResult findAllConfigInfoBetaForDumpAllFetchRows(MapperContext context) {
		Integer startRow = context.getStartRow();
		int pageSize = context.getPageSize();

		String sql = findAllConfigInfoBetaForDumpAllFetchRows(context.getStartRow(),context.getPageSize());
		List<Object> paramList = new ArrayList<>();
		paramList.add(startRow);
		paramList.add(pageSize);

		return new MapperResult(sql, paramList);
	}
	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	@Override
	public String getTableName() {
		return TableConstant.CONFIG_INFO_BETA;
	}
	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String updateConfigInfo4BetaCas() {
		return "UPDATE CONFIG_INFO_BETA SET content = ?,md5 = ?,beta_ips = ?,src_ip = ?,src_user = ?,gmt_modified = ?,app_name = ? "
				+ "WHERE data_id = ? AND group_id = ? AND (tenant_id = ? OR tenant_id IS NULL) AND (md5 = ? or md5 is null or md5 = '')";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllConfigInfoBetaForDumpAllFetchRows(int startRow, int pageSize) {
		return " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,beta_ips "
				+ " FROM ( SELECT rownum ROW_ID,id FROM CONFIG_INFO_BETA  WHERE  ROW_ID<=  " + (startRow + pageSize)
				+ " ORDER BY id )" + " g, CONFIG_INFO_BETA t WHERE g.id = t.id AND g.ROW_ID >" + startRow;
	}


}
