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

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoTagMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.Collections;
/**
 * * *The dameng implementation of ConfigInfoTagMapper.
 * @author zhang wenchao
 *  2024/7/24 15:11
 */
public class ConfigInfoTagMapperByDm extends AbstractDmMapper implements ConfigInfoTagMapper {



	@Override
	public MapperResult findAllConfigInfoTagForDumpAllFetchRows(MapperContext context) {
		String sql = findAllConfigInfoTagForDumpAllFetchRows(context.getStartRow(), context.getPageSize());
				/*" SELECT t.id,data_id,group_id,tenant_id,tag_id,app_name,content,md5,gmt_modified "
				+ " FROM (  SELECT id FROM config_info_tag  ORDER BY id LIMIT " + context.getStartRow() + ","
				+ context.getPageSize() + " ) " + "g, config_info_tag t  WHERE g.id = t.id  ";*/
		return new MapperResult(sql, Collections.emptyList());
	}
	@Override
	public String getTableName() {
		return TableConstant.CONFIG_INFO_TAG;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String updateConfigInfo4TagCas() {
		return "UPDATE config_info_tag SET content = ?, md5 = ?, src_ip = ?,src_user = ?,gmt_modified = ?,app_name = ? "
				+ "WHERE data_id = ? AND group_id = ? AND (tenant_id = ? OR tenant_id IS NULL) AND tag_id = ? AND (md5 = ? OR md5 IS NULL OR md5 = '')";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String findAllConfigInfoTagForDumpAllFetchRows(int startRow, int pageSize) {
		return " SELECT t.id,data_id,group_id,tenant_id,tag_id,app_name,content,md5,gmt_modified "
				+ " FROM (  SELECT id FROM config_info_tag WHERE  ROWNUM > " + startRow + " AND ROWNUM <="
				+ (startRow + pageSize) + "ORDER BY id   " + " ) " + "g, config_info_tag t  WHERE g.id = t.id  ";
	}

}
