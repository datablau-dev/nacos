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

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigTagsRelationMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * The dameng implementation of ConfigTagsRelationMapper
 * @author zhang wenchao
 *  2024/7/24 15:11
 */
public class ConfigTagsRelationMapperByDm extends AbstractDmMapper implements ConfigTagsRelationMapper {



	@Override
	public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
		final String[] tagArr = (String[]) context.getWhereParameter(FieldConstant.TAG_ARR);

		List<Object> paramList = new ArrayList<>();
		StringBuilder where = new StringBuilder(" WHERE ");
		final String sql =
				"SELECT a.id,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content FROM config_info  a LEFT JOIN "
						+ "config_tags_relation b ON a.id=b.id";

		where.append(" a.tenant_id=? ");
		paramList.add(tenant);

		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND a.data_id=? ");
			paramList.add(dataId);
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND a.group_id=? ");
			paramList.add(group);
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND a.app_name=? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND a.content LIKE ? ");
			paramList.add(content);
		}
		where.append(" AND b.tag_name IN (");
		for (int i = 0; i < tagArr.length; i++) {
			if (i != 0) {
				where.append(", ");
			}
			where.append('?');
			paramList.add(tagArr[i]);
		}
		where.append(") ");
		return new MapperResult(sql + where + " AND  ROWNUM > " + sql + " AND ROWNUM <= " + (context.getStartRow() + context.getPageSize()),
				paramList);
	}


	@Override
	public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
		final String[] tagArr = (String[]) context.getWhereParameter(FieldConstant.TAG_ARR);

		List<Object> paramList = new ArrayList<>();

		StringBuilder where = new StringBuilder(" WHERE ");
		final String sqlFetchRows = "SELECT a.id,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content "
				+ "FROM config_info a LEFT JOIN config_tags_relation b ON a.id=b.id ";

		where.append(" a.tenant_id LIKE ? ");
		paramList.add(tenant);
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND a.data_id LIKE ? ");
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND a.group_id LIKE ? ");
			paramList.add(group);
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND a.app_name = ? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND a.content LIKE ? ");
			paramList.add(content);
		}

		where.append(" AND b.tag_name IN (");
		for (int i = 0; i < tagArr.length; i++) {
			if (i != 0) {
				where.append(", ");
			}
			where.append('?');
			paramList.add(tagArr[i]);
		}
		where.append(") ");
		String sql = sqlFetchRows + where + " AND ROWNUM > " + context.getStartRow() + " AND ROWNUM <= " + (context.getStartRow() + context.getPageSize());

		return new MapperResult(sql,paramList);
	}

	@Override
	public String getTableName() {
		return TableConstant.CONFIG_TAGS_RELATION;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String findConfigInfo4PageCountRows(final Map<String, String> params, final int tagSize) {
		final String appName = params.get("appName");
		final String dataId = params.get("dataId");
		final String group = params.get("group");
		StringBuilder where = new StringBuilder(" WHERE ");
		final String sqlCount = "SELECT count(*) FROM config_info  a LEFT JOIN config_tags_relation b ON a.id=b.id";
		where.append(" (a.tenant_id=? OR a.tenant_id IS NULL)");
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND a.data_id=? ");
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND a.group_id=? ");
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND a.app_name=? ");
		}
		where.append(" AND b.tag_name IN (");
		for (int i = 0; i < tagSize; i++) {
			if (i != 0) {
				where.append(", ");
			}
			where.append('?');
		}
		where.append(") ");
		return sqlCount + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String findConfigInfo4PageFetchRows(Map<String, String> params, int tagSize, int startRow, int pageSize) {
		final String appName = params.get("appName");
		final String dataId = params.get("dataId");
		final String group = params.get("group");
		StringBuilder where = new StringBuilder(" WHERE ");
		final String sql = "SELECT a.id,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content FROM config_info  a LEFT JOIN "
				+ "config_tags_relation b ON a.id=b.id";

		where.append("( a.tenant_id=? OR tenant_id IS NULL)");

		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND a.data_id=? ");
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND a.group_id=? ");
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND a.app_name=? ");
		}

		where.append(" AND b.tag_name IN (");
		for (int i = 0; i < tagSize; i++) {
			if (i != 0) {
				where.append(", ");
			}
			where.append('?');
		}
		where.append(") ");
		return sql + where + " AND  ROWNUM > " + sql + " AND ROWNUM <= " + (startRow + pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String findConfigInfoLike4PageCountRows(final Map<String, String> params, int tagSize) {
		final String appName = params.get("appName");
		final String content = params.get("content");
		final String dataId = params.get("dataId");
		final String group = params.get("group");
		StringBuilder where = new StringBuilder(" WHERE ");
		final String sqlCountRows = "SELECT count(*) FROM config_info  a LEFT JOIN config_tags_relation b ON a.id=b.id ";

		where.append(" a.(tenant_id LIKE ? OR tenant_id IS NULL) ");
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND a.data_id LIKE ? ");
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND a.group_id LIKE ? ");
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND a.app_name = ? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND a.content LIKE ? ");
		}

		where.append(" AND b.tag_name IN (");
		for (int i = 0; i < tagSize; i++) {
			if (i != 0) {
				where.append(", ");
			}
			where.append('?');
		}
		where.append(") ");
		return sqlCountRows + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 15:11
	 */
	public String findConfigInfoLike4PageFetchRows(final Map<String, String> params, int tagSize, int startRow,
			int pageSize) {
		final String appName = params.get("appName");
		final String content = params.get("content");
		final String dataId = params.get("dataId");
		final String group = params.get("group");
		StringBuilder where = new StringBuilder(" WHERE ");
		final String sqlFetchRows = "SELECT a.id,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content "
				+ "FROM config_info a LEFT JOIN config_tags_relation b ON a.id=b.id ";

		where.append(" a.(tenant_id LIKE ? OR tenant_id IS NULL) ");
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND a.data_id LIKE ? ");
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND a.group_id LIKE ? ");
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND a.app_name = ? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND a.content LIKE ? ");
		}

		where.append(" AND b.tag_name IN (");
		for (int i = 0; i < tagSize; i++) {
			if (i != 0) {
				where.append(", ");
			}
			where.append('?');
		}
		where.append(") ");
		return sqlFetchRows + where + " AND ROWNUM > " + startRow + " AND ROWNUM <= " + (startRow + pageSize);
	}


}
