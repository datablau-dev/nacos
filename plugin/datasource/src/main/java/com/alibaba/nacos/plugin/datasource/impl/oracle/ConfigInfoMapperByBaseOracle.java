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
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.ContextConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
/**
 *
 * @author zhang wenchao
 *  2024/7/24 10:14
 */
public class ConfigInfoMapperByBaseOracle extends BaseOracleAbstractMapper implements ConfigInfoMapper {

	private static final String DATA_ID = "dataId";

	private static final String GROUP = "group";

	private static final String APP_NAME = "appName";

	private static final String CONTENT = "content";

	private static final String TENANT = "tenant";


	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	@Override
	public MapperResult findConfigInfoByAppFetchRows(MapperContext context) {
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);

		String sql = findConfigInfoByAppFetchRows(context.getStartRow(), context.getPageSize());
	/*			"SELECT ID,data_id,group_id,tenant_id,app_name,content FROM config_info WHERE tenant_id LIKE ? AND "
						+ "app_name = ?" + " OFFSET " + context.getStartRow() + " ROWS FETCH NEXT "
						+ context.getPageSize() + " ROWS ONLY";*/

		return new MapperResult(sql, CollectionUtils.list(tenantId, appName));
	}


	@Override
	public MapperResult getTenantIdList(MapperContext context) {
		String sql = getTenantIdList(context.getStartRow(), context.getPageSize());
		return new MapperResult( sql, Collections.emptyList());
			/*	"SELECT tenant_id FROM config_info WHERE tenant_id != '" + NamespaceUtil.getNamespaceDefaultId()
						+ "' GROUP BY tenant_id OFFSET " + context.getStartRow() + " ROWS FETCH NEXT "
						+ context.getPageSize() + " ROWS ONLY", Collections.emptyList());*/
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	@Override
	public MapperResult getGroupIdList(MapperContext context) {
		String sql = getGroupIdList(context.getStartRow(), context.getPageSize());
		return new MapperResult( sql, Collections.emptyList());
			/*	"SELECT group_id FROM config_info WHERE tenant_id ='" + NamespaceUtil.getNamespaceDefaultId()
						+ "' GROUP BY group_id OFFSET " + context.getStartRow() + " ROWS FETCH NEXT "
						+ context.getPageSize() + " ROWS ONLY", Collections.emptyList());*/
	}
	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	@Override
	public MapperResult findAllConfigKey(MapperContext context) {
		String inSql = buildPaginationSql("SELECT id FROM config_info WHERE tenant_id LIKE ? ORDER BY id "
				, context.getStartRow() ,context.getPageSize());

		//String sql = " SELECT data_id,group_id,app_name FROM CONFIG_INFO WHERE (tenant_id LIKE ? OR tenant_id IS NULL)  ORDER BY id ";

		String sql = " SELECT data_id,group_id,app_name FROM "
				+ " (" + inSql + " ) "
				+ "g, config_info t  WHERE g.id = t.id ";
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.TENANT_ID)));
	}

	@Override
	public MapperResult findAllConfigInfoBaseFetchRows(MapperContext context) {

		return new MapperResult(findAllConfigInfoBaseFetchRows(context.getStartRow(),context.getPageSize())
				, Collections.emptyList());
	}


	@Override
	public MapperResult findAllConfigInfoFragment(MapperContext context) {
		String contextParameter = context.getContextParameter(ContextConstant.NEED_CONTENT);
		boolean needContent = contextParameter != null && Boolean.parseBoolean(contextParameter);
		String sql = findAllConfigInfoFragment(needContent,context.getStartRow(), context.getPageSize());

		return new MapperResult(sql,
				CollectionUtils.list(context.getWhereParameter(FieldConstant.ID)));
	}

	@Override
	public MapperResult findChangeConfigFetchRows(MapperContext context) {
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);

		final Timestamp startTime = (Timestamp) context.getWhereParameter(FieldConstant.START_TIME);
		final Timestamp endTime = (Timestamp) context.getWhereParameter(FieldConstant.END_TIME);

		List<Object> paramList = new ArrayList<>();

		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_modified FROM"
				+ " config_info WHERE ";
		String where = " 1=1 ";

		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
			paramList.add(group);
		}

		if (!StringUtils.isBlank(tenant)) {
			where += " AND tenant_id = ? ";
			paramList.add(tenant);
		}

		if (!StringUtils.isBlank(appName)) {
			where += " AND app_name = ? ";
			paramList.add(appName);
		}
		if (startTime != null) {
			where += " AND gmt_modified >=? ";
			paramList.add(startTime);
		}
		if (endTime != null) {
			where += " AND gmt_modified <=? ";
			paramList.add(endTime);
		}
		String sql = sqlFetchRows + where + " AND id > " +  context.getWhereParameter(FieldConstant.LAST_MAX_ID) + " ORDER BY id ASC";


		return new MapperResult( buildPaginationSql(sql, 0, context.getPageSize()), paramList);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:26
	 */
	@Override
	public MapperResult listGroupKeyMd5ByPageFetchRows(MapperContext context) {

		String inSql = buildPaginationSql("SELECT id FROM config_info ORDER BY id", context.getStartRow(), context.getPageSize());
		return new MapperResult(" SELECT t.id,data_id,group_id,tenant_id,app_name,type,md5,gmt_modified "
				+ "FROM (" + inSql + ") g, config_info t WHERE g.id = t.id", Collections.emptyList());
	}

	@Override
	public MapperResult findConfigInfoBaseLikeFetchRows(MapperContext context) {
		final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);

		List<Object> paramList = new ArrayList<>();
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,content FROM config_info WHERE ";
		String where = " 1=1 AND tenant_id='" + NamespaceUtil.getNamespaceDefaultId() + "' ";
		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
			paramList.add(group);
		}
		if (!StringUtils.isBlank(tenant)) {
			where += " AND content LIKE ? ";
			paramList.add(tenant);
		}
		String sql = sqlFetchRows + where;
		//return buildPaginationSql(sql, startRow, pageSize);
		return new MapperResult(buildPaginationSql(sql, context.getStartRow(), context.getPageSize()), paramList);
	}


	@Override
	public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

		List<Object> paramList = new ArrayList<>();

		final String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" tenant_id=? ");
		paramList.add(tenantId);
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
			paramList.add(dataId);
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
			paramList.add(group);
		}

		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
			paramList.add(content);
		}
		return new MapperResult(buildPaginationSql(sql + where,context.getStartRow(),context.getPageSize()),
			 paramList);
	}

	@Override
	public MapperResult findConfigInfoBaseByGroupFetchRows(MapperContext context) {
		return new MapperResult(findConfigInfoBaseByGroupFetchRows(context.getStartRow(), context.getPageSize()),
				CollectionUtils.list(context.getWhereParameter(FieldConstant.GROUP_ID),
						context.getWhereParameter(FieldConstant.TENANT_ID)));
	}

	@Override
	public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {

		final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
		final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
		final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
		final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
		final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

		List<Object> paramList = new ArrayList<>();

		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,encrypted_data_key FROM config_info";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" tenant_id LIKE ? ");
		paramList.add(tenantId);
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
			paramList.add(dataId);
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
			paramList.add(group);
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
			paramList.add(appName);
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
			paramList.add(content);
		}


		String sql = buildPaginationSql(sqlFetchRows + where, context.getStartRow(), context.getPageSize());
		return new MapperResult(sql, paramList);
	}


	@Override
	public MapperResult findAllConfigInfoFetchRows(MapperContext context) {
	String inSql = buildPaginationSql("SELECT id FROM config_info  WHERE tenant_id LIKE ? ORDER BY id"
			,context.getStartRow(),context.getPageSize());
		return new MapperResult(" SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5 "
				+ " FROM ( "+ inSql + " )"
				+ " g, config_info t  WHERE g.id = t.id ",
				CollectionUtils.list(context.getWhereParameter(FieldConstant.TENANT_ID), context.getStartRow(),
						context.getPageSize()));
	}


	@Override
	public String getTableName() {
		return TableConstant.CONFIG_INFO;
	}

	@Override
	public MapperResult findConfigMaxId(MapperContext context) {

		return new MapperResult("SELECT MAX(id) FROM CONFIG_INFO", Collections.emptyList());
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigMaxId() {
		return "SELECT MAX(id) FROM CONFIG_INFO";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllDataIdAndGroup() {
		return "SELECT DISTINCT data_id, group_id FROM CONFIG_INFO";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoByAppCountRows() {
		return "SELECT count(*) FROM CONFIG_INFO WHERE (tenant_id LIKE ? OR tenant_id IS NULL) AND app_name= ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoByAppFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content FROM CONFIG_INFO"
				+ " WHERE (tenant_id LIKE ? OR tenant_id IS NULL) AND app_name= ?";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String configInfoLikeTenantCount() {
		return "SELECT count(*) FROM CONFIG_INFO WHERE (tenant_id LIKE ? OR tenant_id IS NULL)";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String getTenantIdList(int startRow, int pageSize) {
		String sql = "SELECT tenant_id FROM CONFIG_INFO WHERE tenant_id IS NOT NULL GROUP BY tenant_id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String getGroupIdList(int startRow, int pageSize) {
		String sql = "SELECT group_id FROM CONFIG_INFO WHERE tenant_id IS NULL GROUP BY group_id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllConfigKey(int startRow, int pageSize) {
		String sql = " SELECT id,data_id,group_id,app_name FROM CONFIG_INFO WHERE (tenant_id LIKE ? OR tenant_id IS NULL)  ORDER BY id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllConfigInfoBaseFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,content,md5 FROM  CONFIG_INFO  ORDER BY id  ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllConfigInfoFragment(boolean needContent,int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name," + (needContent ? "content" : "")
				+ ",md5,gmt_modified,type "
				+ "FROM CONFIG_INFO WHERE id > ? ORDER BY id ASC ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findChangeConfig() {
		return "SELECT data_id, group_id, tenant_id, app_name, content, gmt_modified,encrypted_data_key "
				+ "FROM CONFIG_INFO WHERE gmt_modified >= ? AND gmt_modified <= ?";
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findChangeConfigCountRows(Map<String, String> params, final Timestamp startTime,
			final Timestamp endTime) {
		final String tenant = params.get(TENANT);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String appName = params.get(APP_NAME);
		final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
		final String sqlCountRows = "SELECT count(*) FROM CONFIG_INFO WHERE ";
		String where = " 1=1 ";
		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
		}

		if (!StringUtils.isBlank(tenantTmp)) {
			where += " AND (tenant_id = ? OR tenant_id IS NULL) ";
		}

		if (!StringUtils.isBlank(appName)) {
			where += " AND app_name = ? ";
		}
		if (startTime != null) {
			where += " AND gmt_modified >=? ";
		}
		if (endTime != null) {
			where += " AND gmt_modified <=? ";
		}
		return sqlCountRows + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findChangeConfigFetchRows(Map<String, String> params, final Timestamp startTime,
			final Timestamp endTime, int startRow, int pageSize, long lastMaxId) {
		final String tenant = params.get(TENANT);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String appName = params.get(APP_NAME);
		final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_modified FROM CONFIG_INFO WHERE ";
		String where = " 1=1 ";
		if (!StringUtils.isBlank(dataId)) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(group)) {
			where += " AND group_id LIKE ? ";
		}

		if (!StringUtils.isBlank(tenantTmp)) {
			where += " AND (tenant_id = ? OR tenant_id IS NULL) ";
		}

		if (!StringUtils.isBlank(appName)) {
			where += " AND app_name = ? ";
		}
		if (startTime != null) {
			where += " AND gmt_modified >=? ";
		}
		if (endTime != null) {
			where += " AND gmt_modified <=? ";
		}

		String sql = sqlFetchRows + where + " AND id > " + lastMaxId + " ORDER BY id ASC";
		return buildPaginationSql(sql, 0, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String listGroupKeyMd5ByPageFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,md5,type,gmt_modified,encrypted_data_key  CONFIG_INFO  ORDER BY id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllConfigInfo4Export(List<Long> ids, Map<String, String> params) {
		String tenant = params.get("tenant");
		String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_create,gmt_modified,src_user,src_ip,"
				+ "c_desc,c_use,effect,c_schema,encrypted_data_key FROM CONFIG_INFO";
		StringBuilder where = new StringBuilder(" WHERE ");
		List<Object> paramList = new ArrayList<>();
		if (!CollectionUtils.isEmpty(ids)) {
			where.append(" id IN (");
			for (int i = 0; i < ids.size(); i++) {
				if (i != 0) {
					where.append(", ");
				}
				where.append('?');
				paramList.add(ids.get(i));
			}
			where.append(") ");
		}
		else {
			where.append(" (tenant_id= ? OR tenant_id IS NULL)");
			paramList.add(tenantTmp);
			if (!StringUtils.isBlank(params.get(DATA_ID))) {
				where.append(" AND data_id LIKE ? ");
			}
			if (StringUtils.isNotBlank(params.get(GROUP))) {
				where.append(" AND group_id= ? ");
			}
			if (StringUtils.isNotBlank(params.get(APP_NAME))) {
				where.append(" AND app_name= ? ");
			}
		}
		return sql + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoBaseLikeCountRows(Map<String, String> params) {
		final String sqlCountRows = "SELECT count(*) FROM CONFIG_INFO WHERE ";
		String where = " 1=1 AND (tenant_id='' OR tenant_id IS NULL) ";

		if (!StringUtils.isBlank(params.get(DATA_ID))) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(params.get(GROUP))) {
			where += " AND group_id LIKE ";
		}
		if (!StringUtils.isBlank(params.get(CONTENT))) {
			where += " AND content LIKE ? ";
		}
		return sqlCountRows + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoBaseLikeFetchRows(Map<String, String> params, int startRow, int pageSize) {
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,content FROM CONFIG_INFO WHERE ";
		String where = " 1=1 AND (tenant_id='' OR tenant_id IS NULL) ";
		if (!StringUtils.isBlank(params.get(DATA_ID))) {
			where += " AND data_id LIKE ? ";
		}
		if (!StringUtils.isBlank(params.get(GROUP))) {
			where += " AND group_id LIKE ";
		}
		if (!StringUtils.isBlank(params.get(CONTENT))) {
			where += " AND content LIKE ? ";
		}
		String sql = sqlFetchRows + where;
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfo4PageCountRows(Map<String, String> params) {
		final String appName = params.get(APP_NAME);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String sqlCount = "SELECT count(*) FROM CONFIG_INFO";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" ( tenant_id= ?  or tenant_id is NULL ）");
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
		}
		return sqlCount + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfo4PageFetchRows(Map<String, String> params, int startRow, int pageSize) {
		final String appName = params.get(APP_NAME);
		final String dataId = params.get(DATA_ID);
		final String group = params.get(GROUP);
		final String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,encrypted_data_key FROM CONFIG_INFO";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" ( tenant_id= ?  or tenant_id is NULL ） ");
		if (StringUtils.isNotBlank(dataId)) {
			where.append(" AND data_id=? ");
		}
		if (StringUtils.isNotBlank(group)) {
			where.append(" AND group_id=? ");
		}
		if (StringUtils.isNotBlank(appName)) {
			where.append(" AND app_name=? ");
		}
		return buildPaginationSql(sql + where, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoBaseByGroupFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,content FROM CONFIG_INFO WHERE group_id=? AND ( tenant_id= ?  or tenant_id is NULL ）";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoLike4PageCountRows(Map<String, String> params) {
		String dataId = params.get(DATA_ID);
		String group = params.get(GROUP);
		final String appName = params.get(APP_NAME);
		final String content = params.get(CONTENT);
		final String sqlCountRows = "SELECT count(*) FROM CONFIG_INFO";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" (tenant_id LIKE ? OR tenant_id IS NULL) ");
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
		}
		return sqlCountRows + where;
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfoLike4PageFetchRows(Map<String, String> params, int startRow, int pageSize) {
		String dataId = params.get(DATA_ID);
		String group = params.get(GROUP);
		final String appName = params.get(APP_NAME);
		final String content = params.get(CONTENT);
		final String sqlFetchRows = "SELECT id,data_id,group_id,tenant_id,app_name,content,encrypted_data_key FROM CONFIG_INFO";
		StringBuilder where = new StringBuilder(" WHERE ");
		where.append(" (tenant_id LIKE ? OR tenant_id IS NULL) ");
		if (!StringUtils.isBlank(dataId)) {
			where.append(" AND data_id LIKE ? ");
		}
		if (!StringUtils.isBlank(group)) {
			where.append(" AND group_id LIKE ? ");
		}
		if (!StringUtils.isBlank(appName)) {
			where.append(" AND app_name = ? ");
		}
		if (!StringUtils.isBlank(content)) {
			where.append(" AND content LIKE ? ");
		}
		return buildPaginationSql(sqlFetchRows + where, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findAllConfigInfoFetchRows(int startRow, int pageSize) {
		String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,md5 "
				+ " FROM  CONFIG_INFO WHERE (tenant_id LIKE ? OR tenant_id IS NULL) ORDER BY id ";
		return buildPaginationSql(sql, startRow, pageSize);
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String findConfigInfosByIds(int idSize) {
		StringBuilder sql = new StringBuilder(
				"SELECT ID,data_id,group_id,tenant_id,app_name,content,md5 FROM CONFIG_INFO WHERE ");
		sql.append("id IN (");
		for (int i = 0; i < idSize; i++) {
			if (i != 0) {
				sql.append(", ");
			}
			sql.append('?');
		}
		sql.append(") ");
		return sql.toString();
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String removeConfigInfoByIdsAtomic(int size) {
		StringBuilder sql = new StringBuilder("DELETE FROM CONFIG_INFO WHERE ");
		sql.append("id IN (");
		for (int i = 0; i < size; i++) {
			if (i != 0) {
				sql.append(", ");
			}
			sql.append('?');
		}
		sql.append(") ");
		return sql.toString();
	}

	/**
	 *
	 * @author zhang wenchao
	 *  2024/7/24 10:14
	 */
	public String updateConfigInfoAtomicCas() {
		return "UPDATE CONFIG_INFO SET "
				+ "content=?, md5 = ?, src_ip=?,src_user=?,gmt_modified=?, app_name=?,c_desc=?,c_use=?,effect=?,type=?,c_schema=? "
				+ "WHERE data_id=? AND group_id=? AND (tenant_id=? OR tenant_id IS NULL) AND (md5=? OR md5 IS NULL OR md5='')";
	}

}
