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
package com.alibaba.nacos.plugin.datasource.impl.postgresql;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.impl.mysql.HistoryConfigInfoMapperByMySql;
import com.alibaba.nacos.plugin.datasource.mapper.HistoryConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
/**
 *
 * @author zhang wenchao
 *  2024/7/24 15:11
 */
public class HistoryConfigInfoMapperByPostgresql extends BasePostgreMapper implements HistoryConfigInfoMapper {

    /**
     *
     * @author zhang wenchao
     *  2024/7/24 15:11
     */
    public String removeConfigHistorySql() {
        String sql = "WITH temp_table as (SELECT id FROM his_config_info WHERE gmt_modified < ? LIMIT ? ) " +
                "DELETE FROM his_config_info WHERE id in (SELECT id FROM temp_table) ";
        return sql;
    }
    @Override
    public MapperResult removeConfigHistory(MapperContext context) {
        String sql = removeConfigHistorySql();
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.START_TIME),
                context.getWhereParameter(FieldConstant.LIMIT_SIZE)));
    }

    public MapperResult pageFindConfigHistoryFetchRows(MapperContext context) {
        String sql =getLimitPageSql(
                "SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info "
                        + "WHERE data_id = ? AND group_id = ? AND tenant_id = ? ORDER BY nid DESC   "
                        , context.getStartRow() , context.getPageSize());
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.DATA_ID),
                context.getWhereParameter(FieldConstant.GROUP_ID), context.getWhereParameter(FieldConstant.TENANT_ID)));
    }

    @Override
    public String getTableName() {
        return TableConstant.HIS_CONFIG_INFO;
    }

}
