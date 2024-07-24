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

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.dialect.DatabaseDialect;
import com.alibaba.nacos.plugin.datasource.impl.mysql.ConfigInfoBetaMapperByMySql;
import com.alibaba.nacos.plugin.datasource.manager.DatabaseDialectManager;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: nacos
 * @ClassName BaseConfigInfoBetaMapper
 * @description:
 * @author: zhang wenchao
 * @create: 2024-07-24 15:43
 * @Version 1.0
 **/
public class BaseConfigInfoBetaMapper extends ConfigInfoBetaMapperByMySql {
    private DatabaseDialect databaseDialect = DatabaseDialectManager.getInstance().getDialect(this.getDataSource());

    public BaseConfigInfoBetaMapper() {
    }

    public String getTableName() {
        return TableConstant.CONFIG_INFO;
    }

    public String getLimitPageSqlWithOffset(String sql, int startRow, int pageSize) {
        return this.databaseDialect.getLimitPageSqlWithOffset(sql, startRow, pageSize);
    }

    public MapperResult findAllConfigInfoBetaForDumpAllFetchRows(MapperContext context) {
        int startRow = context.getStartRow();
        int pageSize = context.getPageSize();
        String sqlInner = this.getLimitPageSqlWithOffset("SELECT id FROM config_info_beta  ORDER BY id ", startRow, pageSize);
        String sql = " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,beta_ips,encrypted_data_key  FROM ( " + sqlInner + " )  g, config_info_beta t WHERE g.id = t.id ";
        List<Object> paramList = new ArrayList();
        paramList.add(startRow);
        paramList.add(pageSize);
        return new MapperResult(sql, paramList);
    }
}