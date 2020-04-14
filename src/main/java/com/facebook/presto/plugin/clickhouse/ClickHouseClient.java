/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.except.ClickHouseErrorCode;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class ClickHouseClient
        extends BaseJdbcClient
{


    private static final Logger log = Logger.get(ClickHouseClient.class);

    @Inject
    public ClickHouseClient(JdbcConnectorId connectorId, BaseJdbcConfig config, BaseJdbcConfig clickHouseConfig)
            throws SQLException
    {
        super(connectorId, config, "\"", connectionFactory(config, clickHouseConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, BaseJdbcConfig clickHouseConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);

        return new DriverConnectionFactory(new ClickHouseDriver(), config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return new ClickhouseQueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }


    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {

        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }



    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (ClickHouseErrorCode.TABLE_ALREADY_EXISTS.code.toString().equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            if (ClickHouseErrorCode.SYNTAX_ERROR.code.toString().equals(e.getSQLState())) {
                throw new PrestoException(NOT_SUPPORTED, format("Rename column not supported in catalog: '%s'", handle.getCatalogName()), e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        super.renameTable(identity, null, oldTable, newTable);
    }
}