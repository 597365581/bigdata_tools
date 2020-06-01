package com.yongqing.presto.hbase.io;

import com.yongqing.presto.hbase.HbaseClient;
import com.yongqing.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.hbase.client.Connection;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HbasePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HbaseClient client;
    private final Connection connection;

    @Inject
    public HbasePageSinkProvider(
            Connection connection,
            HbaseClient client)
    {
        this.client = requireNonNull(client, "client is null");
        this.connection = requireNonNull(connection, "connection is null");
    }

    //TODO
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkProperties pageSinkProperties) {
        HbaseTableHandle tableHandle = (HbaseTableHandle) outputTableHandle;
        return new HbasePageSink(connection, client.getTable(tableHandle.toSchemaTableName()));
    }
    //TODO
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkProperties pageSinkProperties) {
        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle, pageSinkProperties);
    }
}
