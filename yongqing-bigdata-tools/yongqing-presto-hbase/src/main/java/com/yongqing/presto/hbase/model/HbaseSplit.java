package com.yongqing.presto.hbase.model;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.mapreduce.TabletSplitMetadata;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HbaseSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String rowId;
    private final String schema;
    private final String table;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final List<HbaseColumnConstraint> constraints;
    private final TabletSplitMetadata splitMetadata;
    private final List<HostAddress> addresses;

    @JsonCreator
    public HbaseSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("splitMetadata") TabletSplitMetadata splitMetadata,
            @JsonProperty("nodeSelectionStrategy") NodeSelectionStrategy nodeSelectionStrategy,
            @JsonProperty("constraints") List<HbaseColumnConstraint> constraints,
            @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.constraints = ImmutableList.copyOf(requireNonNull(constraints, "constraints is null"));
        this.splitMetadata = requireNonNull(splitMetadata, "splitMetadata is null");
        if(nodeSelectionStrategy == null){
            this.nodeSelectionStrategy = NodeSelectionStrategy.NO_PREFERENCE;
        } else{
            this.nodeSelectionStrategy = nodeSelectionStrategy;
        }

        // Parse the host address into a list of addresses, this would be an Hbase Tablet server or some localhost thing
        this.addresses = ImmutableList.of(HostAddress.fromString(splitMetadata.getRegionLocation()));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonIgnore
    public String getFullTableName()
    {
        return (this.getSchema().equals("default") ? "" : this.getSchema() + ".") + this.getTable();
    }

    @JsonProperty
    public TabletSplitMetadata getSplitMetadata()
    {
        return splitMetadata;
    }

    @JsonProperty
    public List<HbaseColumnConstraint> getConstraints()
    {
        return constraints;
    }


    public boolean isRemotelyAccessible()
    {
        return true;
    }


    public List<HostAddress> getAddresses()
    {
        return addresses;
    }
    //TODO
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return nodeSelectionStrategy;
    }
    //TODO
    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
        if (sortedCandidates == null || sortedCandidates.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "sortedCandidates is null or empty for HbaseSplit");
        }
//TODO
//        if (getNodeSelectionStrategy() == SOFT_AFFINITY) {
//            // Use + 1 as secondary hash for now, would always get a different position from the first hash.
//            int size = sortedCandidates.size();
//            int mod = path.hashCode() % size;
//            int position = mod < 0 ? mod + size : mod;
//            return ImmutableList.of(
//                    sortedCandidates.get(position),
//                    sortedCandidates.get((position + 1) % size));
//        }
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schema", schema)
                .add("table", table)
                .add("rowId", rowId)
                .add("splitMetadata", splitMetadata)
                .add("constraints", constraints)
                .add("nodeSelectionStrategy",nodeSelectionStrategy)
                .toString();
    }
}
