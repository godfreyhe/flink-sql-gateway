package com.ververica.flink.table.client.result;

import com.ververica.flink.table.rest.result.ColumnInfo;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import java.util.List;

class ResultUtil {

	// TODO
	public static TableSchema convertToTableSchema(List<ColumnInfo> columnInfos) {
		TableSchema.Builder builder = TableSchema.builder();
		for (ColumnInfo column : columnInfos) {
			builder.field(column.getName(), LogicalTypeDataTypeConverter.toDataType(column.getLogicalType()));
		}
		return builder.build();
	}
}
