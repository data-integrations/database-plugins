package io.cdap.plugin.saphana;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class SapHanaDBRecord  extends DBRecord {

    public SapHanaDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
        super(record, columnTypes);
    }

    @SuppressWarnings("unused")
    public SapHanaDBRecord() {
    }

    @Override
    protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                               int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
        if (isUseSchema(resultSet.getMetaData(), columnIndex)) {
            setFieldAccordingToSchema(resultSet, recordBuilder, field, columnIndex);
        } else {
            setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
        }
    }

    private static boolean isUseSchema(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        switch (metadata.getColumnTypeName(columnIndex)) {
            case "*":  //TODO implement this
                return true;
            default:
                return SapHanaSchemaReader.STRING_MAPPED_SAPHANA_TYPES.contains(metadata.getColumnType(columnIndex));
        }
    }

    private Object createSapHanaObject(String type, String value, ClassLoader classLoader) throws SQLException {
        return null; //TODO implement this
    }


    @Override
    protected void writeToDB(PreparedStatement stmt, Schema.Field field, int fieldIndex) throws SQLException {
        int sqlIndex = fieldIndex + 1;
        ColumnType columnType = columnTypes.get(fieldIndex);
        if (SapHanaSchemaReader.STRING_MAPPED_SAPHANA_TYPES_NAMES.contains(columnType.getTypeName()) ||
                SapHanaSchemaReader.STRING_MAPPED_SAPHANA_TYPES.contains(columnType.getType())) {
            stmt.setObject(sqlIndex, createSapHanaObject(columnType.getTypeName(),
                    record.get(field.getName()),
                    stmt.getClass().getClassLoader()));
        } else {
            super.writeToDB(stmt, field, fieldIndex);
        }
    }

    @Override
    protected SchemaReader getSchemaReader() {
        return new SapHanaSchemaReader();
    }
}
