package aptvantage.aptflow.engine.persistence;

import aptvantage.aptflow.model.StepFunctionType;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

class StepFunctionTypeMapper implements ColumnMapper<StepFunctionType> {

    @Override
    public StepFunctionType map(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
        String stringValue = r.getString(columnNumber);
        if (stringValue == null) {
            return null;
        }
        return StepFunctionType.valueOf(stringValue);
    }
}
