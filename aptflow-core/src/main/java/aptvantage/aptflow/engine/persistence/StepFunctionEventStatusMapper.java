package aptvantage.aptflow.engine.persistence;

import aptvantage.aptflow.model.StepFunctionEventStatus;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StepFunctionEventStatusMapper implements ColumnMapper<StepFunctionEventStatus> {

    @Override
    public StepFunctionEventStatus map(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
        String stringValue = r.getString(columnNumber);
        if (stringValue == null) {
            return null;
        }
        return StepFunctionEventStatus.valueOf(stringValue);
    }
}
