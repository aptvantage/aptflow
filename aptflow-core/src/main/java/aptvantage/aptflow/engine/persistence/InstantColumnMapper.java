package aptvantage.aptflow.engine.persistence;

import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

class InstantColumnMapper implements ColumnMapper<Instant> {
    public Instant map(ResultSet rs, int columnNumber, StatementContext ctx) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(columnNumber);
        if (timestamp == null) {
            return null;
        }
        return timestamp.toInstant();
    }
}
