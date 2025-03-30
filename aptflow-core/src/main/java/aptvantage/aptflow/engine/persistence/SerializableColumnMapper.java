package aptvantage.aptflow.engine.persistence;

import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

class SerializableColumnMapper implements ColumnMapper<Serializable> {

    @Override
    public Serializable map(ResultSet rs, int columnNumber, StatementContext ctx) throws SQLException {
        byte[] bytes = rs.getBytes(columnNumber);
        if (bytes == null) {
            return null;
        }
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
             ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
            return (Serializable) objectIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
