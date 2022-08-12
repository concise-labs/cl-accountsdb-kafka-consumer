package com.kafka;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.bitcoinj.core.Base58;

public class CLKafkaDBImpl {

	static Connection conn
        = PostgreSQLJDBC.getConnection();

    public void addRecord(AccountUpdateEvent accountUpdateEvent) throws SQLException {
    	String query = "INSERT INTO raw_kafka_data("
    		+ "slot, pubkey, lamports, owner, executable, rent_epoch, data, data_as_bytes, write_version, timestamp_validator, timestamp_record)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())";

        PreparedStatement ps = conn.prepareStatement(query);

        ps.setLong(1, accountUpdateEvent.getSlot());
        ps.setString(2, accountUpdateEvent.getPubkey());
        ps.setLong(3, accountUpdateEvent.getLamports());
        ps.setString(4, accountUpdateEvent.getOwner());
        ps.setBoolean(5, accountUpdateEvent.getExecutable());
        ps.setLong(6, accountUpdateEvent.getRentEpoch());
        ps.setString(7, accountUpdateEvent.getData());
        ps.setBytes(8, accountUpdateEvent.getDataAsBytes());
        ps.setLong(9, accountUpdateEvent.getWriteVersion());
        // ps.setTimestamp(9, accountUpdateEvent.getTimestampValidator());
        // ps.setTimestamp(10, accountUpdateEvent.getTimestampRecord());
        
        ps.executeUpdate();
    }

    public void printRecord() throws SQLException {
    	System.out.println("Printing data as bytes from postgres...");
		String query = "SELECT data_as_bytes FROM raw_kafka_data WHERE id=70";
		Statement stmt = conn.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(query);
			if (rs != null) {
				while (rs.next()) {
					byte[] dataAsBytes = rs.getBytes("data_as_bytes");
					System.out.println(new String(dataAsBytes));
				}
				rs.close();
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public void printRecordPg() throws SQLException {
    	System.out.println("Printing pubkey and owner converted to base58...");
		String query = "SELECT * FROM account ORDER BY updated_on DESC LIMIT 10";
		Statement stmt = conn.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(query);
			if (rs != null) {
				while (rs.next()) {
					System.out.println("#####################################");
					byte[] pubkeyBytes = rs.getBytes("pubkey");
					System.out.println("pubkey: " + Base58.encode(pubkeyBytes));
					byte[] ownerBytes = rs.getBytes("owner");
					System.out.println("owner: " + Base58.encode(ownerBytes));
					System.out.println("#####################################");
				}
				rs.close();
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}