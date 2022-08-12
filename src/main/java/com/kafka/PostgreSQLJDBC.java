package com.kafka;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQLJDBC {

	private static Connection conn = null;

	static {
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager
				.getConnection(
					"jdbc:postgresql://pg-public-endpoint:5432/db-name",
					"user",
					"password"
				);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}

	public static Connection getConnection() {
		return conn;
	}
}