package org.apache.zeppelin.oracle;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oraclesql interpreter for Zeppelin.
 *
 * @author Cesar.X 316289290@qq.com
 *
 */
public class OracleInterpreter extends Interpreter {
	Logger logger = LoggerFactory.getLogger(OracleInterpreter.class);
	int commandTimeOut = 600000;

	static {
		Interpreter.register("oracle", OracleInterpreter.class.getName());
	}

	public OracleInterpreter(Properties property) {
		super(property);
	}


    @Override
	public void open() {
	}

	@Override
	public void close() {
	}

	@Override
	public InterpreterResult interpret(String cmd,
			InterpreterContext contextInterpreter) {
		logger.info("Run oracle command '" + cmd + "'");
		String user = "";
		String password = "";
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "";
		/**
		 * 以MMB sso 数据库 oracle 连接串为例
		 * sso.oracle.jdbc.driver=oracle.jdbc.driver.OracleDriver
		   sso.oracle.jdbc.url=jdbc:oracle:thin:@(description=(address_list=(address=(host=172.21.99.18)(protocol=tcp)(port=1521))(load_balance=yes)(failover=yes))(connect_data=(service_name=mmbsso)))
		   sso.oracle.jdbc.username=sso
		   sso.oracle.jdbc.password=sso
		 */


		// get Properties
		Properties intpProperty = getProperty();
		for (Object k : intpProperty.keySet()) {
			String key = (String) k;
			String value = (String) intpProperty.get(key);

			if (key.equals("--url") || key.equals("-url")) {
				url = value;
			} else if (key.equals("--password") || key.equals("-p")) {
				password = value;
			} else if (key.equals("--user") || key.equals("-u")) {
				user = value;
			} else {
				logger.info("else key : /" + key + "/");
			}
		}

		// for jdbc connection and result
		Connection conn;
		Statement stmt;
		ResultSet rs;

		logger.info("Connect to " + url);

		try {
			// connect to oracle
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(cmd);

			// make result format ( for zeppelin table style )
			String queryResult = "";
			Vector<String> columnNames = new Vector<String>();

			if (rs != null) {
				ResultSetMetaData columns = rs.getMetaData();
				for (int i = 1; i <= columns.getColumnCount(); ++i) {
					if (i != 1) {
						queryResult += "\t";
					}
					queryResult += columns.getColumnName(i);
                    //这边进行特别处理

					columnNames.add(columns.getColumnName(i));
				}
				queryResult += "\n";

				logger.info("columnNames is: " + columnNames.toString());

				while (rs.next()) {
					for (int i = 0; i < columnNames.size(); ++i) {
						if (i != 0) {
							queryResult += "\t";
						}
						queryResult += rs.getString(columnNames.get(i));
					}
					queryResult += "\n";
				}
			}

			String msg = "%table ";
			msg += queryResult;

			// disconnect
			stmt.close();
			conn.close();

			return new InterpreterResult(Code.SUCCESS, msg);
		} catch (ClassNotFoundException e) {
			logger.error("Class not found", e);
			return new InterpreterResult(Code.ERROR, e.getMessage());
		} catch (SQLException e) {
			logger.error("Can not run " + cmd, e);
			return new InterpreterResult(Code.ERROR, e.getMessage());
		}finally {
            stmt = null;
		}
	}

	@Override
	public void cancel(InterpreterContext context) {
	}

	@Override
	public FormType getFormType() {
		return FormType.SIMPLE;
	}

	@Override
	public int getProgress(InterpreterContext context) {
		return 0;
	}

	@Override
	public Scheduler getScheduler() {
		return SchedulerFactory.singleton().createOrGetFIFOScheduler(
				OracleInterpreter.class.getName() + this.hashCode());
	}

	@Override
	public List<String> completion(String buf, int cursor) {
		return null;
	}

}
