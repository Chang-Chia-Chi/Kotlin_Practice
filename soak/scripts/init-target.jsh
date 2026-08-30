import java.sql.DriverManager;
var c = DriverManager.getConnection("jdbc:duckdb:soak/run/state/report.db");
var st = c.createStatement();
st.execute("CREATE TABLE wip_summary (site VARCHAR, lots BIGINT, total_qty DECIMAL(38,3))");
st.execute("CREATE TABLE equipment_state (state VARCHAR, tools BIGINT)");
st.execute("CREATE TABLE wip_summary_concurrent (site VARCHAR, lots BIGINT, total_qty DECIMAL(38,3))");
st.close();
c.close();
System.out.println("TARGET_INIT_OK");
/exit
