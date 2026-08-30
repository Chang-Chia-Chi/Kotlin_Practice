import java.sql.DriverManager;
var c = DriverManager.getConnection("jdbc:duckdb:soak/run/state/report.db");
var st = c.createStatement();
var rs1 = st.executeQuery("select count(*) from wip_summary");
rs1.next(); System.out.println("wip_summary rows: " + rs1.getLong(1));
var rs2 = st.executeQuery("select count(*) from wip_summary_concurrent");
rs2.next(); System.out.println("wip_summary_concurrent rows: " + rs2.getLong(1));
var rs3 = st.executeQuery("select count(*) from equipment_state");
rs3.next(); System.out.println("equipment_state rows: " + rs3.getLong(1));
st.close();
c.close();
/exit
