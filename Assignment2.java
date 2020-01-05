import java.sql.*;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Statement to run queries
	Statement sql;

	// Prepared Statement
	PreparedStatement ps;

	// Resultset for the query
	ResultSet rs;

	// CONSTRUCTOR
	Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			
		}
	}

	// Using the input parameters, establish a connection to be used for this
	// session. Returns true if connection is sucessful
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			return true;
		} catch (SQLException e) {
			
		}
		return false;
	}

	// Closes the connection. Returns true if closure was sucessful
	public boolean disconnectDB() {
		try {
			connection.close();
			boolean b = connection.isClosed();
			return b;
		} catch (SQLException e) {
			
		}
		return false;
	}

	public boolean insertPlayer(int pid, String pname, int globalRank, int cid) {
		try {
			String query_player = "SELECT * FROM player;";
			sql = connection.createStatement();
			rs = sql.executeQuery(query_player);

			while (rs.next()) {
				if (rs.getObject(1).equals(pid)) {
					return false;
				}
			}
			rs.close();

			sql = connection.createStatement();
			String insertPlayer = "INSERT INTO player VALUES ('" + pid + "', '" + pname + "', '" + globalRank + "', '"
					+ cid + "');";
			sql.executeUpdate(insertPlayer);			
			sql.close();
			return true;
		} catch (SQLException e) {
		
		}
		return false;
	}

	public int getChampions(int pid) {
		try {
			sql = connection.createStatement();
			String query_champions = "SELECT COUNT(pid) AS countChampions FROM champion WHERE pid = " + pid + ";";
			rs = sql.executeQuery(query_champions);
			while (rs.next()) {
				return rs.getInt("countChampions");
			}
		} catch (SQLException e) {
			
		}
		return 0;
	}

	public String getCourtInfo(int courtid) {
		try {
			sql = connection.createStatement();
			String query_court = "SELECT court.courtid, court.courtname, court.capacity, tournament.tname FROM court JOIN tournament ON court.tid = tournament.tid" + 
			" WHERE court.courtid = " + courtid + ";";
			rs = sql.executeQuery(query_court);
			String result = "";
			if (rs != null) {
				while (rs.next()) {
					if (!(rs.getObject(1).equals(courtid))) {
						result += "";
					} else {
						result += "courtid:" + rs.getInt("courtid") + " courtname:" + rs.getString("courtname")
								+ " capacity:" + rs.getInt("capacity") + " tournamentname:" + rs.getString("tname");
					}
				}
			}
			sql.close();
			rs.close();
			return result;
		} catch (SQLException e) {
		
		}
		return "";
	}

	public boolean chgRecord(int pid, int year, int wins, int losses) {
		try {
			sql = connection.createStatement();
			String updateRecord = "UPDATE record SET wins = " + wins + ", losses = " + losses + " WHERE year = " + year
					+ " AND pid = " + pid + ";";
			sql.executeUpdate(updateRecord);
			sql.close();
			return true;
		} catch (SQLException e) {
			
		}
		return false;
	}

	public boolean deleteMatcBetween(int p1id, int p2id) {
		try {
			sql = connection.createStatement();
			String delete_match = "DELETE FROM event WHERE winid = " + p1id + " AND lossid = " + p2id + ";";
			sql.executeUpdate(delete_match);
			sql.close();
			return true;
		} catch (SQLException e) {
			
		}
		return false;
	}

	public String listPlayerRanking() {
		try {
			sql = connection.createStatement();
			String listing = "SELECT pname, globalrank FROM player ORDER BY globalrank ASC;";
			rs = sql.executeQuery(listing);
			int index = 1;
			String result = "";
			if (rs != null) {
				while (rs.next()) {
					result += "p" + index + "name:" + rs.getString("pname") + " p" + index + "rank:"
							+ rs.getInt("globalrank") + "\n";
					index++;
				}
			}
			else {
				result += "";
			}
			rs.close();
			sql.close();
			return result;
		} catch (SQLException e) {
		
		}
		return "";
	}

	public int findTriCircle() {
		try {
			sql = connection.createStatement();
			int pAwin, pBwin, pCwin, pAlose, pBlose, pClose, triCircle = 0;
			String eventInfo = "SELECT * FROM event;";
			ResultSet a, b;
			rs = sql.executeQuery(eventInfo);
			a = sql.executeQuery(eventInfo); b = sql.executeQuery(eventInfo);
			while(rs.next()) {
				pAwin = rs.getInt("winid"); pBlose = rs.getInt("lossid");
				while(a.next()) {
					pBwin = a.getInt("winid"); pClose = a.getInt("lossid");
					while(b.next()) {
						pCwin = b.getInt("winid"); pAlose = b.getInt("lossid");
						if(pAwin == pAlose && pBwin == pBlose && pCwin == pClose) {
							triCircle++;
						}
					}
				}
			}
			sql.close();
			rs.close();
			a.close();
			b.close();
			return triCircle;
		}
		catch(SQLException e){
			
		}
		return 0;
	}

	public boolean updateDB() {
		try {
			sql = connection.createStatement();
			sql.executeUpdate("CREATE TABLE championPlayers (pid INTEGER, pname VARCHAR(20), nchampions INTEGER);");
			sql.executeUpdate("CREATE VIEW champions_pid(pid, pname, nchampions) AS SELECT player.pid, player.pname, COUNT(champion.pid) AS nchampions FROM champion JOIN player ON champion.pid = player.pid GROUP BY player.pid, player.pname;");
			sql.executeUpdate("INSERT INTO championPlayers (SELECT * FROM champions_pid);");
			String drop = "DROP VIEW champions_pid;";
			sql.executeUpdate(drop);
			sql.close();
			return true;
		} catch (SQLException e) {
			
		}
		return false;
	}
}
