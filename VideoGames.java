import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.Scanner;

public class VideoGames {
    static Connection connection;

    public static void main(String[] args) throws Exception {
	
		// startup sequence
		MyDatabase db = new MyDatabase();
		runConsole(db);		

		System.out.println("Exiting...");
	}

	public static void runConsole(MyDatabase db){

		Scanner console = new Scanner(System.in);

		System.out.print("db > ");
		printHelp();
		String line = console.nextLine();
		String [] parts;
		String arg = "";

		while(line != null && !line.equals("q")){
			parts = line.split("\\s+");
			if (line.indexOf(" ") > 0)
				arg = line.substring(line.indexOf(" ")).trim();
			if (parts[0].equals("h"))
				printHelp();
			else if (parts[0].equals("v")){
				db.getAllVideoGames();
			}
			else if (parts[0].equals("vi")){
				db.getAllVideoGamesInfo();
			}
			else if (parts[0].equals("vs")){
				db.getVideoGamesSales();
			}
			else if (parts[0].equals("vre")){
				db.getVideoGamesRatingEveryone();
			}
			else if (parts[0].equals("vrm")){
				db.getVideoGamesRatingMature();
			}
			else if (parts[0].equals("vrt")){
				db.getVideoGamesRatingTeen();
			}
			else if (parts[0].equals("vsi")){
				if (parts.length >= 2)
					db.getSingleVideoGameInfo(arg);
				else
					System.out.println("Require an argument for this command");
			}
			else if (parts[0].equals("vl")){
				db.getLongestVideoGame();
			}
			else if (parts[0].equals("al")){
				db.getAllLength();
			}
			else if (parts[0].equals("p")){
				db.getAllPublishers();
			}
			else if (parts[0].equals("pi")){
				db.getAllPublishersInfo();
			}
			else if (parts[0].equals("psi")){
				if (parts.length >= 2)
					db.getSinglePublisherInfo(arg);
				else
					System.out.println("Require an argument for this command");
			}
			else if (parts[0].equals("pr")){
				db.getPublisherRatios();
			}
			else if (parts[0].equals("pc")){
				db.getConsolesAmountPerPublisher();
			}
			else if (parts[0].equals("pm")){
				db.getPublishersMostPopularGames();
			}
			else if (parts[0].equals("c")){
				db.getAllConsoles();
			}
			else if (parts[0].equals("ci")){
				db.getAllConsolesInfo();
			}
			else if (parts[0].equals("ca")){
				db.getConsolesAge();
			}
			else if (parts[0].equals("csi")){
				if (parts.length >= 2)
					db.getSingleConsoleInfo(arg);
				else
					System.out.println("Require an argument for this command");
			}
			else if (parts[0].equals("cv")){
				db.getGamesAmountPerConsole();
			}
			else if (parts[0].equals("d")){
				db.getAllDevelopers();
			}
			else if (parts[0].equals("di")){
				db.getAllDevelopersInfo();
			}
			else if (parts[0].equals("dsi")){
				if (parts.length >= 2)
					db.getSingleDeveloperInfo(arg);
				else
					System.out.println("Require an argument for this command");
			}
			else if(parts[0].equals("cpx"))
				db.getMostPopularXbox360Games();
			else if(parts[0].equals("cp3"))
				db.getMostPopularPlayStation3Games();
			else if(parts[0].equals("cpw"))
				db.getMostPopularWiiGames();
			else if(parts[0].equals("cpd"))
				db.getMostPopularNintendoDSGames();
			else if(parts[0].equals("cpp"))
				db.getMostPopularSonyPSPGames();
			else
				System.out.println("Read the help with h, or find help somewhere else.");

			System.out.print("db > ");
			line = console.nextLine();
		}

		console.close();
	}

	private static void printHelp(){
		System.out.println("\nVideo Games Database Help Console");
		System.out.println("Commands:");
		System.out.println("h - Get help\n");
		System.out.println("Video Games Queries");
		System.out.println("\tv - Print all video games");
		System.out.println("\tvi - Print all video games info");
		System.out.println("\tvsi title - Print single video game info for 'title'");
		System.out.println("\tvs - Print all video games sales, sorted from most to least (in millions)");
		System.out.println("\tvre - Print all video games targeted for the everyone age group, alphabetized");
		System.out.println("\tvrt - Print all video games targeted for teens, alphabetized");
		System.out.println("\tvrm - Print all video games targeted for mature audience, alphabetized");
		System.out.println("\tvl - Print all video games in order of longest length (in hours)");
		System.out.println("\tal - Print all length info (in hours)");
		System.out.println("Publisher Queries");
		System.out.println("\tp - Print all publishers");
		System.out.println("\tpi - Print all publishers info");
		System.out.println("\tpsi name - Print single publisher info for 'name'");
		System.out.println("\tpr - Print the ratios between % developed in house and % indie releases for all publishers");
		System.out.println("\tpc - Print the number of consoles developed by a publisher");
		System.out.println("\tpm - Print the most popular video game published by a publisher");
		System.out.println("Console Queries");
		System.out.println("\tc - Print all consoles");
		System.out.println("\tci - Print all consoles info");
		System.out.println("\tca - Print all consoles' release years, newest to oldest");
		System.out.println("\tcsi name - Print single console info for 'name'");
		System.out.println("\tcv - Print the number of video games per console");
		System.out.println("\tcpx - Print most popular Xbox 360 games");
		System.out.println("\tcp3 - Print most popular PlayStation 3 games");
		System.out.println("\tcpw - Print most popular Nintendo Wii games");
		System.out.println("\tcpd - Print most popular Nintendo DS games");
		System.out.println("\tcpp - Print most popular Sony PSP games");
		System.out.println("Developers Queries");
		System.out.println("\td - Print all developers");
		System.out.println("\tdi - Print all developers info");
		System.out.println("\tdsi name - Print single developer info for 'name'");
		System.out.println("q - Exit the program");

		System.out.println("---- end help ----- ");
	}
}

class MyDatabase{
	private Connection connection;
	public MyDatabase(){
		try {
			Class.forName("org.hsqldb.jdbcDriver");
			// creates an in-memory database
			connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");

			createTables();
			readInData("create_all_tables.txt");
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace(System.out);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getAllVideoGames() {
		try {
			String sql = "Select videogametitle, consolename from videogames;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game: " + resultSet.getString("videogametitle") + " | Console: " + resultSet.getString("consolename") + "\n");	
			}
			resultSet.close();
			statement.close();


		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getSingleVideoGameInfo(String title) {
		try {
			PreparedStatement getSingle = connection.prepareStatement("Select * from videogames" + " where videogametitle = ?;");

			getSingle.setString(1, title);

			ResultSet resultSet = getSingle.executeQuery();
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Genres: " + resultSet.getString("genres") + " | Console: " + resultSet.getString("consolename") + " | Publisher: " + resultSet.getString("publishername") + " | Review Score: " + resultSet.getInt("reviewscore") + " | Sales: " + resultSet.getInt("sales") + " | Release Rating: " + resultSet.getString("releaserating") + " | Release Year: " + resultSet.getString("releaseyear") + " | Max Players: " + resultSet.getInt("maxplayers") + "\n");	
			}
			resultSet.close();
			getSingle.close();
		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getVideoGamesSales() {
		try {
			String sql = "Select * from videogames order by videogames.sales desc;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Sales: " + resultSet.getDouble("sales") + " Million \n");	
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getVideoGamesRatingEveryone() {
		try {
			String sql = "Select * from videogames where releaserating = 'E' order by videogames.videogametitle;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Release Rating: " + resultSet.getString("releaserating") + "\n");	
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getVideoGamesRatingMature() {
		try {
			String sql = "Select * from videogames where releaserating = 'M' order by videogames.videogametitle;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Release Rating: " + resultSet.getString("releaserating") + "\n");	
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getVideoGamesRatingTeen() {
		try {
			String sql = "Select * from videogames where releaserating = 'T' order by videogames.videogametitle;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Release Rating: " + resultSet.getString("releaserating") + "\n");	
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getAllVideoGamesInfo() {
		try {
			String sql = "Select * from videogames;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Genres: " + resultSet.getString("genres") + " | Console: " + resultSet.getString("consolename") + " | Publisher: " + resultSet.getString("publishername") + " | Review Score: " + resultSet.getInt("reviewscore") + " | Sales: " + resultSet.getDouble("sales") + " | Release Rating: " + resultSet.getString("releaserating") + " | Release Year: " + resultSet.getString("releaseyear") + " | Max Players: " + resultSet.getInt("maxplayers") + "\n");	
			}
			resultSet.close();
			statement.close();


		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getPublisherRatios() {
		try {
			String sql = "Select * from publishers order by publishers.publishername;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Publisher Name: " + resultSet.getString("publishername") + " | % Developed In House: " + (resultSet.getDouble("percentdevelopedinhouse") * 100) + "% | % Indie Releases: " + (resultSet.getDouble("percentindiereleases") * 100) + "%" + " | Ratio In House/Indie: " + ((resultSet.getDouble("percentdevelopedinhouse") * 100)/(resultSet.getDouble("percentindiereleases") * 100)) + "\n");				
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }
	
	public void getAllPublishers() {
		try {
			String sql = "Select publishername from publishers;";
			
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;
			
			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("publishername") + "\n");	
			}
			resultSet.close();
			statement.close();
			
			
		}catch (SQLException e) {
			e.printStackTrace(System.out);
		}
    }
	
	public void getSinglePublisherInfo(String name) {
		try {
			PreparedStatement getSingle = connection.prepareStatement("Select * from publishers" + " where publishername = ?;");

			getSingle.setString(1, name);

			ResultSet resultSet = getSingle.executeQuery();
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Publisher Name: " + resultSet.getString("publishername") + " | Classification: " + resultSet.getString("classification") + " | Total Revenue: " + resultSet.getInt("totalrevenue") + " | Average Revenue: " + resultSet.getInt("avgrevenue") + " | % Developed In House: " + (resultSet.getDouble("percentdevelopedinhouse") * 100) + "% | % Indie Releases: " + (resultSet.getDouble("percentindiereleases") * 100) + "%\n");				
			}
			resultSet.close();
			getSingle.close();
		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}
	
	public void getAllPublishersInfo() {
		try {
			String sql = "Select * from publishers;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Publisher Name: " + resultSet.getString("publishername") + " | Classification: " + resultSet.getString("classification") + " | Total Revenue: " + resultSet.getInt("totalrevenue") + " | Average Revenue: " + resultSet.getInt("avgrevenue") + " | % Developed In House: " + (resultSet.getDouble("percentdevelopedinhouse") * 100) + "% | % Indie Releases: " + (resultSet.getDouble("percentindiereleases") * 100) + "%\n");				
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getAllConsoles() {
		try {
			String sql = "Select consolename from consoles;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("consolename") + "\n");	
			}
			resultSet.close();
			statement.close();


		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getSingleConsoleInfo(String name) {
		try {
			PreparedStatement getSingle = connection.prepareStatement("Select * from consoles" + " where consolename = ?;");

			getSingle.setString(1, name);

			ResultSet resultSet = getSingle.executeQuery();
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Console Name: " + resultSet.getString("consolename") + " | Console Type: " + resultSet.getString("type") + " | Publisher: " + resultSet.getString("publishername") + " | Year Released: " + resultSet.getString("consoleyear") + " | Units Sold: " + resultSet.getInt("unitsold") + "\n");				
			}
			resultSet.close();
			getSingle.close();
		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}

	public void getAllConsolesInfo() {
		try {
			String sql = "Select * from consoles;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Console Name: " + resultSet.getString("consolename") + " | Console Type: " + resultSet.getString("type") + " | Publisher: " + resultSet.getString("publishername") + " | Year Released: " + resultSet.getString("consoleyear") + " | Units Sold: " + resultSet.getInt("unitsold") + "\n");				
			}
			resultSet.close();
			statement.close();


		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getConsolesAge() {
		try {
			String sql = "Select * from consoles order by consoleyear desc;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Console Name: " + resultSet.getString("consolename") + " | Year Released: " + resultSet.getString("consoleyear") + "\n");				
			}
			resultSet.close();
			statement.close();


		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getAllDevelopers() {
		try {
			String sql = "Select devname from developers;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("devname") + "\n");	
			}
			resultSet.close();
			statement.close();


		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getSingleDeveloperInfo(String name) {
		try {
			PreparedStatement getSingle = connection.prepareStatement("Select * from developers" + " where devname = ?;");

			getSingle.setString(1, name);

			ResultSet resultSet = getSingle.executeQuery();
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Developer Name: " + resultSet.getString("devname") + " | Administrative Division: " + resultSet.getString("admindiv") + " | Country: " + resultSet.getString("country") + " | Year Established: " + resultSet.getString("devyear") + " | Notable Games: " + resultSet.getString("notablegames") + "\n");				
			}
			resultSet.close();
			getSingle.close();
		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}

	public void getAllDevelopersInfo() {
		try {
			String sql = "Select * from developers;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Developer Name: " + resultSet.getString("devname") + " | Administrative Division: " + resultSet.getString("admindiv") + " | Country: " + resultSet.getString("country") + " | Year Established: " + resultSet.getString("devyear") + " | Notable Games: " + resultSet.getString("notablegames") + "\n");				
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }

	public void getAllLength() {
		try {
			String sql = "Select * from length join allplaystyles on length.videogametitle = allplaystyles.videogametitle and length.consolename = allplaystyles.consolename" + " join completionist on length.videogametitle = completionist.videogametitle and length.consolename = completionist.consolename" + " join mainplusextras on length.videogametitle = mainplusextras.videogametitle and length.consolename = mainplusextras.consolename" + " join mainstory on length.videogametitle = mainstory.videogametitle and length.consolename = mainstory.consolename;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Console: " + resultSet.getString("consolename") + "\n\tLength->All Play Styles:\t[Average->" + resultSet.getDouble("allplaystyles.average") + "] [Leisure->" + resultSet.getDouble("allplaystyles.leisure") + "] [Median->" + resultSet.getDouble("allplaystyles.median") + "] [Polled->" + resultSet.getDouble("allplaystyles.polled") + "] [Speed Run->" + resultSet.getDouble("allplaystyles.speedrun") + "]\n\tLength->Completionist:\t\t[Average->" + resultSet.getString("completionist.average") + "] [Leisure->" + resultSet.getDouble("completionist.leisure") + "] [Median->" + resultSet.getDouble("completionist.median") + "] [Polled->" + resultSet.getDouble("completionist.polled") + "] [Speed Run->" + resultSet.getDouble("completionist.speedrun") + "]\n\tLength->Main + Extras:\t\t[Average->" + resultSet.getString("mainplusextras.average") + "] [Leisure->" + resultSet.getDouble("mainplusextras.leisure") + "] [Median->" + resultSet.getDouble("mainplusextras.median") + "] [Polled->" + resultSet.getDouble("mainplusextras.polled") + "] [Speed Run->" + resultSet.getDouble("mainplusextras.speedrun") + "]\n\tLength->Main Story:\t\t[Average->" + resultSet.getString("mainstory.average") + "] [Leisure->" + resultSet.getDouble("mainstory.leisure") + "] [Median->" + resultSet.getDouble("mainstory.median") + "] [Polled->" + resultSet.getDouble("mainstory.polled") + "] [Speed Run->" + resultSet.getDouble("mainstory.speedrun") + "]\n");				
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
    }	

	public void getLongestVideoGame() {
		try {
			String sql = "Select videogametitle, consolename," + 
			" (allplaystyles.average+allplaystyles.leisure+allplaystyles.median+allplaystyles.polled+allplaystyles.speedrun+"+
			"completionist.average+completionist.leisure+completionist.median+completionist.polled+completionist.speedrun+"+
			"mainplusextras.average+mainplusextras.leisure+mainplusextras.median+mainplusextras.polled+mainplusextras.speedrun+"+
			"mainstory.average+mainstory.leisure+mainstory.median+mainstory.polled+mainstory.speedrun)"+
			" as totallength from length join allplaystyles on length.videogametitle = allplaystyles.videogametitle and length.consolename = allplaystyles.consolename" + " join completionist on length.videogametitle = completionist.videogametitle and length.consolename = completionist.consolename" + " join mainplusextras on length.videogametitle = mainplusextras.videogametitle and length.consolename = mainplusextras.consolename" + " join mainstory on length.videogametitle = mainstory.videogametitle and length.consolename = mainstory.consolename" + " order by totallength desc limit 100;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Video Game Title: " + resultSet.getString("videogametitle") + " | Console: " + resultSet.getString("consolename") + " | Total Length: " + resultSet.getString("totallength").substring(0, 6) + " hours\n");				
			}
			resultSet.close();
			statement.close();

		}catch (SQLException e) {
				e.printStackTrace(System.out);
		}
	}

	public void getGamesAmountPerConsole() {
		try {
			String sql = "select consolename, count(videogametitle) as numOfGames from videogames" + 
			" join platforms on videogames.videogametitle = platforms.videogametitle" + 
			" join consoles on consoles.consolename = platforms.consolename" + 
			" group by videogames.consolename;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Console: " + resultSet.getString("consolename") + " | Number of Video Games: " + resultSet.getString("numOfGames") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getConsolesAmountPerPublisher() {
		try {
			String sql = "select publishername, count(consolename) as numOfConsoles from consoles" + 
			" join publishedforconsole on consoles.consolename = publishedforconsole.consolename" + 
			" join publishers on publishers.publishername = publishedforconsole.publishername" + 
			" group by consoles.publishername;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Publisher: " + resultSet.getString("publishername") + " | Number of Consoles: " + resultSet.getString("numOfConsoles") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getPublishersMostPopularGames() {
		try {
			String sql = "select publishername, videogametitle, consolename, sales from publishers join" + 
			" publishedforgame on publishers.publishername = publishedforgame.publishername join videogames on" + 
			" videogames.videogametitle = publishedforgame.videogametitle" + 
			" ORDER BY videogames.sales DESC LIMIT 25;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			while (resultSet.next()) {
				System.out.println((idx++) + ". Publisher: " + resultSet.getString("publishername") + " | Video Game Title: " + resultSet.getString("videogametitle") + " | Console: " + resultSet.getString("consolename") + " | Sales: " + resultSet.getDouble("sales") + " Million\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getMostPopularXbox360Games() {
		try {
			String sql = "SELECT videogametitle from videogames JOIN consoles on videogames.consolename = consoles.consolename WHERE consoles.consolename = 'Xbox 360' ORDER BY videogames.sales DESC LIMIT 25;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;
			System.out.println("The most popular videogames for Xbox 360: \n");
			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("videogametitle") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getMostPopularPlayStation3Games() {
		try {
			String sql = "SELECT videogametitle from videogames JOIN consoles on videogames.consolename = consoles.consolename WHERE consoles.consolename = 'PlayStation 3' ORDER BY videogames.sales DESC LIMIT 25;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			System.out.println("The most popular videogames for Playstation 3: \n");
			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("videogametitle") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getMostPopularWiiGames() {
		try {
			String sql = "SELECT videogametitle from videogames JOIN consoles on videogames.consolename = consoles.consolename WHERE consoles.consolename = 'Nintendo Wii' ORDER BY videogames.sales DESC LIMIT 25;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			System.out.println("The most popular videogames for Nintendo Wii: \n");
			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("videogametitle") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getMostPopularNintendoDSGames() {
		try {
			String sql = "SELECT videogametitle from videogames JOIN consoles on videogames.consolename = consoles.consolename WHERE consoles.consolename = 'Nintendo DS' ORDER BY videogames.sales DESC LIMIT 25;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			System.out.println("The most popular videogames for Nintendo DS: \n");
			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("videogametitle") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getMostPopularSonyPSPGames() {
		try {
			String sql = "SELECT videogametitle from videogames JOIN consoles on videogames.consolename = consoles.consolename WHERE consoles.consolename = 'Sony PSP' ORDER BY videogames.sales DESC LIMIT 25;";

			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			int idx = 1;

			System.out.println("The most popular videogames for Sony PSP: \n");
			while (resultSet.next()) {
				System.out.println((idx++) + ". " + resultSet.getString("videogametitle") + "\n");		
			}
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	private void createTables(){
		String videoGames =	"CREATE TABLE videogames ( "+
			" videogametitle VARCHAR(100) NOT NULL,"+
			" genres VARCHAR(100),"+
			" consolename VARCHAR(30) NOT NULL,"+
			" publishername	VARCHAR(30),"+
			" reviewscore INTEGER,"+
			" sales FLOAT(25),"+
			" releaserating	CHAR(1),"+
			" releaseyear VARCHAR(4),"+
			" maxplayers INTEGER,"+
			" PRIMARY KEY(videogametitle,consolename)" +
			");";
		try {
            connection.createStatement().executeUpdate(videoGames);

            String publishers = "CREATE TABLE publishers ( "+
				" publishername	VARCHAR(100) NOT NULL,"+
				" classification VARCHAR(30),"+
				" totalrevenue INTEGER,"+
				" avgrevenue INTEGER,"+
				" percentdevelopedinhouse FLOAT(25),"+
				" percentindiereleases FLOAT(25),"+
				" PRIMARY KEY(publishername)" +
				");";
			
            connection.createStatement().executeUpdate(publishers);

			String consoles = "CREATE TABLE consoles ( "+
				" consolename VARCHAR(30) NOT NULL,"+
				" type VARCHAR(30),"+
				" publishername VARCHAR(30),"+
				" consoleyear VARCHAR(4),"+
				" unitsold INTEGER,"+
				" PRIMARY KEY(consolename)"+
				");";

            connection.createStatement().executeUpdate(consoles);

			String developers = "CREATE TABLE developers ( "+
				" devname VARCHAR(100) NOT NULL,"+
				" admindiv VARCHAR(100),"+
				" country VARCHAR(100),"+
				" devyear VARCHAR(100),"+
				" notablegames VARCHAR(1000),"+
				" PRIMARY KEY(devname)"+
				");";

            connection.createStatement().executeUpdate(developers);

			String length = "CREATE TABLE length ( "+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" PRIMARY KEY(videogametitle,consolename),"+
				" FOREIGN KEY (videogametitle,consolename) REFERENCES videogames(videogametitle,consolename)"+
				");";

            connection.createStatement().executeUpdate(length);

			String allplaystyles = "CREATE TABLE allplaystyles ( "+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" average FLOAT(25),"+
				" leisure FLOAT(25),"+
				" median FLOAT(25),"+
				" polled FLOAT(25),"+
				" speedrun FLOAT(25),"+
				" PRIMARY KEY(videogametitle,consolename),"+
				" FOREIGN KEY (videogametitle,consolename) REFERENCES videogames(videogametitle,consolename)"+
				");";

            connection.createStatement().executeUpdate(allplaystyles);

			String completionist = "CREATE TABLE completionist ( "+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" average FLOAT(25),"+
				" leisure FLOAT(25),"+
				" median FLOAT(25),"+
				" polled FLOAT(25),"+
				" speedrun FLOAT(25),"+
				" PRIMARY KEY(videogametitle,consolename),"+
				" FOREIGN KEY (videogametitle,consolename) REFERENCES videogames(videogametitle,consolename)"+
				");";

            connection.createStatement().executeUpdate(completionist);

			String mainplusextras = "CREATE TABLE mainplusextras ( "+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" average FLOAT(25),"+
				" leisure FLOAT(25),"+
				" median FLOAT(25),"+
				" polled FLOAT(25),"+
				" speedrun FLOAT(25),"+
				" PRIMARY KEY(videogametitle,consolename),"+
				" FOREIGN KEY (videogametitle,consolename) REFERENCES videogames(videogametitle,consolename)"+
				");";

            connection.createStatement().executeUpdate(mainplusextras);	

			String mainstory = "CREATE TABLE mainstory ( "+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" average FLOAT(25),"+
				" leisure FLOAT(25),"+
				" median FLOAT(25),"+
				" polled FLOAT(25),"+
				" speedrun FLOAT(25),"+
				" PRIMARY KEY(videogametitle,consolename),"+
				" FOREIGN KEY (videogametitle,consolename) REFERENCES videogames(videogametitle,consolename)"+
				");";

            connection.createStatement().executeUpdate(mainstory);

			String platforms = "CREATE TABLE platforms ( "+
				" consolename VARCHAR(30) NOT NULL,"+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" FOREIGN KEY(consolename) REFERENCES consoles(consolename),"+
				" FOREIGN KEY(videogametitle,consolename) REFERENCES videogames(videogametitle,consolename),"+
				" PRIMARY KEY(consolename,videogametitle)"+
				");";

            connection.createStatement().executeUpdate(platforms);

			String publishedforgame = "CREATE TABLE publishedforgame ( "+
				" publishername	VARCHAR(30) NOT NULL,"+
				" videogametitle VARCHAR(100) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" FOREIGN KEY(publishername) REFERENCES publishers(publishername),"+
				" FOREIGN KEY(videogametitle,consolename) REFERENCES videogames(videogametitle,consolename),"+
				" PRIMARY KEY(publishername,videogametitle,consolename)"+
				");";

            connection.createStatement().executeUpdate(publishedforgame);

			String publishedforconsole = "CREATE TABLE publishedforconsole ( "+
				" publishername	VARCHAR(30) NOT NULL,"+
				" consolename VARCHAR(30) NOT NULL,"+
				" FOREIGN KEY(publishername) REFERENCES publishers(publishername),"+
				" FOREIGN KEY(consolename) REFERENCES consoles(consolename),"+
				" PRIMARY KEY(publishername,consolename)"+
				");";

            connection.createStatement().executeUpdate(publishedforconsole);			

		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}

	}

	private void readInData(String data){
		String path = data;

		BufferedReader in = null;
		
		try {
			in = new BufferedReader((new FileReader(path)));

			String line = in.readLine();
			while (line != null) {
				PreparedStatement populateVideoGames = connection.prepareStatement(line);		
				line = in.readLine();		
				populateVideoGames.executeUpdate();
				populateVideoGames.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}   
		
	}
}
