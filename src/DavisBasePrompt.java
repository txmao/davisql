import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class DavisBasePrompt {
	
	static String prompt = "davisql> ";
	static String version = "v-s17-1.0";
	static String copyright = "©Spring-2017 mdy";
	static boolean isExit = false;
	static int pageSize = 512; 
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
    public static void main(String[] args) {
    	/* Check for initialization */
    	Table.initilization();
		/* Display the welcome screen */
		splashScreen();
		/* Variable to collect user input from the prompt */
		String userCommand = "";
		while (!isExit) {
			System.out.print(prompt);
			/* case insensitive, \n and \r ignored */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Bye...");
	}
    
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to davisql"); // Display the string.
		System.out.println("davisql Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
		/**
		 *  Help: Display supported commands
		 */
	
		public static void help() {
			System.out.println(line("*",89));
			System.out.println("SUPPORTED COMMANDS");
			System.out.println("All commands below are case insensitive");
			System.out.println();
			System.out.println("\tSHOW TABLES;                                               Display a list of all tables names in the database");
			System.out.println("\tCREATE TABLE table_name (<column_name datatype>);          Create a new table in the database.");
			System.out.println("\tINSERT INTO table_name VALUES (value1,value2,..);          Insert a new record into the table.");
			System.out.println("\tDELETE FROM TABLE table_name WHERE condition;              Delete records based on selection condition.");
			System.out.println("\tUPDATE table_name SET column_name = value WHERE condition; Modifies the records in the table.");
			System.out.println("\tSELECT * FROM table_name;                                  Display all records in the table.");
			System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;            Display records whose rowid is <id>.");
			System.out.println("\tSELECT column_name1,column_name2,... (WHERE ...);          Display specified columns from a table in a query");
			System.out.println("\tDROP TABLE table_name;                                     Remove table data and its schema.");
			System.out.println("\tVERSION;                                                   Show the program version.");
			System.out.println("\tINIT;                                                      Initialize the database");
			System.out.println("\tHELP;                                                      Show this help information");
			System.out.println("\tEXIT;                                                      Exit the program");
			System.out.println();
			System.out.println();
			System.out.println(line("*",89));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
	
	public static void parseUserCommand (String userCommand) {
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement
		 */
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "init":
				parseInitCommand();
				break;
			case "show":
				if (commandTokens.size() == 2) {
					if (commandTokens.get(1).equals("tables")) {
						parseQueryString("select table_name from davisbase_tables");
					} else {
						System.out.println("show what?");
					}
					break;
				} else {
					System.out.println("show what?");
					break;
				}
			case "select":
				parseQueryString(userCommand);
				break;
			case "drop":
				System.out.println("STUB: Calling your method to drop items");
				dropTable(userCommand);
				break;
			case "create":
				parseCreateString(userCommand);
				break;
			case "delete":
				parseDeleteString(userCommand);
				break;
			case "update":
				parseUpdateString(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
				break;
			case "insert":
				parseInsertCommand(userCommand);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	
	private static void parseInitCommand() {
		/* iteratively delete all files */
		try {
			File data = new File("data");
			deleteDATA(data);
			Table.initilization();
		} catch (Exception e) {
			System.out.println("init error: "+e);
		}
	}
	
	private static void deleteDATA (File element) {
		if (element.isDirectory()) {
			for (File sub : element.listFiles()) {
				deleteDATA(sub);
			}
		}
		element.delete();
	}
	
	private static void parseUpdateString(String userCommand) {
		//update table_name set column_name = value where [condition]
		System.out.println("STUB: Calling parseUpdateString(String s) to process queries");
		System.out.println("Parsing the string:\"" + userCommand + "\"");
		String[] updates = userCommand.toLowerCase().split("set");
		String[] table = updates[0].trim().split(" ");
		String tablename = table[1].trim();
		String set_value=null;
		String where=null;
		if (!TableExists(tablename)) {
			System.out.println("Table does not Exist");
			return;
		}
		
		if (updates[1].contains("where")) {
			String[] findupdate = updates[1].split("where");
			set_value = findupdate[0].trim();
			where = findupdate[1].trim();
			Table.update(tablename, parsewhereString(set_value), parsewhereString(where));
		} else {
			set_value=updates[1].trim();
			String[] no_where = new String[0];
			Table.update(tablename, parsewhereString(set_value), no_where);
		}
		
	}
	
	private static void parseDeleteString(String deleteString) {
		//delete based on key (row_id)
		System.out.println("STUB: Calling parseDeleteString(String s) to process queries");
		System.out.println("Parsing the string:\"" + deleteString + "\"");
		String[] delete = deleteString.split("where");
		String[] table = delete[0].trim().split("from");
		String tableName = table[1].trim();
		String[] cond = parsewhereString(delete[1]);

		if(!TableExists(tableName))
		{
			System.out.println("Table doesnot Exists!");
			return;
		}
		Table.delete(tableName+".tbl",cond);
	}
	
	private static void parseInsertCommand(String userCommand) {
		System.out.println("STUB: Calling parseInsertString(String s) to process queries");
		System.out.println("Parsing the string:\"" + userCommand + "\"");
		
		String[] tokens = userCommand.split(" ");
		
		String tableName = tokens[2];
		String values = userCommand.split("values")[1].trim();
		values = values.substring(1, values.length()-1);
		String[] values_to_insert = values.split(",");
		for(int i=0;i<values_to_insert.length;i++) {
			values_to_insert[i] = values_to_insert[i].trim();
		}
		
		if(!TableExists(tableName)) {
			System.out.println("Table does not exists!");
			System.out.println();
		} else {
			Table.insert(tableName+".tbl",values_to_insert);
		}
		
	}

	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		String[] drop=dropTableString.split(" ");
		
		String tableName=drop[2].trim();
		if(!TableExists(tableName))
		{
			System.out.println("Table does not exists!");
			return;
		}
		else
		{
			Table.drop(tableName);	
		}
	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 */
	public static void parseQueryString(String queryString) {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		//eg. select table_name from davisbase_columns where rowid = 1
		String[] select_cmd = queryString.split("where");
		String[] comp;
		
		if (select_cmd.length>1) {
			comp=parsewhereString(select_cmd[1]);
		} else {
			comp=new String[0];
		}
		
		String[] stringQuery=select_cmd[0].split("from");
		String tableName = stringQuery[1].trim();
		
		String[] columnNames =stringQuery[0].split(" ");
		if (columnNames[1].equals("*")) {
			String[] columns = new String[]{"*"};
			if(!TableExists(tableName)){
				System.out.println("davisbase error! Table does not exists.");
				System.out.println();
			} else {
				Table.SelectQuery(columns,tableName,comp);
			}
		} else {
			String[] columns=columnNames[1].split(",");
			for(int i=0;i<columns.length;i++){
				columns[i]=columns[i].trim();
			}
			if(!TableExists(tableName)){
				System.out.println("davisbase error! Table does not exists.");
				System.out.println();
			}else{
				Table.SelectQuery(columns,tableName,comp);
			}
		}
	}
	
	private static String[] parsewhereString(String where) {
		// TODO Auto-generated method stub
		// column_name = value
		String[] comp = new String[3];
		String[] arr = new String[2];
		
		if(where.contains("=")){
			arr = where.split("=");
			comp[0]=arr[0].trim();
			comp[1]="=";
			comp[2]=arr[1].trim();
		}
		else if(where.contains(">")){
			arr = where.split(">");
			comp[0]=arr[0].trim();
			comp[1]=">";
			comp[2]=arr[1].trim();
		}
		else if(where.contains("<")){
			arr = where.split("<");
			comp[0]=arr[0].trim();
			comp[1]="<";
			comp[2]=arr[1].trim();
		}
		else if(where.contains(">=")){
			arr = where.split(">=");
			comp[0]=arr[0].trim();
			comp[1]=">=";
			comp[2]=arr[1].trim();
		}
		else if(where.contains("<=")){
			arr = where.split("<=");
			comp[0]=arr[0].trim();
			comp[1]="<=";
			comp[2]=arr[1].trim();
		}
		else if(where.contains("<>")){
			arr = where.split("<>");
			comp[0]=arr[0].trim();
			comp[1]="<>";
			comp[2]=arr[1].trim();
		}
		// {"column_name", "=", "value"}
		return comp;
	}

	private static boolean TableExists(String tableName) {
		// TODO Auto-generated method stub
		String filename =tableName+".tbl";
		File catalog = new File("data/catalog");
		String[] tablenames = catalog.list();
		for (String string : tablenames) {
			if(filename.equals(string))
				return true;
		}
		File userdata = new File("data/user_data");
		String[] tables = userdata.list();
		for (String string : tables) {
			if(filename.equals(string))
				return true;
		}	
		return false;
	}
	
	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 */
	public static void parseCreateString(String createTableString) {
		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
		/* Define table file name */
		String tableFileName = createTableTokens.get(2) + ".tbl";
		if(TableExists(createTableTokens.get(2))){
			System.out.println("Table Already exists!Please specify some other name");
		}else{
			String[] columnNames = createTableString.replaceAll("\\(", "").replaceAll("\\)", "").split(createTableTokens.get(2));
			columnNames=columnNames[1].trim().split(",");
			for(int i=0;i<columnNames.length;i++)
				columnNames[i]=columnNames[i].trim();
			
			try {
				RandomAccessFile tableFile = new RandomAccessFile("data/user_data/"+tableFileName, "rw");
				Table.createTable(tableFile,createTableTokens.get(2),columnNames);
				tableFile.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
