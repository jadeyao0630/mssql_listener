
using Microsoft.IdentityModel.Tokens;
using mssql_listener;

using System.Timers;

var parser = new IniFileParser();
var iniData = parser.Parse("config.ini");
var Mssql = "MsSqlServer";
var Mysql = "MySqlServer";
/*
foreach (var section in iniData)
{
    Console.WriteLine($"Section: {section.Key}");
    foreach (var key in section.Value)
    {
        Console.WriteLine($"  {key.Key} = {key.Value}");
    }
}
*/


if (iniData.ContainsKey(Mssql) && iniData.ContainsKey(Mysql))
{
    var server = iniData[Mssql];
    var server_mysql = iniData[Mysql];
    if (server!=null && 
        server.ContainsKey("Server") &&
        server.ContainsKey("Database") &&
        server.ContainsKey("User") &&
        server.ContainsKey("Password") &&
        server_mysql != null &&
        server_mysql.ContainsKey("Server") &&
        server_mysql.ContainsKey("Database") &&
        server_mysql.ContainsKey("User") &&
        server_mysql.ContainsKey("Password"))
    {
        var port = server.ContainsKey("Port") ? server["Port"] : "1433";
        var port_mysql = server.ContainsKey("Port") ? server_mysql["Port"] : "3306";
        //var data = $"server={server["Server"]},{port};Database={server["Database"]};User Id={server["User"]};Password={server["Password"]};Trusted_Connection=True";
        DatabaseChangeNotification notifier = new DatabaseChangeNotification(new MsSqlServerData {
            server=server["Server"],
            port= int.Parse(port),
            database = server["Database"],
            user = server["User"],
            password = server["Password"]

        },new MySqlServerData
        {
            server = server_mysql["Server"],
            port = int.Parse(port_mysql),
            database = server_mysql["Database"],
            user = server_mysql["User"],
            password = server_mysql["Password"]
        });
        if (iniData.ContainsKey("General"))
        {
            var general = iniData["General"];
            if (general.ContainsKey("Tables") && !general["Tables"].IsNullOrEmpty())
            {
                notifier.StartListening(general["Tables"].Split(','));
            }
            else
            {
                Console.WriteLine("tables have not been defineded...");
            }
            
        }

    }
}

//TrackingCT rrackingCT= new TrackingCT();
//rrackingCT.run();
//new ChangeTracker();