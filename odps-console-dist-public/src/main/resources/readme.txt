Usage: odpscmd [OPTION]...
where options include:
    --help                                  (-h)for help
    --config=<config_file>                  specify another config file
    --project=<prj_name>                    use project
    --endpoint=<http://host:port>           set endpoint
    -k <n>                                  will skip begining queries and start from specified position
    -r <n>                                  set retry times
    -f <"file_path;">                       execute command in file
    -e <"command;[command;]...">            execute command, include sql command    
where -e option  include:
    -e "<sql>;"                                                execute sql
    -e "quit;"                                                 quit
    -e "list projects;"                                        list projects
    -e "use <prj_name> [with-settings];"                       open project
    -e "read  <table_name> [<(col_name>[,..])][PARTITION <(partition_spec)>][line_num];"
                                                               read data from table
    -e "add file <local_file> [as alias] [comment 'cmt'][-f];"   
                                                               add file
    -e "add archive <local_file> [as alias] [comment 'cmt'][-f];"
                                                               add archive
    -e "add table <table_name> [partition (spec)] [as alias] [comment 'cmt'][-f];"
                                                               add table
    -e "add jar <local_file.jar> [comment 'cmt'][-f];"
                                                               add jar
    -e "get resource [<project name>:]<resource name> <path>;" download a file resource from server to path.
                                                               
    -e "create function <fun_name> as '<classname>' using '<res>,...';"
                                                               create function
    -e "drop resource <res_name>;"                             drop resource
    -e "drop function <fun_name>;"                             drop function
    -e "list resources;"                                       list resources
    -e "list functions;"                                       list functions
    -e "show p  [from startdate to enddate] [number];"         show instances, date format: eg. 2012-08-27
    -e "kill [-sync] <instanceid>;"                                    stop instance
    -e "set <key>=<value>;"                                    set config value
    -e "alias <key>=<value>;"                                  alias config value
    -e "show flags;"                                           show set & alias config
    -e "status <instanceid>;"                                  get instance status
    -e "show securityconfiguration;"                         show the access control config of the project
    -e "list users|roles;"                                   list all users or roles 
    -e "create|drop role <name>;"                            create or drop role 
    -e "add|remove user <name>;"                             add or remove user
    -e "describe|desc role <rolename>;"                           desc the role
    -e "grant <rolenamelist> to <username>;"                 
    -e "revoke <rolenamelist> from <username>;"
    -e "grant <privlist> on <objecttype> <objectname> to user|role <name>;"
    -e "revoke <privlist> on <objecttype> <objectname> from user|role <name>;"
    -e "grant super <privlist> to user <username>"
    -e "revoke super <privlist> from user <username>"
    -e "show grants [for <username>] [on type <objecttype>];"  
    -e "show acl for <objectname> [on type <objecttype>];"   
    -e "put policy <local_file> [on role <rolename>];"        e.g put policy e:\policy.txt
    -e "get policy [on role <rolename>];"                               
    -e "set checkpermissionusingacl|checkpermissionusingpolicy|objectcreatorhasaccesspermission
            |objectcreatorhasgrantpermission|projectprotection=true|false;"
    -e "set labelsecurity=true|false;"
    -e "set label <num> to user <username>;"
    -e "set label <num> to table <tablename>[column_list];"
    -e "grant label <num> on table <tablename>[column_list] to user <username> [with exp <days>];"
    -e "revoke label on table <tablename>[column_list] from user <username>;"
    -e "clear expired grants;"
    -e "show label [level] grants [on table <tablename>] [for user <username>];"
    -e "set projectprotection=true|false with exception <policyfile>;"    
    -e "add|remove trustedproject <projectname>;"
    -e "list trustedprojects;"
    -e "create|drop package <package name>;"                                      Create or drop a package
    -e "add <resource type> <resource name> to package <package name>;"           Add a resource to the resource list of the package
    -e "desc|describe_resource <project name>.<resource name>;"                   Describe a resource
    -e "remove <resource type> <resource name> from package <package name>;"      Remove a resource from the resource list of the package
    -e "allow|disallow project <project name> to install package <package name> [using label <num>];" Allow or disallow a project to install the package
    -e "show packages;"                                                           Show created package list and installed package list
    -e "desc package [<project name>.]<package name>;"                            Display the details of the package, include resource list
    -e "install|uninstall package <project name>.<package name>;"                 Install or uninstall a package
    -e "tunnel upload <file> <table>;"                                            Tunnel Command, could find usage in ODPS Documents.
    -e "tunnel download <table> <file>;"
    -e "export table <tablename>;"
    -e "export <projectname> <local_path>  [-rftpd];"
    -e "describe|desc [extended] [<project name>.[<schema name>.]]<table name> [partition(<partition spec>)]"
    -e "show version"
