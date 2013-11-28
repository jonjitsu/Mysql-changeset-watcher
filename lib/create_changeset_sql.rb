#!/usr/bin/ruby
=begin
Requires:
Gems:
mysql
getopt

packages:
libmysqlclient-dev


=end

require 'mysql'
require 'getopt/long'

opt = Getopt::Long.getopts(
  ["--create-triggers","-c", Getopt::BOOLEAN ],
  ["--delete-triggers","-d", Getopt::BOOLEAN ],
  ["--host","-h", Getopt::OPTIONAL ], # assumes localhost
  ["--username","-u", Getopt::REQUIRED ],
  ["--password","-p", Getopt::OPTIONAL ],
  ["--database","-b", Getopt::REQUIRED ]
)

db_host = 'localhost'
if opt['h']
  db_host=opt['h']
end
if opt['b']
  db=opt['b']
else
  puts "Missing database!"
  exit 2
end

if opt['u']
  username = opt['u']
else
  puts "Missing username!"
  exit 1
end
if opt['p']
  password = opt['p']
else
  puts "Password:\n"
  password = STDIN.gets.chomp
end

def create_triggers_sql(tbl, fields, delimiter)# create trigger templates
  create_trigger_template = 
  "\nCREATE TRIGGER dbcs%s_%s 
      AFTER %s ON `%s` FOR EACH ROW
      BEGIN
        INSERT INTO `db_change_set` (`sql_statement`) VALUES (%s);
      END#{delimiter}\n"
  ['INSERT', 'UPDATE', 'DELETE'].each do |type|
    case type
    when 'INSERT'
      column_parts = fields.map { |f| "`#{f.name}`" }
      value_parts = fields.map { |f| escape_it(f, 'NEW') }
      sql = "CONCAT('INSERT INTO `#{tbl}` (" + column_parts.join(', ') + ")\n"
      sql+= "VALUES (', " + value_parts.join(', \', \', ') + ", ');')\n"
    when 'UPDATE'
      set_parts = fields.map { |f| "'`#{f.name}`=', " + escape_it(f, 'NEW')}
      where_parts = fields.map { |f| "'`#{f.name}`=', " +  escape_it(f, 'OLD')}
      sql = "CONCAT('UPDATE `#{tbl}` set '," + set_parts.join(', \', \', ') + "\n"
      sql+= ", ' WHERE ', " + where_parts.join(', \', \', ') + ")\n"
    when 'DELETE'
      where_parts = fields.map { |f| "'`#{f.name}`=', " +  escape_it(f, 'OLD')}
      sql = "CONCAT('DELETE FROM `#{tbl}` "
      sql+= "WHERE ', " + where_parts.join(', \', \', ') + ")\n"
    end
    printf(create_trigger_template, type[0].downcase,tbl, type, tbl, sql)
  end  
end
def escape_it(field, prefix)
  [ Mysql::Field::TYPE_STRING, Mysql::Field::TYPE_BLOB, Mysql::Field::TYPE_TIME, Mysql::Field::TYPE_CHAR,
    Mysql::Field::TYPE_TIMESTAMP, Mysql::Field::TYPE_DATE, Mysql::Field::TYPE_DATETIME, Mysql::Field::TYPE_VAR_STRING,
    Mysql::Field::TYPE_YEAR, Mysql::Field::TYPE_SET, Mysql::Field::TYPE_ENUM ]
    .include?(field.type) ?
      "\"'\", #{prefix}.`#{field.name}`, \"'\"" :
      "#{prefix}.`#{field.name}`" 
end

def delete_triggers_sql(tbl, fields, delimiter)
  ['INSERT', 'UPDATE', 'DELETE'].each do |type|
    printf("\nDROP TRIGGER IF EXISTS #{tbl}.dbcs%s_#{tbl} #{delimiter}\n", type[0].downcase)
  end
end


tbl_processors=[]
if opt['d']
  tbl_processors.push(method(:delete_triggers_sql))
end
if opt['c']
  tbl_processors.push(method(:create_triggers_sql))
end


my = Mysql.new(db_host, username, password, db)

delimiter = "$$"
printf ("DELIMITER #{delimiter}\nUSE #{db} #{delimiter}")
printf("CREATE TABLE IF NOT EXISTS `db_change_set` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `sql_statement` longtext,
  `applied_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=latin1#{delimiter}")

change_set_table_name = 'db_change_set'
tbl_processors.each do |action|
  #puts action
  my.list_tables
    .select { |t| t!=change_set_table_name }
    .each do |tbl| 
    fields = my.list_fields(tbl).fetch_fields
    action.call(tbl,fields, delimiter)
  end
end