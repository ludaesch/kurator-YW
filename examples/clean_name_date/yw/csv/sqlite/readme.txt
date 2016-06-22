SQLite download:
- https://www.sqlite.org/download.html
- install the binary version of SQLite for your platform (Mac OS X (x86)
	- sqlite-tools-osx-x86-3130000.zip
	- A bundle of command-line tools for managing SQLite database files, including the command-line shell program, the sqldiff program, and the sqlite3_analyzer program.
(sha1: 6ce3ccc7ed79eab2f95f4acefa2fa219920fafc9)

some basic hands-on skills for SQLite,
- Software Carpentry lessons at http://swcarpentry.github.io/sql-novice-survey/
- Command Line Shell For SQLite: https://www.sqlite.org/cli.html
- sqlite3 analyzer: https://www.sqlite.org/sqlanalyze.html
- SQLite FAQ: http://sqlite.org/faq.html#q11

$ sqlite3 demo.db
The SQLite command is sqlite3 and you are telling SQLite to open up the demo.db. You need to specify the .db file otherwise, SQLite will open up a temporary, empty database.

To get out of SQLite, type out .exit or .quit. For some terminals, Ctrl-D can also work. If you forget any SQLite . (dot) command, type .help.

sqlite> .tables 
to list the tables in the database.


rename a table: 
sqlite> ALTER TABLE old_table_name RENAME TO new_table_name;

add a new colun to the existing table:
sqlite> alter table table_name add column new_column_name new_column_type;

Drop the old table X 
sqlite> drop table X;

To to create a table from an existing table by copying the existing table's columns:
create table new_table as 
	select expressons
	from exisiting_tables
	[where conditions];

Copy information from one/more table(s) into another:
SELECT column_name(s)
INTO newtable
FROM table(s);
[LEFT JOIN othertable
ON condition];




