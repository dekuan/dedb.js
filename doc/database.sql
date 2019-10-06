#
#       create database
#
create database dedb;


#
#       grant privileges
#

#	for MySQL 8.0
CREATE USER 'dedb'@'localhost' IDENTIFIED BY 'dedb';
GRANT ALL PRIVILEGES ON dedb.* TO 'dedb'@'localhost';
FLUSH PRIVILEGES;

ALTER USER 'dedb'@'localhost' IDENTIFIED WITH mysql_native_password BY 'dedb';
FLUSH PRIVILEGES;


#	for older vesions
grant ALL privileges on dedb.* to 'dedb'@'localhost' identified by 'dedb';
FLUSH PRIVILEGES;
