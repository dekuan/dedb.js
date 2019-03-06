# dedb.js
Database connections in Node.js by DeKuan, Inc.

<br />


# Initialization for testing

#### CREATE DATABASE

```
> CREATE DATABASE dedb;
```

#### CREATE USER

```
> CREATE USER 'dedb'@'localhost' IDENTIFIED WITH mysql_native_password BY 'dedb';
> FLUSH PRIVILEGES;
```

#### GRANT PRIVILEGES

```
> GRANT ALL PRIVILEGES ON dedb.* TO 'dedb'@'localhost';
> FLUSH PRIVILEGES;
```



