CREATE DATABASE cljdbc;
CREATE USER 'cljdbc'@'%' IDENTIFIED BY 'cljdbc';
GRANT ALL PRIVILEGES ON cljdbc.* TO 'cljdbc'@'%';
