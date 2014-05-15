CREATE PERSISTENT SITE `dtsite0` url= 'jdbc:mysql://localhost:3307'; 
CREATE PERSISTENT GROUP `dtgroup` ADD dtsite0; 
CREATE DATABASE `dtdb` DEFAULT COLLATE = latin1_swedish_ci DEFAULT PERSISTENT GROUP dtgroup; 
USE `dtdb`; 
CREATE RANGE `r_nnni` ( integer, integer, integer, int ) PERSISTENT GROUP dtgroup; 
CREATE TABLE `t0` ( dvix int, rix int, c0 integer , c1 binary (42) , c2 integer , c3 integer , c4 int ) RANGE DISTRIBUTE ON ( c0, c3, c2, c4 ) USING `r_nnni`; 
CREATE TABLE `t1` ( dvix int, rix int, c0 integer , c1 int , c2 integer , c3 integer ) RANGE DISTRIBUTE ON ( c0, c3, c2, c1 ) USING `r_nnni`; 
INSERT INTO `t0` VALUES ( 4, 0, 2114738097, 'dcbbhcheihjdjccjggdaghhbicchghdjjecjjafhhf', 1469834481, 2007905771, 822890675); 
INSERT INTO `t1` VALUES ( 29, 0, 1642548899, 1472713773, 260152959, 1501252996); 
CREATE PERSISTENT SITE `dtsite1` url= 'jdbc:mysql://localhost:3307'; 
ALTER PERSISTENT GROUP `dtgroup` ADD GENERATION dtsite1; 

DROP TABLE `t0`;
DROP TABLE `t1`;
DROP RANGE `r_nnni`;
DROP DATABASE `dtdb`;
DROP PERSISTENT GROUP `dtgroup`;
DROP PERSISTENT SITE `dtsite0`; 
DROP PERSISTENT SITE `dtsite1`; 
