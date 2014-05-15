CREATE PERSISTENT SITE `dtsite0` url= 'jdbc:mysql://localhost:3307';
CREATE PERSISTENT GROUP `dtgroup` ADD dtsite0;
CREATE DATABASE `dtdb` DEFAULT COLLATE = latin1_swedish_ci DEFAULT PERSISTENT GROUP dtgroup;
USE `dtdb`;
CREATE RANGE `r_innb` ( int, integer, integer, binary(47) ) PERSISTENT GROUP dtgroup;
CREATE TABLE `t0` ( dvix int, rix int, c0 binary (28) , c1 integer , c2 datetime , c3 char (27) collate latin1_swedish_ci , c4 binary (47) , c5 char (28) collate latin1_swedish_ci , c6 int , c7 integer , c8 tinyint , c9 char (16) collate latin1_swedish_ci , c10 int , c11 int , c12 int , c13 varchar (9) collate latin1_swedish_ci , c14 datetime , c15 int , c16 integer ) RANGE DISTRIBUTE ON ( c6, c16, c1, c4 ) USING `r_innb`;
INSERT INTO `t0` VALUES ( 5, 0, 'ggchgbfjaabchfhghchehajejehd', 1984800797, '2002-05-27 10:18:28', 'myhfzqzeimmfpnlgpoxmxacmqgq', 'iicjgfgficajhhgjaegbbibjjfceagdigfhccfhaiabhhhg', 'ommlykuglkfrusdahslnjamvxnzj', 2072364313, 1830459335, -27, 'brupeuzzamjgdgaj', 437110706, 1269273807, 1898632971, 'rylrniqak', '2017-04-20 19:02:46', 2073458547, 545003904);
CREATE PERSISTENT SITE `dtsite1` url= 'jdbc:mysql://localhost:3307';
ALTER PERSISTENT GROUP `dtgroup` ADD GENERATION dtsite1;
INSERT INTO `t0` VALUES ( 5, 0, 'ggchgbfjaabchfhghchehajejehd', 1984800797, '2002-05-27 10:18:28', 'myhfzqzeimmfpnlgpoxmxacmqgq', 'iicjgfgficajhhgjaegbbibjjfceagdigfhccfhaiabhhhg', 'ommlykuglkfrusdahslnjamvxnzj', 2072364313, 1830459335, -27, 'brupeuzzamjgdgaj', 437110706, 1269273807, 1898632971, 'rylrniqak', '2017-04-20 19:02:46', 2073458547, 545003904);

DROP TABLE `t0`;
DROP RANGE `r_innb`;
DROP DATABASE `dtdb`;
DROP PERSISTENT GROUP `dtgroup`;
DROP PERSISTENT SITE `dtsite0`; 
DROP PERSISTENT SITE `dtsite1`; 
