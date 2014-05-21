---
-- #%L
-- Tesora Inc.
-- Database Virtualization Engine
-- %%
-- Copyright (C) 2011 - 2014 Tesora Inc.
-- %%
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License, version 3,
-- as published by the Free Software Foundation.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
-- 
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.
-- #L%
---
CREATE PERSISTENT SITE `dtsite0` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT SITE `dtsite1` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT SITE `dtsite2` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT SITE `dtsite3` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT GROUP `dtgroup` ADD dtsite0, dtsite1, dtsite2, dtsite3;
CREATE DATABASE `dtdb` DEFAULT COLLATE = latin1_swedish_ci DEFAULT PERSISTENT GROUP dtgroup;
USE `dtdb`;
CREATE RANGE `r_nnn` ( integer, integer, integer ) PERSISTENT GROUP dtgroup;
CREATE RANGE `r_iinni` ( int, int, integer, integer, int ) PERSISTENT GROUP dtgroup;
CREATE TABLE `t0` ( dvix int, rix int, c0 integer , c1 tinyint , c2 integer , c3 integer ) RANGE DISTRIBUTE ON ( c0, c3, c2 ) USING `r_nnn`;
CREATE TABLE `t1` ( dvix int, rix int, c0 integer , c1 tinyint , c2 integer , c3 varchar (18) collate latin1_swedish_ci , c4 integer ) RANGE DISTRIBUTE ON ( c0, c4, c2 ) USING `r_nnn`;
CREATE TABLE `t2` ( dvix int, rix int, c0 binary (35) , c1 integer , c2 char (26) collate latin1_swedish_ci , c3 integer , c4 integer ) RANGE DISTRIBUTE ON ( c1, c4, c3 ) USING `r_nnn`;
INSERT INTO `t0` VALUES 
  ( 2, 0, 2132025468, 101, 1789559538, 38273273),
  ( 21, 1, 1847617265, -121, 861199598, 1854160908),
  ( 5, 2, 2050384226, 108, 1666496038, 801947246),
  ( 18, 3, 1927140689, -66, 1266306207, 1439409672),
  ( 6, 4, 2050086330, 29, 548300895, 2033986122);
INSERT INTO `t1` VALUES 
  ( 10, 0, 2032753248, -37, 47042915, 'nU2CThJxpvqNJOuG#O', 1433748001),
  ( 26, 1, 1753700599, -7, 220601201, 'GfPKgjn=z:yFf$xHuy', 392062344),
  ( 22, 2, 1832019796, -18, 1711424533, 'Ml?WSZvKX.ozB:85<P', 759286936),
  ( 8, 3, 2037852978, 65, 692200153, 'f .>E?9VvnL4TR_ezH', 997118376),
  ( 29, 4, 1685036741, -51, 34427083, '!:QWlPOeE8THMSYC&M', 414381490);
INSERT INTO `t2` VALUES 
  ( 1, 0, '55229642623399154142985267547912736', 2135598954, 'O@<{dWrkBt1Wh|oD NCQx1NJwK', 1113855199, 2067553365),
  ( 5, 1, '20779581796533298576075390282964142', 2050384226, 'iRCSru8ipnsKdZ[[btDKAg5Smq', 1666496038, 801947246),
  ( 2, 2, '64306429833878353787938428238234787', 2132025468, 'r&1oC2F2Dc:{$buU0zsc9MvcmN', 1789559538, 38273273),
  ( 4, 3, '92201042676897855303545538765516966', 2087338832, 'NS;zIsewdfILZXj2u4RdjG6rr5', 1291792733, 682586272),
  ( 10, 4, '47326607977136381277427214005131084', 2032753248, 'Eu6V4piBThAtQW(z5s6n2kb6JI', 47042915, 1433748001);
SELECT @dve_sitename, dvix, rix, 't0' from `t0`;
SELECT @dve_sitename, dvix, rix, 't1' from `t1`;
SELECT @dve_sitename, dvix, rix, 't2' from `t2`;
CREATE PERSISTENT SITE `dtsite4` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT SITE `dtsite5` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT SITE `dtsite6` url= 'jdbc:mysql://localhost:3305';
CREATE PERSISTENT SITE `dtsite7` url= 'jdbc:mysql://localhost:3305';
ALTER PERSISTENT GROUP `dtgroup` ADD GENERATION dtsite4, dtsite5, dtsite6, dtsite7;
INSERT INTO `t0` VALUES 
  ( 59, 300, 1282838150, 29, 1255742893, 613079165),
  ( 54, 301, 1346167219, -98, 249108185, 1028278547),
  ( 51, 302, 1375601003, -90, 1107415166, 392523611),
  ( 45, 303, 1466342562, 21, 934483570, 828492208),
  ( 38, 304, 1592635657, -37, 660299877, 184409554);
INSERT INTO `t1` VALUES 
  ( 55, 300, 1321736609, 120, 370022392, 'LuEVD|ciqp_75Eui:H', 1605995205),
  ( 40, 301, 1484812602, 112, 1001829015, '7g0GOJv{x45<G{YTlI', 466325520),
  ( 43, 302, 1475509055, 103, 171976413, 'tT4.oTzgVsNP]N,n+H', 719298857),
  ( 40, 303, 1484812602, -71, 1001829015, '9Y.x(BfFeq7rDofsar', 466325520),
  ( 44, 304, 1468374273, -110, 212945082, 'acIHqvSnIBOJvDFMj0', 95446418);
INSERT INTO `t2` VALUES 
  ( 55, 300, '94570444665329329775123314842093642', 1321736609, '#WwX8uCXbhtX:8ZSt;31YtnZHT', 370022392, 1605995205),
  ( 57, 301, '82824622996185137168511625346054438', 1314797785, '6ad#3A.LE?eALVW*fH6l8KjM69', 1817171321, 1212325100),
  ( 59, 302, '91231234548471837293548878824404549', 1282838150, '7_avjQMdADoJ&Oc$9#B5QYXwu<', 1255742893, 613079165),
  ( 11, 303, '24565479800008581706476954505807466', 2015403858, 'Owz$iBLefDx15QZzKRDyE#9GLb', 762068515, 1355279645),
  ( 41, 304, '18096011127144220585690692063144945', 1484746221, 'qQy9fqSC=B1/DMRAXIH X*hEQy', 1385177317, 418024656);
SELECT @dve_sitename, dvix, rix, 't0' from `t0`;
SELECT @dve_sitename, dvix, rix, 't1' from `t1`;
SELECT @dve_sitename, dvix, rix, 't2' from `t2`;

DROP TABLE `t0`;
DROP TABLE `t1`;
DROP TABLE `t2`;
DROP RANGE `r_nnn`; 
DROP RANGE `r_iinni`; 
DROP DATABASE `dtdb`; 
DROP PERSISTENT GROUP `dtgroup`; 
DROP PERSISTENT SITE `dtsite5`; 
DROP PERSISTENT SITE `dtsite6`; 
DROP PERSISTENT SITE `dtsite7`; 
DROP PERSISTENT SITE `dtsite4`; 
DROP PERSISTENT SITE `dtsite3`; 
DROP PERSISTENT SITE `dtsite2`; 
DROP PERSISTENT SITE `dtsite1`; 
DROP PERSISTENT SITE `dtsite0`; 


