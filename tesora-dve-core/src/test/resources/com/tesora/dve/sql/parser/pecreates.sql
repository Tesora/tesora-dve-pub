create persistent site site1 'jdbc:mysql://s1/db1';
create persistent site site2 'jdbc:mysql://s2/db1';
create persistent group g1 add site1, site2;
create database if not exists mydb;
