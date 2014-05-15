insert into user_table (table_id, name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
select 100, "t1", dist.id, sg.persistent_group_id, db.user_database_id, "InnoDB", "BASE TABLE"
from dve_catalog.distribution_model dist, dve_catalog.persistent_group sg, dve_catalog.user_database db
where dist.name = "Random" and sg.name = "DefaultGroup" and db.name = "TestDB";

insert into user_table (table_id, name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type)
select 200, "t2", dist.id, sg.persistent_group_id, db.user_database_id, "InnoDB", "BASE TABLE"
from dve_catalog.distribution_model dist, dve_catalog.persistent_group sg, dve_catalog.user_database db
where dist.name = "Broadcast" and sg.name = "DefaultGroup" and db.name = "TestDB";

insert into user_column 
(name, data_type, native_type_name, user_table_id, size, hash_position, prec, scale, nullable, has_default_value, default_value_is_constant, auto_generated, default_value, order_in_table, on_update, cdv)
values 
("a", 4, "integer", 100,  4, 1, 10, 0, 0, 0, 1, 1, null, 0, 0, 0),
("b", 4, "integer", 100,  4, 2, 10, 0, 0, 0, 1, 1, null, 1, 0, 0),
("c", 4, "integer", 100,  4, 0, 10, 0, 1, 0, 1, 0, null, 2, 0, 0),
("d", -6, "tinyint", 100,  4, 0, 10, 0, 1, 0, 1, 0, null, 3, 0, 0),

("a", 4, "integer", 200,  4, 1, 10, 0, 0, 0, 1, 1, null, 0, 0, 0),
("b", 4, "integer", 200,  4, 2, 10, 0, 0, 0, 1, 1, null, 1, 0, 0),
("p", -6, "tinyint", 200,  4, 0, 10, 0, 1, 0, 1, 0, null, 2, 0, 0),
("q", 4, "integer", 200,  4, 0, 10, 0, 1, 0, 1, 0, null, 3, 0, 0);
