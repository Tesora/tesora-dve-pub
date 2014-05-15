insert into user_table (table_id, name, distribution_model_id, persistent_group_id, user_database_id, engine, table_type) values (100, "alltypes", 1, 3, 3, 'InnoDB', 'BASE TABLE');

insert into user_column (name, data_type, native_type_name, native_type_modifiers, user_table_id, has_default_value, default_value_is_constant, auto_generated, nullable, size, hash_position, prec, scale, order_in_table, on_update, cdv) 
values 
("bit_column", -7, "bit", null, 100, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0),
("tinyint_column", -6, "tinyint", null, 100, 0, 1, 0, 1, 3, 0, 0, 0, 1, 0, 0),
("bigint_column", -5, "bigint", null, 100, 0, 1, 0, 1, 19, 0, 0, 0, 2, 0, 0),
("longvarb_column", -4, "long varbinary", null, 100, 0, 1, 0, 1, 16777215, 0, 0, 0, 3, 0, 0),
("varb_column", -3, "varbinary", null, 100, 0, 1, 0, 1, 200, 0, 0, 0, 4, 0, 0),
("binary_column", -2, "binary", null, 100, 0, 1, 0, 1, 10, 0, 0, 0, 5, 0, 0),
("text_column", -1, "text", null, 100, 0, 1, 0, 1, 2147483647, 0, 0, 0, 6, 0, 0),
("char_column", 1, "char", null, 100, 0, 1, 0, 1, 10, 0, 0, 0, 7, 0, 0),
("num_column", 2, "numeric", null, 100, 0, 1, 0, 1, 65, 0, 10, 2, 8, 0, 0),
("dec_column", 3, "decimal", null, 100, 0, 1, 0, 1, 65, 0, 5, 4, 9, 0, 0),
("int_column", 4, "integer", null, 100, 0, 1, 0, 1, 10, 0, 0, 0, 10, 0, 0),
("smallint_column", 5, "smallint", null, 100, 0, 1, 1, 0, 5, 0, 0, 0, 11, 0, 0),
("float_column", 7, "float", null, 100, 0, 1, 0, 1, 10, 0, 0, 0, 12, 0, 0),
("double_column", 8, "double", null, 100, 0, 1, 0, 1, 17, 0, 0, 0, 13, 0, 0),
("varchar_column", 12, "varchar", null, 100, 0, 1, 0, 1, 10, 0, 0, 0, 14, 0, 0),
("date_column", 91, "date", null, 100, 0, 1, 0, 1, 0, 0, 0, 0, 15, 0, 0),
("time_column", 92, "time", null, 100, 0, 1, 0, 1, 0, 0, 0, 0, 16, 0, 0),
("datetime_column", 93, "datetime", null, 100, 0, 1, 0, 1, 0, 0, 0, 0, 17, 0, 0),
("intu_column", 4, "integer", "unsigned", 100, 0, 1, 0, 1, 10, 0, 0, 0, 18, 0, 0),
("bigintu_column", -5, "bigint", "unsigned", 100, 0, 1, 0, 1, 19, 0, 0, 0, 19, 0, 0);

