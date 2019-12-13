/**
 * Контроль версии сущностей
 * PostgreSQL 11+
 *
 * Author: Lovpache Shumaf aka Soarex16
 */

--begin transaction
BEGIN;

/**
 * Полная очистка
 */
DO $remove_vc$
DECLARE
	vc_catalog name DEFAULT current_database(); --текущая база
	vc_schema name DEFAULT current_schema(); --схема
	
	tbl_name_row RECORD; --временная переменная для получения результата запроса
	source_table_name name; --имя таблицы-источника
	vc_table_name name; --имя архивной таблица
BEGIN
	RAISE NOTICE 'Removing version control from %...', vc_schema;
	
	/* итерируемся по таблицам, для каждой удаляем:
		- триггер
		- таблицу версий
		- вспомогательные атрибуты
	*/
	FOR tbl_name_row IN SELECT table_name FROM information_schema.tables
			   WHERE table_type = 'BASE TABLE' AND
			   		 table_schema = vc_schema AND
					 table_name NOT LIKE '#_#_vc#_#_%' ESCAPE '#'
	LOOP
		RAISE NOTICE 'Proccessing %', tbl_name_row.table_name;
		
		source_table_name := tbl_name_row.table_name;
		vc_table_name := '__vc__' || source_table_name;
		
		--удаляем триггерную функцию и связанные с ней триггеры (CASCADE)
		EXECUTE format('DROP FUNCTION IF EXISTS %I() CASCADE', '__vc__record_tracker_' || source_table_name);
		
		--удаляем архивную таблицу
		EXECUTE format('DROP TABLE IF EXISTS %I;', vc_table_name);
		
		--удаляем служебные атрибуты из исходной таблицы
		EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS __vc__snapshot_version;', source_table_name);
		EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS __vc__snapshot_timestamp;', source_table_name);
	END LOOP;
	
	--удаляем функции для манипуляции версионными данными
	EXECUTE format('DROP FUNCTION IF EXISTS get_constraint_name CASCADE');
	EXECUTE format('DROP FUNCTION IF EXISTS get_pk_columns CASCADE');
	EXECUTE format('DROP PROCEDURE IF EXISTS init_version_control CASCADE');
	EXECUTE format('DROP PROCEDURE IF EXISTS remove_version_control CASCADE');
	EXECUTE format('DROP FUNCTION IF EXISTS vc_get_version CASCADE');
	EXECUTE format('DROP FUNCTION IF EXISTS record_to_text_array CASCADE');
	EXECUTE format('DROP PROCEDURE IF EXISTS vc_restore_version CASCADE');
	EXECUTE format('DROP FUNCTION IF EXISTS jsonb_diff_val CASCADE');
	EXECUTE format('DROP FUNCTION IF EXISTS vc_generate_diff CASCADE');
	
	RAISE NOTICE 'Version control successfully removed';
END;
$remove_vc$ LANGUAGE plpgsql;

COMMIT;