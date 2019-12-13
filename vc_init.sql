/**
 * Контроль версии сущностей
 * PostgreSQL 11+
 *
 * Author: Lovpache Shumaf aka Soarex16
 */

--begin transaction
BEGIN;

/**
 * Get name of PK constraint in relation
 *
 * @param rel_name идентификатор отношения
 * @param constraint_type тип ограничения целостности
 * Может принимать следующие значения:
 * c = check constraint
 * f = foreign key constraint
 * p = primary key constraint
 * u = unique constraint
 * t = constraint trigger
 * x = exclusion constraint
 */
CREATE OR REPLACE FUNCTION get_constraint_name(rel_name name, constraint_type char)
	RETURNS name
	AS $$
	SELECT conname FROM pg_constraint
	   WHERE conrelid = (rel_name::regclass)::oid AND
	   		 contype = constraint_type;
$$ LANGUAGE sql;


/**
 * Вспомогательная процедура, возвращающая набор идентификаторов полей первичного ключа
 *
 * @param rel_name идентификатор отношения
 */
CREATE OR REPLACE FUNCTION get_pk_columns(rel_name name)
	RETURNS TABLE(attrname name)
	AS $$
BEGIN
	RETURN QUERY SELECT attr.attname
	FROM pg_index idx
	JOIN pg_attribute attr ON attr.attrelid = idx.indrelid AND
						   attr.attnum = ANY(idx.indkey)
	WHERE idx.indrelid = rel_name::regclass
	AND idx.indisprimary;
END;
$$ LANGUAGE plpgsql;


/**
 * Процедура инициализации системы контроля версии сущностей для текущей схемы
 *
 * @param tables_blacklist массив с названиями таблиц, которые не включаются в контроль версий 
 * (только название отношения, не полное квалифицированное имя, т.е. bookings.aircrafts_data не валидное имя,
 * вместо него следует писать aircrafts_data)
 * (Важно: ввиду высокой сложности реализации не происходит контроль ограничений целостности, 
 * т.е. если таблица присутствует в списке исключений и связана с другими таблицами, то
 * ограничения целостности не соблюдаются).
 *
 * TODO: в идеале, можно было бы проверять, есть ли ссылки на таблицы из черного списка и кидать ошибку
 * @return true в случае успешной инициализации
 */
CREATE OR REPLACE PROCEDURE init_version_control(VARIADIC tables_blacklist name[] DEFAULT '{}') AS $init_vc$
DECLARE
	vc_catalog name DEFAULT current_database(); --текущая база
	vc_schema name DEFAULT current_schema(); --схема
	
	pk_columns_str varchar;
	pk_constraint_name name;
	
	equality_condition varchar; --для процедурной генерации условий проверки
	temp_str name;
	
	tbl_name_row RECORD; --временная переменная для получения результата запроса
	source_table_name name; --имя таблицы-источника
	vc_table_name name; --имя архивной таблица
BEGIN
	RAISE NOTICE 'Selected schema: %', vc_schema;
	
	RAISE NOTICE 'Tables in blacklist:';
	FOR tbl_name_row IN SELECT unnest(tables_blacklist)
	LOOP
		RAISE NOTICE '	%', tbl_name_row.unnest;
	END LOOP;
	
	/* итерируемся по таблицам, для каждой:
		- создаем таблицу контроля версий
		- добавляем служебные атрибуты к таблицам
		- навешиваем триггеры
	*/
	RAISE NOTICE 'Tables to be tracked by vesrion control:';
	FOR tbl_name_row IN SELECT table_name FROM information_schema.tables
			   WHERE table_type = 'BASE TABLE' AND
			   		 table_schema = vc_schema AND
					 table_name NOT IN (SELECT unnest(tables_blacklist))
	LOOP
		RAISE NOTICE '	%', tbl_name_row.table_name;
		
		/*Тут возникнет проблема с соблюдением всех ограничений ССЫЛОЧНОЙ целостноти.
		  Придется их модифицировать под условия версионности, т.е. если у отношения был PRIMARY KEY(поле1, ... ,полеN), 
		  то теперь будет PRIMARY KEY(поле1, ..., полеN, version). 
		  
		  Аналогичная ситуация с внешними ключами, каждое отношение, которое имеет внешние ключи надо преобразовывать 
		  и добавлять дополнительное поле чтобы обеспечить ссылку на конкретную версию */
		
		source_table_name := tbl_name_row.table_name;
		vc_table_name := '__vc__' || source_table_name;
		
		RAISE NOTICE 'Creating service table %', vc_table_name;
		EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL);', vc_table_name, source_table_name);
		
		-- версия записи
		EXECUTE format('ALTER TABLE %I ADD COLUMN __vc__snapshot_version INT NOT NULL DEFAULT 1;', vc_table_name);
		
		-- метка времени
		EXECUTE format('ALTER TABLE %I ADD COLUMN __vc__snapshot_timestamp TIMESTAMP NOT NULL DEFAULT NOW();', vc_table_name);
		
		--получаем аттрибуты, входящие в первичный ключ
		SELECT string_agg(attrname, ',') INTO pk_columns_str FROM get_pk_columns(vc_table_name);
		--получаем название ограничения целостности PK
		pk_constraint_name := get_constraint_name(vc_table_name, 'p');
		
		--удаляем старый первичный ключ в архивной таблице
		EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', vc_table_name, pk_constraint_name);
		
		--создаем новый PK с тем же названием
		EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I PRIMARY KEY(%s);', vc_table_name, pk_constraint_name, pk_columns_str || ',__vc__snapshot_version');	
		
		-- модифицируем исходную таблицу для хранения номера последней версии
		-- а может это вынести в отдельую таблицу? 50/50 С одной стороны не захламляем таблицу, с другой избыточность.
		EXECUTE format('ALTER TABLE %I ADD COLUMN __vc__snapshot_version INT NOT NULL DEFAULT 1;', source_table_name);
		EXECUTE format('ALTER TABLE %I ADD COLUMN __vc__snapshot_timestamp TIMESTAMP NOT NULL DEFAULT NOW();', source_table_name);
		
		--генериурем условие равенства строк для триггера
		equality_condition := '';
		FOR temp_str IN SELECT column_name FROM information_schema.columns
 						WHERE table_schema = current_schema() AND 
			  				  table_name = source_table_name AND
			  				  column_name NOT IN ('__vc__snapshot_version', '__vc__snapshot_timestamp')
		LOOP
			equality_condition = equality_condition || temp_str::varchar || ' = OLD.' || temp_str::varchar || ' AND ';
		END LOOP;
		
		equality_condition := trim(trailing ' AND' from equality_condition);
		
		-- создаем триггер на отслеживание изменений
		-- в PostgreSQL для создания триггера сначала необходимо создать т.н. триггерную функцию 
		EXECUTE format('CREATE OR REPLACE FUNCTION __vc__record_tracker_%1$I() RETURNS trigger AS $vc_record_tracker$
BEGIN				   
	IF (TG_OP = ''DELETE'') THEN
		--если пользователь хочет удалить сущность, то перемещаем последнюю версию в архивную таблицу и удаляем ее в исходной таблице
		RAISE NOTICE ''The tuple has been deleted from the source table and moved to the archive table.'';
		INSERT INTO %2$I SELECT OLD.*;
        
		RETURN OLD;
	ELSIF (TG_OP = ''UPDATE'') THEN
		--если кортеж уже есть в исходной таблице (т.е. версия не изменилась, а кто-то решил поменять служебные поля), 
		--то запрещаем изменение и не обновляем данные в архивной таблице				   
		IF EXISTS (SELECT 1 FROM %2$I WHERE %3$s) THEN
			RAISE NOTICE ''Entity attributes have not been changed'';
			RETURN OLD;
		END IF;
		
		--увеличиваем номер версии
		NEW.__vc__snapshot_version := OLD.__vc__snapshot_version + 1;
		NEW.__vc__snapshot_timestamp := now();
					   
		--записываем в архивную таблицу старую версию
		INSERT INTO %2$I SELECT OLD.*;
		
		RETURN NEW;
    ELSIF (TG_OP = ''INSERT'') THEN
		--на всякий случай
		NEW.__vc__snapshot_version := 1;
		NEW.__vc__snapshot_timestamp := now();
        
		RETURN NEW;
    END IF;
        RETURN NULL;
	
	RETURN NEW;
END;
$vc_record_tracker$ LANGUAGE plpgsql;

CREATE TRIGGER __vc__record_tracker_%1$I
--на delete стоит ли вешать? может переносим при удалении в архив и все? да, хороший вариант
BEFORE INSERT OR UPDATE OR DELETE ON %1$I
	FOR EACH ROW EXECUTE PROCEDURE __vc__record_tracker_%1$I();', source_table_name, vc_table_name, equality_condition);
		
		--добавляем комментарии к сгенерированным сущностям
		--к архивной таблице
		EXECUTE format('COMMENT ON TABLE %1$I IS ''Table for storing previous versions of tupples of relation %2$s. Note that last version stored in the main table.''', vc_table_name, source_table_name);
		--к служебным атрибутам в основной таблице
		EXECUTE format('COMMENT ON COLUMN %1$I.__vc__snapshot_version IS ''The latest version of the tuple.''', vc_table_name);
		EXECUTE format('COMMENT ON COLUMN %1$I.__vc__snapshot_timestamp IS ''Time of change (creation, change) of the latest version of the tuple.''', source_table_name);
		--к триггеру и триггерной функции
		EXECUTE format('COMMENT ON TRIGGER __vc__record_tracker_%1$I ON %1$I IS ''A trigger that tracks any changes to the table.''', source_table_name);
		EXECUTE format('COMMENT ON FUNCTION __vc__record_tracker_%1$I() IS ''Trigger function for the corresponding trigger..''', source_table_name);
	END LOOP;
	
	RAISE NOTICE 'Version control successfully initialized';
END;
$init_vc$ LANGUAGE plpgsql;


/**
 * Процедура деинициализации контроля версий.
 * 
 * Удаляет созданные триггеры, архивные таблицы (если не указано иное) и служебные атрибуты в исходном отношении
 * В случае, если вы хотите удалить любые следы системы контроля версий, то воспользуйтесь скриптом из файла {@link remove_vc.sql}
 *
 * @param drop_vc_tables удалять созданные архивные таблицы или нет
 * @param tables_blacklist отношения, которые будут игнорироваться в процессе очистки
 */
CREATE OR REPLACE PROCEDURE remove_version_control(drop_vc_tables BOOLEAN DEFAULT TRUE, VARIADIC tables_blacklist name[] DEFAULT '{}') AS $remove_vc$
DECLARE
	vc_catalog name DEFAULT current_database(); --текущая база
	vc_schema name DEFAULT current_schema(); --схема
	
	tbl_name_row RECORD; --временная переменная для получения результата запроса
	source_table_name name; --имя таблицы-источника
	vc_table_name name; --имя архивной таблица
BEGIN
	RAISE NOTICE 'Removing version control from %...', vc_schema;
	
	RAISE NOTICE 'Tables in blacklist:';
	FOR tbl_name_row IN SELECT unnest(tables_blacklist)
	LOOP
		RAISE NOTICE '	%', tbl_name_row.unnest;
	END LOOP;
	
	/* итерируемся по таблицам, для каждой удаляем:
		- триггер
		- таблицу версий
		- вспомогательные атрибуты
	*/
	FOR tbl_name_row IN SELECT table_name FROM information_schema.tables
			   WHERE table_type = 'BASE TABLE' AND
			   		 table_schema = vc_schema AND
					 table_name NOT IN (SELECT unnest(tables_blacklist)) AND
					 table_name NOT LIKE '#_#_vc#_#_%' ESCAPE '#'
	LOOP
		RAISE NOTICE 'Proccessing %', tbl_name_row.table_name;
		
		source_table_name := tbl_name_row.table_name;
		vc_table_name := '__vc__' || source_table_name;
		
		--удаляем триггерную функцию и связанные с ней триггеры (CASCADE)
		EXECUTE format('DROP FUNCTION IF EXISTS %I() CASCADE', '__vc__record_tracker_' || source_table_name);
		
		IF drop_vc_tables THEN
			EXECUTE format('DROP TABLE IF EXISTS %I;', vc_table_name);
		END IF;
		
		--удаляем служебные атрибуты из исходной таблицы
		EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS __vc__snapshot_version;', source_table_name);
		EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS __vc__snapshot_timestamp;', source_table_name);
	END LOOP;
	
	RAISE NOTICE 'Version control successfully removed';
END;
$remove_vc$ LANGUAGE plpgsql;


/**
 * Возвращает конкретную версию сущности
 *
 * Если сущность отсутствует в архивной таблице, то предпринимается попытка найти ее в основной в предположении, что запрошена последняя версия.
 * Возвращает NULL, если строка удовлетворяющая заданным условиям и версии не найдена (поиск осущест)
 *
 * Внимание: функция возвращает найденную сущность в бестиповом формате RECORD, поэтому при запросе нужно полностью указать список колонок.
 *
 * Примеры вызова функции:
 * select vc_get_version('relation_name', 'col1 = val1 AND col2 = val2', 1);
 *
 * select * from vc_get_version('relation_name', 'col1 = val1 AND col2 = val2', 1) as 
 * tab(col1 type1, col2 type2, ..., __vc__snapshot_version int, __vc__snapshot_timestamp timestamp);
 * Служебные поля __vc__snapshot_version и __vc__snapshot_timestamp обязательно должны быть указаны 
 * (требуется описание схемы таблицы с полным списком столбцов в правильном порядке)
 *
 * @param rel_name идентификатор отношения
 * @param selection_condition_str условия отбора отношений, записанное в виде строки
 * @param record_version требуемая версия сущности
 */
CREATE OR REPLACE FUNCTION vc_get_version(rel_name name, selection_condition_str text, record_version integer)
	RETURNS record
	AS $$
DECLARE
	query_res record;
BEGIN
	IF record_version < 1 THEN
		RAISE EXCEPTION 'Value error. Version must be greater than 0';
	END IF;
	
	--если запрашивается последняя, то надо брать из основной таблицы (или схитрить и сказать, что функция возвращает архивную версию, если вернуло нулл, то чекайте еще основную таблицу)
	EXECUTE format('SELECT * FROM __vc__%1$I WHERE %2$s AND __vc__snapshot_version = %3$s;', rel_name, selection_condition_str, record_version) INTO query_res;
	
	--если ничего не достали из архивной таблицы, то пытаемся получить из основной
	IF query_res IS NULL THEN
		EXECUTE format('SELECT * FROM %1$I WHERE %2$s AND __vc__snapshot_version = %3$s;', rel_name, selection_condition_str, record_version) INTO query_res;
	END IF;
	
	RETURN query_res;
END;
$$ LANGUAGE plpgsql;


/**
 * Преобразует запись в массив строк
 *
 * @param r запись
 */
CREATE OR REPLACE FUNCTION record_to_text_array(r record) RETURNS text[] AS $$
DECLARE
	temp_str text;
BEGIN
	IF r IS NULL THEN
		RETURN '{}';
	END IF;

	temp_str := trim(leading '(' from r::text);
	temp_str := trim(trailing ')' from temp_str);
	
	RETURN string_to_array(temp_str, ',');
END;
$$ LANGUAGE plpgsql;


/**
 * Восстанавливает определенную версию сущности из архива, накатывая новую версию поверх старой.
 *
 * Возвращает ошибку, если строка удовлетворяющая заданным условиям и версии не найдена.
 *
 * Пример вызова:
 * CALL vc_restore_version('relation_name', 'col1 = val1 AND col2 = val2', 1);
 *
 * @param rel_name идентификатор отношения
 * @param selection_condition_str условия отбора отношений, записанное в виде строки
 * @param record_version требуемая версия сущности
 */
CREATE OR REPLACE PROCEDURE vc_restore_version(rel_name name, selection_condition_str text, record_version integer) AS $$
DECLARE
	archived_tupple record;
	
	data_substitution text;
	record_vals text[];
	
	col_name name;
	col_pos int;
BEGIN
	archived_tupple := vc_get_version(rel_name, selection_condition_str, record_version);
	
	--get version from archive
	IF archived_tupple IS NULL THEN
		RAISE EXCEPTION 'Tuple satisfying specified conditions (%) not found', selection_condition_str ||  'AND __vc__snapshot_version = ' || record_version;
	END IF;
	
	--генерируем подстановку данных
	data_substitution := ''; 
	record_vals := record_to_text_array(archived_tupple);
	
	FOR col_name, col_pos IN SELECT column_name, ordinal_position FROM information_schema.columns
 							 WHERE table_schema = current_schema() AND 
			  					   table_name = rel_name AND
			  					   column_name NOT IN ('__vc__snapshot_version', '__vc__snapshot_timestamp')
		ORDER BY ordinal_position
	LOOP
		data_substitution := data_substitution || col_name || ' = ''' || trim(both '"' from record_vals[col_pos]) || ''' , ';
	END LOOP;
	
	data_substitution := trim(trailing ' ,' FROM data_substitution);
	
	--генерируем запрос, обновляющий все колонки, кроме служебных полей
	EXECUTE format('UPDATE %1$I SET %2$s WHERE %3$s;', rel_name, data_substitution, selection_condition_str); 
END;
$$ LANGUAGE plpgsql;


/**
 * Вспомогательная функция для сравнения json объектов и генерации объекта с различиями
 *
 * Честно позаимствовано отсюда:
 * https://stackoverflow.com/questions/36041784/postgresql-compare-two-jsonb-objects
 *
 * @param val1 json объект
 * @param val2 json объект
 */
CREATE OR REPLACE FUNCTION jsonb_diff_val(val1 json,val2 json)
	RETURNS json AS $$
DECLARE
	result jsonb;
	v record;
BEGIN
	result = val1::jsonb;
	
	FOR v IN SELECT * FROM jsonb_each(val2::jsonb) LOOP
		IF result @> jsonb_build_object(v.key, v.value) THEN 
			result = result - v.key;
     	ELSIF result ? v.key THEN 
			CONTINUE;
     	ELSE
        	result = result || jsonb_build_object(v.key, 'null');
     	END IF;
   END LOOP;
   
   RETURN result::json;
END;
$$ LANGUAGE plpgsql;


/**
 * Генерирует json объект, содержащий в себе различия.
 *
 * Возвращает ошибку, если строка удовлетворяющая заданным условиям и версии не найдена.
 *
 * Пример вызова:
 * CALL vc_generate_diff('relation_name', 'col1 = val1 AND col2 = val2', 1, 2);
 *
 * @param rel_name идентификатор отношения
 * @param selection_condition_str условия отбора отношений, записанное в виде строки
 * @param version_a требуемая версия сущности 1
 * @param version_a требуемая версия сущности 2
 */
CREATE OR REPLACE FUNCTION vc_generate_diff(rel_name name, selection_condition_str text, version_a integer, version_b integer)
	RETURNS json
	AS $$
DECLARE
	rec_a record;
	rec_b record;
BEGIN
	rec_a := vc_get_version(rel_name, selection_condition_str, version_a);
	
	IF rec_a IS NULL THEN
		RAISE EXCEPTION 'Tuple satisfying specified conditions (%) not found.', selection_condition_str ||  'AND __vc__snapshot_version = ' || record_version;
	END IF;
	
	rec_b := vc_get_version(rel_name, selection_condition_str, version_b);
	
	IF rec_b IS NULL THEN
		RAISE EXCEPTION 'Tuple satisfying specified conditions (%) not found.', selection_condition_str ||  'AND __vc__snapshot_version = ' || record_version;
	END IF;
	
	RETURN jsonb_diff_val(row_to_json(rec_a), row_to_json(rec_b));
END;
$$ LANGUAGE plpgsql;

COMMIT;