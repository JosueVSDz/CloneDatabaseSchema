


GO
CREATE OR ALTER PROCEDURE sys_CloneDatabaseSchema(
	@CopySchema VARCHAR(MAX),
	@NewSchema VARCHAR(MAX)
)
AS
--> by Dev. Josue Diaz
--> https://www.linkedin.com/in/josue-diaz-8007611a1
	
--////Definitions
DECLARE @T_SQL VARCHAR(MAX)
DECLARE @Id_SCHEMA INT = (SELECT TOP 1 schema_id FROM sys.schemas WHERE name = @CopySchema)

BEGIN TRANSACTION ts_CopySchemaJdz
BEGIN TRY
---================================================
---////////////////////////////////////////////////

	PRINT 'Starting SCHEMA creation...'
	SET @T_SQL = 'CREATE SCHEMA ' + @NewSchema + ''
	EXEC(@T_SQL)
	PRINT 'SCHEMA creation completed!'

	--<[==================================================================================
	--<[///CLONE TABLES
	PRINT 'Starting Table Cloning...'
	SET @T_SQL = NULL
	DECLARE cTABL CURSOR FOR
	SELECT OBJtablas = NAME FROM sys.tables WHERE SCHEMA_ID = @Id_SCHEMA

	OPEN cTABL
	FETCH NEXT FROM cTABL INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC('SET NOCOUNT ON; SELECT * INTO ' + @NewSchema + '.' + @T_SQL + ' FROM '+ @CopySchema +'.' + @T_SQL + ' WHERE 0=1')
				FETCH NEXT FROM cTABL INTO @T_SQL
			END
		CLOSE cTABL
	DEALLOCATE cTABL
	PRINT 'Completed Table Cloning!'

	--<[==================================================================================
	--<[///CLONE PRIMARY
	PRINT 'Starting Primary Keys Cloning...'
	SET @T_SQL = NULL
	DECLARE cPKs CURSOR FOR
	SELECT
		'ALTER TABLE ' + @NewSchema + '.' + OBJECT_NAME(i.object_id) + 
		' ADD CONSTRAINT ' + i.name + ' PRIMARY KEY(' +
		stuff((
				SELECT ',' + COL_NAME(ic.OBJECT_ID,ic.column_id)
				FROM sys.index_columns ic
				WHERE i.OBJECT_ID = ic.OBJECT_ID AND i.index_id = ic.index_id
				FOR XML PATH('')
				), 1, 1, '')
		+ ')' AS PKkeys
	FROM sys.indexes i
	INNER JOIN sys.objects o ON o.object_id = i.object_id
	WHERE i.is_primary_key = 1 AND o.schema_id = @Id_SCHEMA
	ORDER BY i.object_id

	OPEN cPKs
	FETCH NEXT FROM cPKs INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cPKs INTO @T_SQL
			END
		CLOSE cPKs
	DEALLOCATE cPKs
	PRINT 'Completed Cloning of Primary Keys!'

	--<[==================================================================================
	--<[///CLONE DEFAULT
	PRINT 'Starting DF Cloning...'
	SET @T_SQL = NULL
	DECLARE cDFs CURSOR FOR
	SELECT
		'ALTER TABLE ' + @NewSchema + '.' + OBJECT_NAME(dc.parent_object_id) + 
		' ADD CONSTRAINT ' + dc.name + ' DEFAULT(' + definition 
		+ ') FOR ' + c.name AS DFkeys
	FROM sys.default_constraints dc
	INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
	WHERE dc.SCHEMA_ID = @Id_SCHEMA

	OPEN cDFs
	FETCH NEXT FROM cDFs INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cDFs INTO @T_SQL
			END
		CLOSE cDFs
	DEALLOCATE cDFs
	PRINT 'Completed DF Cloning!'

	--<[==================================================================================
	--<[///CLONE FOREIGN KEY
	PRINT 'Starting Foreign Key Cloning...'
	SET @T_SQL = NULL
	DECLARE cFKs CURSOR FOR
	SELECT
		'ALTER TABLE ' + @NewSchema + '.' + OBJECT_NAME(ff.parent_object_id) + 
		' WITH NOCHECK ADD CONSTRAINT ' + OBJECT_NAME(ff.constraint_object_id) + ' FOREIGN KEY(' + --+ c.name
		stuff((
				SELECT ',' + c.name
				FROM sys.foreign_key_columns fa
				INNER JOIN sys.columns c ON fa.parent_column_id = c.column_id AND fa.parent_object_id = c.object_id
				WHERE fa.constraint_object_id = o.object_id
				FOR XML PATH('')
				), 1, 1, '')
		+ ') REFERENCES ' + @NewSchema + '.' + OBJECT_NAME(ff.referenced_object_id) + '(' +
		stuff((
				SELECT ',' + cc.name
				FROM sys.foreign_key_columns fb
				INNER JOIN sys.columns cc ON fb.referenced_column_id = cc.column_id AND fb.referenced_object_id = cc.object_id
				WHERE fb.constraint_object_id = o.object_id
				FOR XML PATH('')
				), 1, 1, '')
		+ ')' 
		+ (CASE WHEN o.update_referential_action = 1 THEN ' ON UPDATE CASCADE ' ELSE '' END)
		+ (CASE WHEN o.delete_referential_action = 1 THEN ' ON DELETE CASCADE ' ELSE '' END) AS FKkeys
	FROM sys.foreign_keys o
	INNER JOIN sys.foreign_key_columns ff on ff.constraint_object_id = o.object_id
	WHERE o.type = 'F' AND o.SCHEMA_ID = @Id_SCHEMA
	GROUP BY ff.parent_object_id, ff.constraint_object_id, ff.referenced_object_id
		   , o.object_id, o.update_referential_action, o.delete_referential_action
	ORDER BY ff.parent_object_id

	OPEN cFKs
	FETCH NEXT FROM cFKs INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cFKs INTO @T_SQL
			END
		CLOSE cFKs
	DEALLOCATE cFKs
	PRINT 'Completed Cloning of Foreign Key!'

		--<[==================================================================================
	--<[///CLONE TRIGGERS
	PRINT 'Starting Trigger Cloning...'
	SET @T_SQL = NULL
	DECLARE cTGRs CURSOR FOR
	SELECT T_SQL =  REPLACE(
								REPLACE(definition
									    , CASE WHEN CHARINDEX('['+ @CopySchema +'].['+o.name+']', definition) > 0
											THEN '['+ @CopySchema +'].['+o.name+']'
											ELSE o.name
									     END 
							            , '['+ @NewSchema +'].[' + o.name + ']')
						     , CASE WHEN CHARINDEX(@CopySchema +'.'+object_name(t.parent_id), definition) > 0
											THEN @CopySchema +'.'+object_name(t.parent_id)
											ELSE object_name(t.parent_id)
									  END
						    , @NewSchema +'.' + object_name(t.parent_id))
	FROM sys.triggers t 
	INNER JOIN sys.sql_modules m ON t.object_id = m.object_id
	INNER JOIN sys.objects o ON o.object_id = t.object_id
	WHERE o.schema_id = @Id_SCHEMA

	OPEN cTGRs
	FETCH NEXT FROM cTGRs INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cTGRs INTO @T_SQL
			END
		CLOSE cTGRs
	DEALLOCATE cTGRs
	PRINT 'Completed Trigger Cloning!'

	--<[==================================================================================
	--<[///CLONE FUNCTIONS
	PRINT 'Starting Function Cloning...'
	SET @T_SQL = NULL
	DECLARE cFUNC CURSOR FOR
	SELECT T_SQL = REPLACE(definition
							 , CASE WHEN CHARINDEX('[' + @CopySchema + '].['+f.name+']', definition) > 0 THEN '[' + @CopySchema + '].['+f.name+']'
									WHEN CHARINDEX('' + @CopySchema + '.'+f.name+'', definition) > 0 THEN '' + @CopySchema + '.'+f.name+''
									ELSE f.name
							  END 
						 , '[' + @NewSchema + '].[' + f.name + ']')
	FROM sys.objects f 
	INNER JOIN sys.sql_modules m ON f.object_id = m.object_id WHERE f.TYPE in ('FN', 'IF', 'TF') AND f.SCHEMA_ID = @Id_SCHEMA

	OPEN cFUNC
	FETCH NEXT FROM cFUNC INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cFUNC INTO @T_SQL
			END
		CLOSE cFUNC
	DEALLOCATE cFUNC
	PRINT 'Completed Function Cloning!'

	--<[==================================================================================
	--<[///CLONE STORE PROCEDURES
	PRINT 'Started Cloning Store Procedures...'
	SET @T_SQL = NULL
	DECLARE cPROC CURSOR FOR
	select T_SQL  = REPLACE(definition
									 , CASE WHEN CHARINDEX('[' + @CopySchema + '].['+p.name+']', definition) > 0
											THEN '[' + @CopySchema + '].['+p.name+']'
											ELSE 
												CASE WHEN CHARINDEX(@CopySchema + '.'+p.name, definition) > 0
													THEN @CopySchema + '.'+p.name
													ELSE p.name
												END
									  END 
						 , '['+@NewSchema+'].[' + p.name + ']')
	from sys.procedures p
	INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
	where p.SCHEMA_ID = @Id_SCHEMA AND p.type = 'P' and name <> 'CloneLogicDB64'

	OPEN cPROC
	FETCH NEXT FROM cPROC INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				--SELECT cmd = @T_SQL 
				EXEC(@T_SQL)
				FETCH NEXT FROM cPROC INTO @T_SQL
			END
		CLOSE cPROC
	DEALLOCATE cPROC
	PRINT 'Completed Cloning of Store Procedures!'

	--<[==================================================================================
	--<[///CLONE VIEW
	PRINT 'Starting View Cloning...'
	SET @T_SQL = NULL
	DECLARE cVIEW CURSOR FOR
	select T_SQL  = REPLACE(definition
									 , CASE WHEN CHARINDEX('[' + @CopySchema + '].['+v.name+']', definition) > 0
											THEN '[' + @CopySchema + '].['+v.name+']'
											ELSE 
												CASE WHEN CHARINDEX(@CopySchema + '.'+v.name, definition) > 0
													THEN @CopySchema + '.'+v.name
													ELSE v.name
												END
									  END 
						 , '['+@NewSchema+'].[' + v.name + ']')
	from sys.views v
	INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
	where v.SCHEMA_ID = @Id_SCHEMA AND v.type = 'V'

	OPEN cVIEW
	FETCH NEXT FROM cVIEW INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				--SELECT cmd = @T_SQL 
				EXEC(@T_SQL)
				FETCH NEXT FROM cVIEW INTO @T_SQL
			END
		CLOSE cVIEW
	DEALLOCATE cVIEW
	PRINT 'Completed View Cloning!'

	--<[==================================================================================
	--<[///CLONANDO VISTAS
	PRINT 'Starting Unique Index Cloning...'
	SET @T_SQL = NULL
	DECLARE cINDEXU CURSOR FOR
	select T_SQL  = 'CREATE UNIQUE INDEX '+ ind.name +' ON '+ '['+@NewSchema+'].[' + t.name +'] (' +
							 stuff((
										SELECT ',' + col.name
										FROM sys.index_columns ic
										INNER JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id 
										WHERE ind.object_id = ic.object_id and ind.index_id = ic.index_id
										ORDER BY ic.index_column_id
										FOR XML PATH('')
										), 1, 1, '')
								+ ')'
	FROM sys.indexes ind 
	INNER JOIN sys.tables t ON ind.object_id = t.object_id 
	WHERE 
			ind.is_primary_key = 0 
			AND ind.is_unique = 1
			AND ind.is_unique_constraint = 0 
			AND t.is_ms_shipped = 0
			AND t.SCHEMA_ID = @Id_SCHEMA
	ORDER BY 
			t.name, ind.name, ind.index_id

	OPEN cINDEXU
	FETCH NEXT FROM cINDEXU INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				--SELECT cmd = @T_SQL 
				EXEC(@T_SQL)
				FETCH NEXT FROM cINDEXU INTO @T_SQL
			END
		CLOSE cINDEXU
	DEALLOCATE cINDEXU
	PRINT 'Completed Cloning of Unique Index!'


	COMMIT TRANSACTION ts_CopySchemaJdz
	PRINT 'CLONING SUCCESSFULLY COMPLETED!'
	SELECT  
        ErrorNumber = -1
        ,ErrorSeverity = 0
        ,ErrorState = 0
        ,ErrorProcedure = NULL
        ,ErrorLine  = 0
        ,ErrorMessage = '0 errors!'

---\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
---================================================
END TRY
BEGIN CATCH
	---================================================
	---////////////////////////////////////////////////
	ROLLBACK TRANSACTION ts_CopySchemaJdz
	PRINT 'INTERRUPTION...'
	SELECT  
        ERROR_NUMBER() AS ErrorNumber  
        ,ERROR_SEVERITY() AS ErrorSeverity  
        ,ERROR_STATE() AS ErrorState  
        ,ERROR_PROCEDURE() AS ErrorProcedure  
        ,ERROR_LINE() AS ErrorLine  
        ,ERROR_MESSAGE() AS ErrorMessage;

	IF (SELECT CURSOR_STATUS('global','cTABL')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cTABL')) > -1
	   BEGIN
		CLOSE cTABL
	   END
	 DEALLOCATE cTABL
	END

	IF (SELECT CURSOR_STATUS('global','cPKs')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cPKs')) > -1
	   BEGIN
		CLOSE cPKs
	   END
	 DEALLOCATE cPKs
	END

	IF (SELECT CURSOR_STATUS('global','cDFs')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cDFs')) > -1
	   BEGIN
		CLOSE cDFs
	   END
	 DEALLOCATE cDFs
	END

	IF (SELECT CURSOR_STATUS('global','cFKs')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cFKs')) > -1
	   BEGIN
		CLOSE cFKs
	   END
	 DEALLOCATE cFKs
	END

	IF (SELECT CURSOR_STATUS('global','cPROC')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cPROC')) > -1
	   BEGIN
		CLOSE cPROC
	   END
	 DEALLOCATE cPROC
	END

	IF (SELECT CURSOR_STATUS('global','cTGRs')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cTGRs')) > -1
	   BEGIN
		CLOSE cTGRs
	   END
	 DEALLOCATE cTGRs
	END

	IF (SELECT CURSOR_STATUS('global','cFUNC')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cFUNC')) > -1
	   BEGIN
		CLOSE cFUNC
	   END
	 DEALLOCATE cFUNC
	END
	---\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
	---================================================
END CATCH

-->>> sys_CloneDatabaseSchema 'dbo', 'CompanyX'

GO
