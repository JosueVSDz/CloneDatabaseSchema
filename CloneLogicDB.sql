

ALTER PROCEDURE [dbo].[CloneLogicDB64](@CloneShema VARCHAR(MAX), @NuevoSchema VARCHAR(MAX))
AS

--////Definiciones
DECLARE @T_SQL VARCHAR(MAX) --usado por cursores..
DECLARE @Id_SCHEMA INT = (SELECT TOP 1 schema_id FROM sys.schemas WHERE name = @CloneShema)

BEGIN TRANSACTION ClonacionLogicaDataBaseJdz
BEGIN TRY
---================================================
---////////////////////////////////////////////////

	PRINT 'Comenzando creación de SCHEMA...'
	SET @T_SQL = 'CREATE SCHEMA ' + @NuevoSchema + ''
	EXEC(@T_SQL)
	PRINT 'Completado creacion de SCHEMA!'

	--<[==================================================================================
	--<[///CLONANDO TABLAS
	PRINT 'Comenzando Clonacion de Tablas...'
	SET @T_SQL = NULL
	DECLARE cTABL CURSOR FOR
	SELECT OBJtablas = NAME FROM sys.tables WHERE SCHEMA_ID = @Id_SCHEMA

	OPEN cTABL
	FETCH NEXT FROM cTABL INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC('SET NOCOUNT ON; SELECT * INTO ' + @NuevoSchema + '.' + @T_SQL + ' FROM '+ @CloneShema +'.' + @T_SQL + ' WHERE 0=1')
				FETCH NEXT FROM cTABL INTO @T_SQL
			END
		CLOSE cTABL
	DEALLOCATE cTABL
	PRINT 'Completado Clonacion de Tablas!'

	--<[==================================================================================
	--<[///CLONANDO LLAVES PK
	PRINT 'Comenzando Clonacion de PKs...'
	SET @T_SQL = NULL
	DECLARE cPKllaves CURSOR FOR
	SELECT
		'ALTER TABLE ' + @NuevoSchema + '.' + OBJECT_NAME(i.object_id) + 
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

	OPEN cPKllaves
	FETCH NEXT FROM cPKllaves INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cPKllaves INTO @T_SQL
			END
		CLOSE cPKllaves
	DEALLOCATE cPKllaves
	PRINT 'Completado Clonacion de PKs!'

	--<[==================================================================================
	--<[///CLONANDO LLAVES DF
	PRINT 'Comenzando Clonacion de DFs...'
	SET @T_SQL = NULL
	DECLARE cDFllaves CURSOR FOR
	SELECT
		'ALTER TABLE ' + @NuevoSchema + '.' + OBJECT_NAME(dc.parent_object_id) + 
		' ADD CONSTRAINT ' + dc.name + ' DEFAULT(' + definition 
		+ ') FOR ' + c.name AS DFkeys
	FROM sys.default_constraints dc
	INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
	WHERE dc.SCHEMA_ID = @Id_SCHEMA

	OPEN cDFllaves
	FETCH NEXT FROM cDFllaves INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cDFllaves INTO @T_SQL
			END
		CLOSE cDFllaves
	DEALLOCATE cDFllaves
	PRINT 'Completado Clonacion de DFs!'

	--<[==================================================================================
	--<[///CLONANDO LLAVES FK
	PRINT 'Comenzando Clonacion de FKs...'
	SET @T_SQL = NULL
	DECLARE cFKllaves CURSOR FOR
	SELECT
		'ALTER TABLE ' + @NuevoSchema + '.' + OBJECT_NAME(ff.parent_object_id) + 
		' WITH NOCHECK ADD CONSTRAINT ' + OBJECT_NAME(ff.constraint_object_id) + ' FOREIGN KEY(' + --+ c.name
		stuff((
				SELECT ',' + c.name
				FROM sys.foreign_key_columns fa
				INNER JOIN sys.columns c ON fa.parent_column_id = c.column_id AND fa.parent_object_id = c.object_id
				WHERE fa.constraint_object_id = o.object_id
				FOR XML PATH('')
				), 1, 1, '')
		+ ') REFERENCES ' + @NuevoSchema + '.' + OBJECT_NAME(ff.referenced_object_id) + '(' +
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

	OPEN cFKllaves
	FETCH NEXT FROM cFKllaves INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cFKllaves INTO @T_SQL
			END
		CLOSE cFKllaves
	DEALLOCATE cFKllaves
	PRINT 'Completado Clonacion de FKs!'

		--<[==================================================================================
	--<[///CLONANDO DISPARADORES
	PRINT 'Comenzando Clonacion de TGs...'
	SET @T_SQL = NULL
	DECLARE cDISP CURSOR FOR
	SELECT DEFIdisparadores =  REPLACE(
								REPLACE(definition
									    , CASE WHEN CHARINDEX('['+ @CloneShema +'].['+o.name+']', definition) > 0
											THEN '['+ @CloneShema +'].['+o.name+']'
											ELSE o.name
									     END 
							            , '['+ @NuevoSchema +'].[' + o.name + ']')
						     , CASE WHEN CHARINDEX(@CloneShema +'.'+object_name(t.parent_id), definition) > 0
											THEN @CloneShema +'.'+object_name(t.parent_id)
											ELSE object_name(t.parent_id)
									  END
						    , @NuevoSchema +'.' + object_name(t.parent_id))
	FROM sys.triggers t 
	INNER JOIN sys.sql_modules m ON t.object_id = m.object_id
	INNER JOIN sys.objects o ON o.object_id = t.object_id
	WHERE o.schema_id = @Id_SCHEMA

	OPEN cDISP
	FETCH NEXT FROM cDISP INTO @T_SQL
		WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC(@T_SQL)
				FETCH NEXT FROM cDISP INTO @T_SQL
			END
		CLOSE cDISP
	DEALLOCATE cDISP
	PRINT 'Completado Clonacion de TGs!'

	--<[==================================================================================
	--<[///CLONANDO FUNCIONES
	PRINT 'Comenzando Clonacion de FNs...'
	SET @T_SQL = NULL
	DECLARE cFUNC CURSOR FOR
	SELECT DEFIfunciones = REPLACE(definition
							 , CASE WHEN CHARINDEX('[' + @CloneShema + '].['+f.name+']', definition) > 0 THEN '[' + @CloneShema + '].['+f.name+']'
									WHEN CHARINDEX('' + @CloneShema + '.'+f.name+'', definition) > 0 THEN '' + @CloneShema + '.'+f.name+''
									ELSE f.name
							  END 
						 , '[' + @NuevoSchema + '].[' + f.name + ']')
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
	PRINT 'Completado Clonacion de FNs!'

	--<[==================================================================================
	--<[///CLONANDO PROCEDIMIENTOS
	PRINT 'Comenzando Clonacion de SPs...'
	SET @T_SQL = NULL
	DECLARE cPROC CURSOR FOR
	select DEFIprocedimientos  = REPLACE(definition
									 , CASE WHEN CHARINDEX('[' + @CloneShema + '].['+p.name+']', definition) > 0
											THEN '[' + @CloneShema + '].['+p.name+']'
											ELSE 
												CASE WHEN CHARINDEX(@CloneShema + '.'+p.name, definition) > 0
													THEN @CloneShema + '.'+p.name
													ELSE p.name
												END
									  END 
						 , '['+@NuevoSchema+'].[' + p.name + ']')
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
	PRINT 'Completado Clonacion de SPs!'

	--<[==================================================================================
	--<[///CLONANDO VISTAS
	PRINT 'Comenzando Clonacion de Views...'
	SET @T_SQL = NULL
	DECLARE cVIEW CURSOR FOR
	select DEFIviews  = REPLACE(definition
									 , CASE WHEN CHARINDEX('[' + @CloneShema + '].['+v.name+']', definition) > 0
											THEN '[' + @CloneShema + '].['+v.name+']'
											ELSE 
												CASE WHEN CHARINDEX(@CloneShema + '.'+v.name, definition) > 0
													THEN @CloneShema + '.'+v.name
													ELSE v.name
												END
									  END 
						 , '['+@NuevoSchema+'].[' + v.name + ']')
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
	PRINT 'Completado Clonacion de Views!'

	--<[==================================================================================
	--<[///CLONANDO VISTAS
	PRINT 'Comenzando Clonacion de IUnique...'
	SET @T_SQL = NULL
	DECLARE cINDEXU CURSOR FOR
	select DEFIuniqueindex  = 'CREATE UNIQUE INDEX '+ ind.name +' ON '+ '['+@NuevoSchema+'].[' + t.name +'] (' +
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
	PRINT 'Completado Clonacion de IUnique!'


	COMMIT TRANSACTION ClonacionLogicaDataBaseJdz
	PRINT 'CLONACION COMPLETADA EXITOSAMENTE!'
	SELECT  
        ErrorNumber = -1
        ,ErrorSeverity = 0
        ,ErrorState = 0
        ,ErrorProcedure = NULL
        ,ErrorLine  = 0
        ,ErrorMessage = 'Sin Errores!'

---\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
---================================================
END TRY
BEGIN CATCH
	---================================================
	---////////////////////////////////////////////////
	ROLLBACK TRANSACTION ClonacionLogicaDataBaseJdz
	PRINT 'INTERRUPCION...'
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

	IF (SELECT CURSOR_STATUS('global','cPKllaves')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cPKllaves')) > -1
	   BEGIN
		CLOSE cPKllaves
	   END
	 DEALLOCATE cPKllaves
	END

	IF (SELECT CURSOR_STATUS('global','cDFllaves')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cDFllaves')) > -1
	   BEGIN
		CLOSE cDFllaves
	   END
	 DEALLOCATE cDFllaves
	END

	IF (SELECT CURSOR_STATUS('global','cFKllaves')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cFKllaves')) > -1
	   BEGIN
		CLOSE cFKllaves
	   END
	 DEALLOCATE cFKllaves
	END

	IF (SELECT CURSOR_STATUS('global','cPROC')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cPROC')) > -1
	   BEGIN
		CLOSE cPROC
	   END
	 DEALLOCATE cPROC
	END

	IF (SELECT CURSOR_STATUS('global','cDISP')) >= -1
	 BEGIN
	  IF (SELECT CURSOR_STATUS('global','cDISP')) > -1
	   BEGIN
		CLOSE cDISP
	   END
	 DEALLOCATE cDISP
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

-->>> CloneLogicDB64 'dbo', 'GOTNX01'