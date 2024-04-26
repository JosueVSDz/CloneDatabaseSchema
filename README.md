# SQL Server Schema Cloner

## Description
This SQL Server stored procedure allows you to clone a schema from one database to another schema with a different name. It facilitates the replication of core and basic objects of a database in another environment, such as development, testing, or production.

## Usage
1. Copy the content of the `CloneSchema.sql` script into your SQL Server Management Studio or any tool you use to execute SQL scripts on your server.
2. Open a new query and paste the script.
3. Edit the `@source_schema` and `@destination_schema` variables with the names of the source and destination schemas, respectively.
4. Execute the script.
5. The stored procedure will clone the core objects from the source schema to the destination schema.

**Syntax for executing the stored procedure:**

```sql
EXEC sys_CloneDatabaseSchema 'source_schema_name', 'destination_schema_name'
```

## Features
The `CloneSchema` stored procedure clones the following objects from the source database to the destination database:
- Tables (including columns, primary keys, foreign keys, and unique constraints)
- Views
- Stored Procedures
- Functions
- Triggers
- Default constraints

## Contribution
Contributions are welcome. If you encounter any issues or have any suggestions for improving the stored procedure, please create an issue in this repository or submit a pull request.

## License
This project is licensed under the MIT License. For more details, see the [LICENSE](LICENSE) file.
