# NITCbase — A Homegrown Relational Database Management System

NITCbase is a fully functional RDBMS built from scratch in C++ as part of the [NITCbase project](https://nitcbase.github.io) at NIT Calicut. All 12 stages of implementation are complete, resulting in a working database engine capable of executing SQL-like queries through a command-line interface.

The system implements an **8-layer architecture** — from the Physical Layer at the bottom up through the Buffer, B+ Tree, Block Access, Cache, Algebra, and Schema layers, to a Frontend Interface at the top.

---

## Features

- Create, drop, open, close, and rename relations
- Insert records individually or in bulk from CSV files
- Select, project, filter, and equi-join relations
- B+ Tree based indexing with create/drop index support
- LRU-based disk buffer caching (up to 32 blocks)
- XFS Interface for direct disk access and file management
- Batch command execution via script files

---

## Architecture Overview

```
┌─────────────────────────────────┐
│        Frontend Interface       │  ← User-facing CLI
├─────────────────────────────────┤
│          Schema Layer           │  ← DDL commands
│          Algebra Layer          │  ← DML commands
├─────────────────────────────────┤
│          Cache Layer            │  ← Relation & Attribute Cache
│       Block Access Layer        │  ← Core select/project logic
│         B+ Tree Layer           │  ← Indexing
├─────────────────────────────────┤
│          Buffer Layer           │  ← LRU disk buffer
│         Physical Layer          │  ← Raw disk read/write
└─────────────────────────────────┘
```

---

## Building and Running

### Prerequisites

- Unix/Linux system
- `g++` (C++17 or later recommended)
- `make`

### Build

```bash
git clone <your-repo-url>
cd nitcbase
make
```

### Run the Frontend Interface

```bash
./nitcbase
```

### Run the XFS Interface

```bash
./xfs-interface
```

> The XFS Interface is used to format the disk, import/export files, and run DDL/DML operations directly on the NITCbase file system from the host environment.

---

## Command Reference

### Data Definition Language (DDL)

DDL commands define and modify the database schema. They are supported by both the Frontend Interface and the XFS Interface.

---

#### CREATE TABLE

Creates a new relation with the given attribute names and types. Attribute types can only be `NUM` (number) or `STR` (string).

```
CREATE TABLE tablename(attr1_name attr1_type, attr2_name attr2_type, ...)
```

```sql
CREATE TABLE Students(Rollno NUM, Name STR, CGPA NUM)
```

> Attribute names and table names must not exceed 15 characters. All attribute names within a relation must be unique.

---

#### DROP TABLE

Deletes a relation along with all its records, indexes, and catalog entries.

```
DROP TABLE tablename
```

```sql
DROP TABLE Students
```

---

#### OPEN TABLE

Opens a relation for manipulation. Must be called before operations like `CREATE INDEX`.

```
OPEN TABLE tablename
```

---

#### CLOSE TABLE

Closes an open relation and updates the cache.

```
CLOSE TABLE tablename
```

---

#### CREATE INDEX

Creates a B+ Tree index on the specified attribute of a relation. The relation must be open.

```
CREATE INDEX ON tablename.attributename
```

```sql
CREATE INDEX ON Students.Rollno
```

---

#### DROP INDEX

Drops the B+ Tree index on the specified attribute. The relation must be open.

```
DROP INDEX ON tablename.attributename
```

```sql
DROP INDEX ON Students.Rollno
```

---

#### ALTER TABLE RENAME

Renames an existing relation.

```
ALTER TABLE RENAME tablename TO new_tablename
```

```sql
ALTER TABLE RENAME Students TO UGStudents
```

---

#### ALTER TABLE RENAME COLUMN

Renames an attribute of an existing relation.

```
ALTER TABLE RENAME tablename COLUMN column_name TO new_column_name
```

```sql
ALTER TABLE RENAME Students COLUMN CGPA TO GPA
```

---

### Data Manipulation Language (DML)

DML commands insert and query records. Supported by both interfaces.

---

#### INSERT INTO TABLE VALUES

Inserts a single record into a relation. Values must match the order and types of attributes.

```
INSERT INTO tablename VALUES (value1, value2, ...)
```

```sql
INSERT INTO Students VALUES (1, Alice, 9.2)
```

---

#### INSERT INTO TABLE FROM FILE

Inserts multiple records from a CSV file. The file must be placed in `NITCbase/Files/Input_Files/`.

```
INSERT INTO tablename VALUES FROM filename.csv
```

```sql
INSERT INTO Students VALUES FROM students.csv
```

Each line in the CSV corresponds to one record, in the same attribute order as the relation.

---

#### SELECT \* FROM TABLE

Copies all records from a source relation into a new target relation.

```
SELECT * FROM source_relation INTO target_relation
```

```sql
SELECT * FROM Students INTO Students_Copy
```

---

#### SELECT Attrlist FROM TABLE

Projects specific attributes from a source relation into a new target relation.

```
SELECT Attr1, Attr2, ... FROM source_relation INTO target_relation
```

```sql
SELECT Name, CGPA FROM Students INTO NameGPA
```

---

#### SELECT \* FROM TABLE WHERE

Filters records from a source relation based on a condition and writes them to a new relation.

```
SELECT * FROM source_relation INTO target_relation WHERE attrname OP value
```

Supported operators: `=`, `>`, `<`, `>=`, `<=`, `!=`

```sql
SELECT * FROM Students INTO TopStudents WHERE CGPA > 8.5
```

---

#### SELECT Attrlist FROM TABLE WHERE

Projects specific attributes with a filter condition.

```
SELECT Attr1, Attr2, ... FROM source_relation INTO target_relation WHERE attrname OP value
```

```sql
SELECT Name, CGPA FROM Students INTO TopStudents WHERE CGPA >= 9
```

---

#### SELECT \* FROM JOIN WHERE

Performs an equi-join of two relations and writes the result to a new target relation.

```
SELECT * FROM relation1 JOIN relation2 INTO target_relation WHERE relation1.attr1 = relation2.attr2
```

```sql
SELECT * FROM Students1 JOIN Students2 INTO Merged WHERE Students1.Rollno = Students2.Rollno
```

> This automatically creates a B+ Tree index on the join attribute of the second relation if one doesn't exist.

---

#### SELECT Attrlist FROM JOIN WHERE

Equi-join with attribute projection.

```
SELECT Attr1, Attr2, ... FROM relation1 JOIN relation2 INTO target_relation WHERE relation1.attr1 = relation2.attr2
```

```sql
SELECT Rollno, Name, Marks FROM Students1 JOIN Students2 INTO Result WHERE Students1.Rollno = Students2.Rollno
```

---

### Utility Commands

Available in both interfaces.

| Command          | Description                                                                                 |
| ---------------- | ------------------------------------------------------------------------------------------- |
| `help`           | Lists all available commands with brief descriptions                                        |
| `exit`           | Exits the interface                                                                         |
| `echo <message>` | Prints the given message — useful for debugging in scripts                                  |
| `run <filename>` | Executes commands line-by-line from a batch file in `NITCbase/Files/Batch_Execution_Files/` |

**Example — batch execution:**

```bash
run setup/init_db
```

If any command in the script fails, execution halts and the failing line number is reported.

---

## File Organization

```
NITCbase/
├── Files/
│   ├── Input_Files/        # CSV files for bulk INSERT
│   └── Batch_Execution_Files/  # Scripts for the `run` command
├── Disk_Class/
├── Buffer/
├── Cache/
├── BlockAccess/
├── BPlusTree/
├── Schema/
├── Algebra/
├── Frontend/
└── FrontendInterface/
```

---

## Implementation Summary

This project involved implementing approximately **3000 lines of C++** across all layers. Key components built:

- **Buffer Layer** — LRU block cache with `readBlock`/`writeBlock` support
- **Cache Layer** — Relation and attribute cache for open relations
- **Block Access Layer** — Core linear and indexed scan, insert, delete
- **B+ Tree Layer** — Full B+ Tree insertion and search for indexing
- **Schema Layer** — All DDL operations wired to lower layers
- **Algebra Layer** — All DML operations including equi-join
- **Frontend Programming Interface** — Bridges user queries to Schema/Algebra layers

---

## Reference

- [NITCbase Official Documentation](https://nitcbase.github.io)
- [Architecture Overview](https://nitcbase.github.io/docs/Design/Architecture)
- [Command Reference](https://nitcbase.github.io/docs/User%20Interface%20Commands/)
- [B+ Tree Internals](https://nitcbase.github.io/docs/Misc/B+%20Trees)

---

_Built as part of the NITCbase RDBMS Implementation Project — NIT Calicut._
