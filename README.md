# GoEnt
GO Entity or just "GoEnt" is an easy-to-use ORM for Go

[![test status](https://github.com/azhai/goent/actions/workflows/tests.yml/badge.svg "test status")](https://github.com/azhai/goent/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/azhai/goent)](https://goreportcard.com/report/github.com/azhai/goent)
[![Go.Dev reference](https://img.shields.io/badge/go.dev-reference-blue?logo=go&logoColor=white)](https://pkg.go.dev/github.com/azhai/goent)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

![GoEnt Logo](goent-logo.svg)

## Requirements
- go v1.26 or above

## Real world examples
GoEnt has [examples](https://github.com/azhai/goent/tree/main/examples) of queries and integrations, if you think in a nice example and wanted to see if GoEnt can handler, just try out.

Common examples are:

- How to use GoEnt with other frameworks;
- Where GoEnt key features can shine;

## Features
Check out the [Benchmarks](#benchmarks) section for a overview on GoEnt performance compared to other packages like, ent, GORM, sqlc, and others.

- 🔖 Type Safety;
	- get errors on compile time
- 📦 Auto Migrations;
	- automatic generate tables from your structs
- 📄 SQL Like queries; 
	- use Go code to write queries with well known functions
- 🗂️ Iterator
	- range over function to iterate over the rows
- 📚 Pagination
	- paginate your large selects with just a function call
- ♻️ Wrappers
	- wrappers for simple queries and builders for complex ones


## Content
- [Install](#install)
- [Available Drivers](#available-drivers)
    - [PostgreSQL](#postgresql)
	- [SQLite](#sqlite)
- [Quick Start](#quick-start)
- [Database](#database)
	- [Supported Types](#supported-types)
	- [Struct Mapping](#struct-mapping)
	- [Setting primary key](#setting-primary-key)
	- [Setting type](#setting-type)
	- [Setting null](#setting-null)
	- [Setting default](#setting-default)
	- [Relationship](#relationship)
		- [One to One](#one-to-one)
		- [Many to One](#many-to-one)
		- [Many to Many](#many-to-many)
		- [Self Referential](#self-referential)
	- [Index](#index)
		- [Create Index](#create-index)
		- [Unique Index](#unique-index)
		- [Two Columns Index](#two-columns-index)
		- [Function Index](#function-index)
	- [Schemas](#schemas)
	- [Logging](#logging)
	- [Open](#open)
	- [Migrate](#migrate)
		- [Auto Migrate](#auto-migrate)
		- [Drop and Rename](#drop-and-rename)
		- [Migrate to a SQL file](#migrate-to-a-sql-file) 
- [Select](#select)
	- [Find](#find)
	- [Select Specific Fields](#select-specific-fields)
	- [Select Iterator](#select-iterator)
	- [Where](#where)
	- [Filter (Non-Zero Dynamic Where)](#filter-non-zero-dynamic-where)
	- [Match (Non-Zero Dynamic Where)](#match-non-zero-dynamic-where)
	- [Join](#join)
	- [Order By](#order-by)
	- [Group By](#group-by)
	- [Pagination](#pagination)
	- [Aggregates](#aggregates)
	- [Functions](#functions)
- [Insert](#insert)
	- [Insert One](#insert-one)
	- [Insert Batch](#insert-batch)
- [Update](#update)
	- [Save](#save)
	- [Update Set](#update-set)
- [Delete](#delete)
	- [Delete Batch](#delete-batch)
- [Transaction](#transaction)
	- [Begin Transaction](#begin-transaction)
	- [Manual Transaction](#manual-transaction)
		- [Commit and Rollback](#commit-and-rollback)
		- [Save Point](#save-point)
- [Benchmarks](#benchmarks)
- [Code Generation](#code-generation)

## Install
```
go get github.com/azhai/goent
```
As any database/sql support in go, you have to get a specific driver for your database, check [Available Drivers](#available-drivers)

## Available Drivers
* PostgreSQL
* SQLite
```
go get github.com/azhai/goent
```

#### Usage
```go
import (
	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
)

type Animal struct {
	// animal fields
}

type Database struct {
	Animal *goent.Table[Animal]
	*goent.DB
}

dsn := "postgres://dba:pass@127.0.0.1:5432/test?sslmode=disable"
db, err := goent.Open[Database](pgsql.Open(dsn), "stdout")
// db, err := goent.Open[Database](sqlite.Open("goent.db"), "stdout")
```

## Quick Start
```go
package main

import (
	"fmt"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type Animal struct {
	ID    int
	Name  string
	Emoji string
}

type PublicSchema struct {
	Animal *goent.Table[Animal]
}

type Database struct {
	PublicSchema `goe:"public"`
	*goent.DB
}

func main() {
	db, err := goent.Open[Database](sqlite.Open("goent.db"), "stdout")
	if err != nil {
		panic(err)
	}
	defer goent.Close(db)

	err = goent.AutoMigrate(db)
	if err != nil {
		panic(err)
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		panic(err)
	}

	animals := []*Animal{
		{Name: "Cat", Emoji: "🐈"},
		{Name: "Dog", Emoji: "🐕"},
		{Name: "Rat", Emoji: "🐀"},
		{Name: "Pig", Emoji: "🐖"},
		{Name: "Whale", Emoji: "🐋"},
		{Name: "Fish", Emoji: "🐟"},
		{Name: "Bird", Emoji: "🐦"},
	}

	// Insert all animals in a single transaction
	// retPK = false is 5 times faster than retPK = true
	err = db.Animal.Insert().All(false, animals)
	if err != nil {
		panic(err)
	}

	animals, err = db.Animal.Select().All()
	if err != nil {
		panic(err)
	}
	fmt.Println(animals)
}
```

To run the quick start follow this steps:

1. Init the go.mod file
	```bash
	go mod init quickstart
	```

2. Get the necessary packages:
	```bash
	go mod tidy
	```

3. Run the example:
	```bash
	go run main.go
	```

4. If everything was ok, you should see a output like this:
	```
	[{1 Cat 🐈} {2 Dog 🐕} {3 Rat 🐀} {4 Pig 🐖} {5 Whale 🐋} {6 Fish 🐟} {7 Bird 🐦}]
	```

## Database
```go
type Database struct {
	User    	*goent.Table[User]
	Role    	*goent.Table[Role]
	UserLog 	*goent.Table[UserLog]
	*goent.DB
}
```
In goe, it's necessary to define a Database struct,
this struct implements *goent.DB and a pointer to all
the structs that's it's to be mappend.

It's through the Database struct that you will
interact with your database.

### Supported Types

GoEnt supports any type that implements the [Scanner Interface](https://pkg.go.dev/database/sql#Scanner). Most common are sql.Null types from database/sql package.

```go
type Table struct {
	Price      decimal.Decimal     `goe:"type:decimal(10,4)"`
	NullID     sql.Null[uuid.UUID] `goe:"type:uuid"`
	NullString sql.NullString      `goe:"type:varchar(100)"`
}
```

[Back to Contents](#content)
### Struct mapping
```go
type User struct {
	ID        	uint //this is primary key
	Login     	string
	Password  	string
}
```
> [!NOTE] 
> By default the field "ID" is primary key and all ids of integers are auto increment.

[Back to Contents](#content)
### Table Name Resolution

Table names are resolved in the following order:
1. **`TableName()` method** - If the struct implements `TableName() string`, that value is used directly
2. **Struct name in snake_case with prefix** - If no `TableName()` method, the struct name is converted to snake_case, with optional `prefix:` from schema tag prepended

The schema tag format is: `goe:"schema_name;prefix:table_prefix"`

```go
// Method 1: TableName() method (returns exact table name, no prefix added)
type OrderDetail struct {
    OrderID   int64 `goe:"pk;not_incr"`
    ProductID int64 `goe:"pk;not_incr"`
}

func (*OrderDetail) TableName() string {
    return "t_order_detail"  // Exact table name, prefix is ignored
}

// Method 2: Auto-generated from struct name with prefix
type PublicSchema struct {
    User     *goent.Table[User]     // Table: t_user (with prefix t_)
    Category *goent.Table[Category] // Table: t_category
}

type Database struct {
    PublicSchema `goe:"public;prefix:t_"`  // schema=public, prefix=t_
    *goent.DB
}

// Without prefix
type AuthSchema struct {
    Role *goent.Table[Role]  // Table: role (no prefix)
}

type Database struct {
    AuthSchema `goe:"auth"`  // schema=auth, no prefix
    *goent.DB
}
```

[Back to Contents](#content)
### Setting primary key
```go
type User struct {
	Identifier	uint `goe:"pk"`
	Login     	string
	Password	string
}
```
In case you want to specify 
a primary key use the tag value "pk".

#### Composite Primary Key
```go
type OrderDetail struct {
	OrderID   int `goe:"pk;not_incr"`
	ProductID int `goe:"pk;not_incr"`
	Quantity  int
}
```
Use multiple `pk` tags to create a composite primary key. Add `not_incr` to prevent auto-increment on primary key columns.

#### Non-Auto-Increment Primary Key
```go
type User struct {
	ID   string `goe:"pk;not_incr;default:uuid_generate_v4()"`
	Name string
}
```
Use `not_incr` tag to prevent auto-increment behavior on primary key columns. This is useful for UUID or string primary keys.

[Back to Contents](#content)
### Setting type
```go
type User struct {
	ID       	string `goe:"pk;type:uuid"`
	Login    	string `goe:"type:varchar(10)"`
	Name     	string `goe:"type:varchar(150)"`
	Password 	string `goe:"type:varchar(60)"`
}
```
You can specify a type using the tag value "type"

[Back to Contents](#content)

### Setting null
```go
type User struct {
	ID        int
	Name      string
	Email     *string // this will be a null column
	Phone     sql.NullString `goe:"type:varchar(20)"` // also null
}
```

> [!IMPORTANT] 
> A pointer is considered a null column in Database.

[Back to Contents](#content)

### Setting default

```go
type User struct {
	ID        int
	Name      string
	Email     *string
	CreatedAt  time.Time `goe:"default:current_timestamp"`
}
```

A default value is used when the field is inserted with no value.

```go
// CreatedAt will have the default value
err = db.User.Insert().One(&User{Name: "Rose"})

if err != nil {
	// handler error
}
```

#### Primary Key with Default Value

For non-auto-increment primary keys, you can specify a default value:

```go
type Default struct {
	ID   string `goe:"default:'Default'"` // Primary key with default value
	Name string
}
```

When inserting, the default value is automatically set on the struct:

```go
// d := Default{Name: "Test"}
// err = db.Default.Insert().One(&d)

// recommend:
d := &Default{Name: "Test"}
err = db.Default.Insert().One(d)

// d.ID == "Default" (set from default value)

if err != nil {
	// handler error
}
```

> [!NOTE]
> For primary keys with default values, the value is set on the struct before INSERT, and the column is included in the INSERT statement. This is different from auto-increment primary keys, where the column is excluded and the ID is retrieved using `last_insert_rowid()`.

[Back to Contents](#content)

### Relationship
In GoEnt relational fields are created using the pattern `TargetTable`+`TargetTableID`, so if you want to have a foreign key to User, you will have to write a field like `UserID` or `UserIDOrigin`.
#### One To One
```go
type User struct {
	ID       	uint
	Login    	string
	Name     	string
	Password 	string
}

type UserDetails struct {
	ID       	uint
	Email   	string
	Birthdate 	time.Time
	UserID   	uint  // one to one with User
}
```

[Back to Contents](#content)
#### Many To One
**For simplifications all relational slices should be the last fields on struct.**
```go
type User struct {
	ID       	uint
	Name     	string
	Password 	string
	UserLogs 	[]UserLog // one User has many UserLogs
}

type UserLog struct {
	ID       	uint
	Action   	string
	DateTime 	time.Time
	UserID   	uint // if remove the slice from user, will become a one to one
}
```

The difference from one to one and many to one it's a slice field on the "many" struct.

[Back to Contents](#content)
#### Many to Many
**For simplifications all relational slices should be the last fields on struct.**

Using implicit many to many:

```go
type Person struct {
	ID   int
	Name string
	Jobs []Job // Person has a slice to Jobs
}

// Person and Job are implicit relational
type PersonJob struct {
	PersonID   int `goe:"pk"`
	JobID      int `goe:"pk"`
	CreatedAt  time.Time
}

type Job struct {
	Name    string
	ID      int
	Persons []Person // Job has a slice to Person
}
```

> [!IMPORTANT]
> It's used the tags "pk" for ensure that the foreign keys will be both primary key.

Using many to one pattern:

```go
type User struct {
	ID       	uint
	Name     	string
	Password 	string
	UserRoles 	[]UserRole
}

type UserRole struct {
	UserID  	uint `goe:"pk"`
	RoleID  	uint `goe:"pk"`
}

type Role struct {
	ID        	uint
	Name      	string
	UserRoles 	[]UserRole
}
```
Is used a combination of two many to one to generate a many to many. In this example, User has many UserRole and Role has many UserRole.

> [!IMPORTANT]
> It's used the tags "pk" for ensure that the foreign keys will be both primary key.

[Back to Contents](#content)

#### Self-Referential

One to Many

```go
type Person struct {
	ID       int
	Name     string
	PersonID *int
	Family   []Person
}
```

One to One

```go
type Page struct {
	ID         int
	Number     int
	PageIDNext *int
	PageIDPrev *int
}
```

[Back to Contents](#content)
### Index
#### Unique Index
```go
type User struct {
	ID       	uint
	Name     	string
	Email    	string  `goe:"unique"`
}
```
To create a unique index you need the "unique" goe tag

[Back to Contents](#content)
#### Create Index
```go
type User struct {
	ID       uint
	Name     string
	Email 	 string `goe:"index"`
}
```
To create a common index you need the "index" goe tag

[Back to Contents](#content)
<!-- #### Function Index
```
type User struct {
	ID       uint
	Name     string
	Email    string `goe:"index(n:idx_email f:lower)"`
}
```
> To create a function index you need to pass the "f" parameter with the function name -->
#### Two Columns Index
```go
type User struct {
	ID       uint
	Name    string `goe:"index(n:idx_name_status)"`
	Email   string `goe:"index(n:idx_name_status);unique"`
}
```

Using the goe tag "index()", you can pass the index infos as a function call. "n:" is a parameter for name, to have a two column index just need two indexes with same name. You can use the semicolon ";" to create another single index for the field.

[Back to Contents](#content)

#### Two Columns Unique Index
```go
type User struct {
	ID       uint
	Name    string `goe:"index(unique n:idx_name_status)"`
	Email   string `goe:"index(unique n:idx_name_status);unique"`
}
```

Just as creating a [Two Column Index](#two-columns-index) but added the "unique" value inside the index function.

[Back to Contents](#content)

#### Function Index
```go
type User struct {
	Id        int
	Name      string `goe:"index(n:idx_name_lower f:lower)"`
	Email     string `goe:"unique"`
	UserRoles []UserRole
}
```

Use the `f:` parameter to pass a function in the index tag.

[Back to Contents](#content)


## Schemas

On GoEnt it's possible to create schemas by the database struct, all schemas should have the suffix `Schema`
or a tag `goe:"schema"`.

> [!IMPORTANT]
> Tables must be nested under schema structs. The hierarchy is: **Database -> Schema -> Table**.
> SQLite ignores schema names (tables are created directly in the main database).

```go
type User struct {
	...
}

type UserRole struct {
	...
}

type Role struct {
	...
}
// schema with suffix Schema
type UserSchema struct {
	User     *goent.Table[User]
	UserRole *goent.Table[UserRole]
	Role     *goent.Table[Role]
}
// schema with any name
type Authentication struct {
	User     *goent.Table[User]
	UserRole *goent.Table[UserRole]
	Role     *goent.Table[Role]
}

type Database struct {
	*UserSchema // all structs on UserSchema will be created inside user schema
	*Authentication `goe:"schema"` // will create Authentication schema
	*goent.DB
}
```

> [!TIP]
> On SQLite any schema will be a new attached db file.

[Back to Contents](#content)

## Logging

GoEnt supports any logger that implements the Logger interface

```go
type Logger interface {
	InfoContext(ctx context.Context, msg string, kv ...any)
	WarnContext(ctx context.Context, msg string, kv ...any)
	ErrorContext(ctx context.Context, msg string, kv ...any)
}
```

The logger is defined on database opening
```go
	db, err := drivers.QuickOpen[Database]("sqlite", "goent.db", "stdout")
```

> [!TIP]
> You can use slog as your standard logger or make a adapt over the Logger interface.

[Back to Contents](#content)

## Open
To open a database use `goent.Open` function, it's require a valid driver. Most of the drives will require a dsn/path connection and a config setup. On `goent.Open` needs to specify the struct database.

If you don't need the database connection anymore, call `goent.Close` to ensure that all the database resources will be removed from memory.

```go
type Database struct {
	Animal         *goent.Table[Animal]
	AnimalFood     *goent.Table[AnimalFood]
	Food           *goent.Table[Food]
	*goent.DB
}

dsn := "user=postgres password=postgres host=localhost port=5432 database=postgres"
db, err := facade.QuickOpen[Database]("pgsql", dsn, "")

if err != nil {
	// handler error
}
```

[Back to Contents](#content)
## Migrate

### Auto Migrate

To auto migrate the structs, use the `goent.AutoMigrate(db)` passing the database returned by `goent.Open`.

```go
// migrate all database structs
err = goent.AutoMigrate(db)
if err != nil {
	// handler error
}
```
[Back to Contents](#content)
### Drop and Rename

```go
type Select struct {
	ID   int
	Name string
}

type Database struct {
	Select         *goent.Table[Select]
	*goent.DB
}

err = goent.AutoMigrate(db).OnTable("Select").RenameColumn("Name", "NewName")
if err != nil {
	// handler error
}

err = goent.AutoMigrate(db).OnTable("Select").DropColumn("NewName")
if err != nil {
	// handler error
}

err = goent.AutoMigrate(db).OnTable("Select").RenameTable("NewSelect")
if err != nil {
	// handler error
}

err = goent.AutoMigrate(db).OnTable("NewSelect").DropTable()
if err != nil {
	// handler error
}
```
[Back to Contents](#content)
### Migrate to a SQL file

GoEnt drivers supports a output migrate path to specify a directory to store the generated SQL. In this way, calling the "AutoMigrate" function goe WILL NOT auto apply the migrations and output the result as a sql file in the specified path.

```go
// open the database with the migrate path config setup
db, err := goent.Open[Database](sqlite.Open("goent.db", sqlite.Config{
	MigratePath: "migrate/",
}))
if err != nil {
	// handler error
}

// AutoMigrate will output the result as a sql file, and not auto apply the migration
err = goent.AutoMigrate(db)
if err != nil {
	// handler error
}
```

In this example the file will be output in the "migrate/" path, as follow:

```
📂 migrate
|   ├── SQLite_1760042267.sql
go.mod
```

> [!TIP]
> Any other migration like "DropTable", "RenameColumn" and others... will have the same result as "AutoMigrate", and will generate the SQL file.

[Back to Contents](#content)
## Select
### Find
Find is used when you want to return a single result.
```go
// one primary key
animal, err := db.Animal.Select().Match(Animal{ID: 2}).One()

// two primary keys
animalFood, err := db.AnimalFood.Select().Match(AnimalFood{IDAnimal: 3, IDFood: 2}).One()

// find record by value, if have more than one it will returns the first
cat, err := db.Animal.Select().Match(Animal{Name: "Cat"}).One()
```

> [!TIP]
> Use **goent.SelectContext** for specify a context.

[Back to Contents](#content)
### Select

Select has support for [OrderBy](#orderby), [Pagination](#pagination) and [Join](#join).

```go
// select all animals
animals, err = db.Animal.Select().All()

// select the animals with name "Cat", ID "3" and IDHabitat "4"
animals, err = db.Animal.Select().Filter(EqualsMap(db.Animal.Field("id"), map[string]any{"name": "Cat", "id": 3})).All()

// when using % on filter, goent makes a like operation
animals, err = db.Animal.Select().Match(Animal{Name: "%Cat%"}).All()
```

> [!TIP]
> Use **goent.SelectContext** for specify a context.

[Back to Contents](#content)
### Select Iterator

Iterate over the rows
```go
for row, err := range db.Animal.Select().IterRows() {
	// iterator rows
 }
```

[Back to Contents](#content)

### Select Specific Fields
```go
var result []struct {
	User    string
	Role    *string
	EndTime *time.Time
}

// row is the generic struct
for row, err := range goent.Select[struct {
		User    string     // output row
		Role    *string    // output row
		EndTime *time.Time // output row
	}](db.User.Field("Name"), db.Role.Field("Name"), db.UserRole.Field("EndDate")).
	Join(goent.InnerJoin, db.UserRole.Table(), EqualsField(db.User.Field("id"), db.UserRole.Field("user_id"))).
	Join(goent.InnerJoin, db.Role.Table(), EqualsField(db.UserRole.Field("role_id"), db.Role.Field("id"))).
	OrderBy("id").IterRows() {

	if err != nil {
		//handler error
	}
	//handler rows
	result = append(result, row)
}
```

For specific field is used a new struct, each new field guards the reference for the database attribute.

[Back to Contents](#content)

### Where
Where conditions are created using goent functions like Equals, And, Or, In, Like, etc.
```go
animals, err = db.Animal.Select().Where("id = %s", 2).All()

if err != nil {
	//handler error
}
```

It's possible to group a list of where operations inside Filter()

```go
animals, err = db.Animal.Select().Filter(
		goent.And(
			goent.LessEquals(db.Animal.Field("ID"), 2), 
			goent.In(db.Animal.Field("Name"), []string{"Cat", "Dog"}),
		),
	).All()

if err != nil {
	//handler error
}
```

You can use a if to call a where operation only if it's match
```go
selectQuery := db.Animal.Select().Filter(goent.LessEquals(db.Animal.Field("ID"), 30))

if filter.In {
	selectQuery = selectQuery.Filter(
		goent.And(
			goent.LessEquals(db.Animal.Field("ID"), 30), 
			goent.In(db.Animal.Field("Name"), []string{"Cat", "Dog"}),
		),
	)
}

animals, err = selectQuery.All()

if err != nil {
	//handler error
}
```

It's possible to use a query inside a `goent.In`

```go
// use AsQuery() for get a result as a query
querySelect := goent.Select[any](db.Animal.Field("Name")).
					Join(goent.InnerJoin, db.AnimalFood.Table(), EqualsField(db.Animal.Field("id"), db.AnimalFood.Field("animal_id"))).
					Join(goent.InnerJoin, db.Food.Table(), EqualsField(db.AnimalFood.Field("food_id"), db.Food.Field("id"))).
					Filter(
						goent.In(db.Food.Field("Name"), []string{foods[0].Name, foods[1].Name})).
					AsQuery()

// where in with another query
a, err := db.Animal.Select().Filter(goent.In(db.Animal.Field("Name"), querySelect)).All()

if err != nil {
	//handler error
}
```
On where, GoEnt supports operations on two columns, all where operations that have `Arg` as suffix it's used for operation on columns.

In the example, the operator greater (>) on the columns Score and Minimum is used to return all exams that have a score greater than the minimum.

```go
err = db.Exam.Select().
	Filter(goent.GreaterArg[float32](db.Exam.Field("Score"), db.Exam.Field("Minimum"))).All()
```


[Back to Contents](#content)

### Filter (Non-Zero Dynamic Where)

Filter creates where operations on non-zero values, so if you want a dynamic where to show up only if has values, filter is the call.

```go
var s []string
a, err = db.Animal.Select().
	Filter(
		goent.And(goent.In(db.Animal.Field("Name"), s),
			goent.And(goent.Equals(db.Animal.Field("Id"), 0),
				goent.And(
					goent.Equals(db.Animal.Field("Name"), ""),
					goent.Like(db.Animal.Field("Name"), "%o%"), // valid filter
				),
			),
		),
	).
	OrderBy("id DESC").All()

if err != nil {
	//handler error
}
```

> [!TIP] 
> It's possible to call **Filter** and **Where** on the same query.

[Back to Contents](#content)

### Match (Non-Zero Dynamic Where)

Match creates where operations on non-zero values using the query model. Match uses a LIKE operator with the ToUpper function on all string values.

```go
// SELECT * FROM "status" where UPPER("status"."name") LIKE '%A%'
result, err := db.Status.Select().Match(Status{Name: "a"}).All()
if err != nil {
	//handler error
}
```

It's possible to use Match on Select

```go
// SELECT "animals"."name", "foods"."name" FROM "animals"
// JOIN "animal_foods" on ("animals"."id" = "animal_foods"."animal_id")
// JOIN "food_habitat_schema"."foods" on ("animal_foods"."food_id" = "foods"."id")
// WHERE UPPER("foods"."name") LIKE '%A%'
result, err := goent.Select[struct {
	AnimalName string
	FoodName   string
}](db.Animal.Field("Name"), db.Food.Field("Name")).Match(struct {
	AnimalName string
	FoodName   string
}{FoodName: "a"}).
	Join(goent.InnerJoin, db.AnimalFood.Table(), EqualsField(db.Animal.Field("id"), db.AnimalFood.Field("animal_id"))).
	Join(goent.InnerJoin, db.Food.Table(), EqualsField(db.AnimalFood.Field("food_id"), db.Food.Field("id"))).All()

if err != nil {
	//handler error
}
```

> [!TIP] 
> It's possible to call **Match** and **Where** on the same query.

[Back to Contents](#content)

### Join
Join operations use goent constants like InnerJoin, LeftJoin, RightJoin.

For the join operations, you need to specify the type, this make the joins operations more safe. So if you change a type from a field, the compiler will throw a error.
```go
animals, err = db.Animal.Select().
				Join(goent.InnerJoin, db.AnimalFood.Table(), EqualsField(db.Animal.Field("id"), db.AnimalFood.Field("animal_id"))).
				Join(goent.InnerJoin, db.Food.Table(), EqualsField(db.AnimalFood.Field("food_id"), db.Food.Field("id"))).
			   All()

if err != nil {
	//handler error
}
```

Same as where, you can use a if to only make a join if the condition match.

#### LeftJoin Helper Method

The `LeftJoin` method is a convenient helper for LEFT JOIN operations with automatic column selection:

```go
// LeftJoin automatically selects columns from the joined table
orderDetails, err := db.OrderDetail.Select().
    LeftJoin("product_id", db.Product.Field("id")).
    Filter(goent.Equals(db.OrderDetail.Field("order_id"), orderID)).
    All()

// The Product field will be populated for each OrderDetail
for _, detail := range orderDetails {
    if detail.Product != nil {
        fmt.Printf("Product: %s, Price: %.2f\n", detail.Product.Name, detail.Product.Price)
    }
}
```

The `LeftJoin` method:
- Automatically adds the joined table's columns to the SELECT list
- Creates the JOIN condition using the local foreign key field and the referenced field
- Supports chaining multiple joins

```go
// Multiple joins example
results, err := db.Person.Select().
    LeftJoin("id", db.PersonJobTitle.Field("person_id")).
    LeftJoin("job_title_id", db.JobTitle.Field("id")).
    Filter(goent.Equals(db.JobTitle.Field("name"), "Developer")).
    All()
```

> [!NOTE]
> `LeftJoin` only populates non-slice foreign fields. For slice relationships (e.g., `Jobs []JobTitle`), use the standard `Join` method with manual column selection.

[Back to Contents](#content)
### Order By
For OrderBy you need to pass a reference to a mapped database field.

It's possible to OrderBy desc and asc. Select has support for OrderBy queries.
```go
animals, err = db.Animal.Select().OrderBy("id DESC").All()

if err != nil {
	//handler error
}
```

### Group By
For GroupBy you need to pass a reference to a mapped database field.

It's possible to GroupBy by a aggregate.
#### Select
```go
habitatCount, err := goent.Select[struct {
	Name  string
	Count int64
}](db.Habitat.Field("Name"), aggregate.Count(db.Animal.Field("Id"))).Join(goent.InnerJoin, db.Habitat.Table(), EqualsField(db.Animal.Field("habitat_id"), db.Habitat.Field("id"))).
	OrderBy("count DESC").
	GroupBy("name").All()

if err != nil {
	//handler error
}
```

[Back to Contents](#content)
### Pagination
For pagination, it's possible to run on Select function

#### Select Pagination
```go
// page 1 of size 10
page, err := db.Animal.Select().Pagination(1, 10)

if err != nil {
	//handler error
}
```

> [!NOTE]
> Pagination default values for page and size are 1 and 10 respectively.

[Back to Contents](#content)
### Aggregates
Aggregate functions like Count, Sum, Avg are available as table methods.

```go
count, err := db.Animal.Count("id") // (int64, error)
```

[Back to Contents](#content)
### Functions
SQL functions like ToUpper can be called as table methods.

```go
names, err := db.Animal.ToUpper("name") // ([]string, error)
```

Functions can be used inside where.
```go
animals, err = db.Animal.Select().
			   Filter(
					goent.Expr("ToUpper(name) LIKE '%CAT%'"),
			   ).All()

if err != nil {
	//handler error
}
```

> [!NOTE] 
> where like expected a second argument always as string.

```go
animals, err = db.Animal.Select().
			   Filter(
					goent.Expr("ToUpper(name) LIKE ?", "%CAT%"),
			   ).All()

if err != nil {
	//handler error
}
```

> [!IMPORTANT]
> to by pass the compiler type warning, use function.Argument. This way the compiler will check the argument value.

[Back to Contents](#content)
## Insert
On Insert if the primary key value is auto-increment, the new ID will be stored on the object after the insert.

> [!NOTE]
> For auto-increment primary keys, the column is excluded from INSERT and the ID is retrieved using `last_insert_rowid()`. For primary keys with default values, the value is set before INSERT and the column is included in the statement.

### Insert One
```go
a := &Animal{Name: "Cat", Emoji: "🐘"}
err = db.Animal.Insert().One(a)

if err != nil {
	//handler error
}

// new generated id
a.ID
```

> [!TIP] 
> Use **goent.InsertContext** for specify a context.

[Back to Contents](#content)
### Insert Batch
```go
foods := []*Food{
		{Name: "Meat", Emoji: "🥩"},
		{Name: "Hotdog", Emoji: "🌭"},
		{Name: "Cookie", Emoji: "🍪"},
	}
err = db.Food.Insert().All(true, foods)

if err != nil {
	//handler error
}
```

> [!NOTE]
> The first parameter of `All()` is `autoIncr`. When `true`, it only applies to tables with auto-increment primary keys. For tables with non-auto-increment primary keys (like UUID or string with default), the primary key column is included in the INSERT statement.

> [!TIP] 
> Use **goent.InsertContext** for specify a context.

[Back to Contents](#content)
## Update
### Save
Save is the basic function for updates a single record; 
only updates the non-zero values.
```go
a := &Animal{ID: 2}
a.Name = "Update Cat"

// update animal of id 2
err = db.Animal.Save().One(a)
changes := map[string]any{
	"name": a.Name,
}
filter := goent.Equals(db.Animal.Field("id"), a.ID)
err = db.Animal.Save().Filter(filter).SetMap(changes).Exec()

if err != nil {
	//handler error
}
```

> [!TIP] 
> Use **goent.SaveContext** for specify a context.

[Back to Contents](#content)

### Update Set
Update with set uses Set method. This is used for more complex updates, like updating a field with zero/nil values or make a batch update.

```go
a := Animal{ID: 2}

// a.IDHabitat is nil, so is ignored by Save
err = db.Animal.Update().
	  Set(goent.Pair{Key: "habitat_id", Value: a.IDHabitat}).
	  Filter(goent.Equals(db.Animal.Field("id"), a.ID)).Exec()

if err != nil {
	//handler error
}
```

Check out the [Where](#where) section for more information about where operations.

> [!CAUTION]
> The where call ensures that only the matched rows will be updated.

> [!TIP] 
> Use **goent.UpdateContext** for specify a context.

[Back to Contents](#content)
## Delete

### Delete Batch
Delete all records from Animal
```go
err = db.Animal.Delete().Exec()

if err != nil {
	//handler error
}
```

Delete one record by primary key
```go
err = db.Animal.Delete().Match(Animal{ID: 2}).Exec()

if err != nil {
	//handler error
}
```

Delete all matched records
```go
err = db.Animal.Delete().Filter(goent.Like(db.Animal.Field("name"), "%Cat%")).Exec()

if err != nil {
	//handler error
}
```

Check out the [Where](#where) section for more information about where operations.

> [!CAUTION]
> The filter call ensures that only the matched rows will be deleted.

> [!TIP]
> Use **goent.DeleteContext** for specify a context.

[Back to Contents](#content)

## Transaction

### Begin Transaction

```go
	err = db.BeginTransaction(func(tx goent.Transaction) error {
		cat := &Animal{
			Name: "Cat",
		}
		if err = db.Animal.Insert().OnTransaction(tx).One(cat); err != nil {
			return err // try a rollback
		}

		dog := &Animal{
			Name: "Dog",
		}
		if err = db.Animal.Insert().OnTransaction(tx).One(dog); err != nil {
			return err // try a rollback
		}
		return nil // try a commit
	})

	if err != nil {
		//begin transaction error...
	}
```

Nested Transaction

```go
err = db.BeginTransaction(func(tx goent.Transaction) error {
	cat := &Animal{
		Name: "Cat",
	}
	if err = db.Animal.Insert().OnTransaction(tx).One(cat); err != nil {
		return err // try a rollback
	}

	tx.BeginTransaction(func(tx2 goent.Transaction) error {
		meat := &Food{
			Name: "meat",
		}
		if err := db.Food.Insert().OnTransaction(tx2).One(meat); err != nil {
			return err // try a rollback in nested transaction
		}
		return nil // try a commit in nested transaction
	})

	dog := &Animal{
		Name: "Dog",
	}
	if err = db.Animal.Insert().OnTransaction(tx).One(dog); err != nil {
		return err // try a rollback
	}
	return nil // try a commit
})

if err != nil {
	//begin transaction error...
}
```

You need to call the `OnTransaction()` function to setup a transaction for [Select](#select), [Insert](#insert), [Update](#update) and [Delete](#delete).

> [!NOTE]
> Any select inside a transaction will be "FOR UPDATE".

> [!TIP]
> Use **goent.BeginTransactionContext** for specify a context

[Back to Contents](#content)

### Manual Transaction
Setup the transaction with the database function `db.NewTransaction()`
```go
tx, err = db.NewTransaction()
if err != nil {
	// handler error
}

defer func() {
	if r := recover(); r != nil {
		tx.Rollback()
	}
}()
```

You need to call the `OnTransaction()` function to setup a transaction for [Select](#select), [Insert](#insert), [Update](#update) and [Delete](#delete).

> [!NOTE]
> Any select inside a transaction will be "FOR UPDATE".

> [!TIP]
> Use **goent.NewTransactionContext** for specify a context

[Back to Contents](#content)

#### Commit and Rollback

To Commit a Transaction just call `tx.Commit()`
```go
err = tx.Commit()

if err != nil {
	// handler the error
}
```

To Rollback a Transaction just call `tx.Rollback()`
```go
err = tx.Rollback()

if err != nil {
	// handler the error
}
```

[Back to Contents](#content)

#### Save Point

```go
sv, err := tx.SavePoint()
if err != nil {
	// handler the error
}
defer func() {
	if r := recover(); r != nil {
		sv.Rollback() // rollback save point
	}
}()

...

sv.Commit() // commit save point
```

## Benchmarks

Source code of benchmarks can be find on [azhai/go-orm-benchmarks](https://github.com/azhai/go-orm-benchmarks) or [lauro-santana/go-orm-benchmarks](https://github.com/lauro-santana/go-orm-benchmarks). 

```bash
go run main.go -orm goe -operation all
go run main.go -orm goent -operation all
```

### Benchmark on MacMini M4
| Operation       | Package |     N | Avg ns/op | Avg B/op | Avg allocs/op |
|-----------------|---------|------:|----------:|---------:|--------------:|
| **insert**      | goe     | 14930 |     81200 |     2644 |            31 |
|                 | goent   | 14835 |     84908 |     3402 |            67 |
| **insert-bulk** | goe     |   174 |   9098946 |  5202188 |         28013 |
|                 | goent   |   133 |   9688873 |  8752617 |         46120 |
| **update**      | goe     | 14932 |     77634 |     2593 |            27 |
|                 | goent   | 14541 |     83918 |     3423 |            63 |
| **delete**      | goe     | 45292 |     26490 |     1051 |            15 |
|                 | goent   |  1226 |   1543646 |     1398 |            34 |
| **select-one**  | goe     | 41014 |     29327 |     3508 |            54 |
|                 | goent   | 34629 |     33767 |     4340 |            85 |
| **select-page** | goe     |  3415 |    341912 |    55400 |           870 |
|                 | goent   |  3132 |    388181 |    83605 |          1722 |

[Back to Contents](#content)

## Code Generation

GoEnt provides a code generator that creates type-safe scan methods for your structs, eliminating reflection overhead and improving performance by up to 27x.

### Quick Start

```bash
git clone --depth=1 https://github.com/azhai/goent.git
cd goent && go mod tidy && make
./bin/goent-gen ./example/models
# view file ./example/models/goent_gen.go
```

### Install

```bash
go install github.com/azhai/goent/cmd/goent-gen@latest
```

### Usage

1. Add `//go:generate goent-gen .` to your models package:

```go
//go:generate goent-gen .

package models

type User struct {
    ID       int64  `goe:"pk"`
    Name     string `goe:"unique"`
    Email    string `goe:"unique"`
    StatusID int64  `goe:"m2o"`
}
```

2. Run the generator:

```bash
go generate ./models
```

3. This creates `goent_gen.go` with:

```go
// Code generated by goent-gen. DO NOT EDIT.

package models

import "github.com/azhai/goent"

// ScanFields returns a slice of pointers to User fields for database scanning.
func (t *User) ScanFields() []any {
    return []any{
        &t.ID,
        &t.Name,
        &t.Email,
        &t.StatusID,
    }
}

// NewUser creates a new User with pre-allocated scan fields.
func NewUser() *User {
    return &User{}
}

// FetchUser creates a FetchFunc for User.
func FetchUser() goent.FetchFunc {
    return func(target any) []any {
        return target.(*User).ScanFields()
    }
}
```

### Performance Comparison

| Method | Time | Memory | Allocations |
|--------|------|--------|-------------|
| **Generated ScanFields** | 1.68 ns/op | 0 B/op | 0 allocs/op |
| Reflection-based | 25.04 ns/op | 24 B/op | 1 allocs/op |
| **Generated FetchFunc** | 1.68 ns/op | 0 B/op | 0 allocs/op |
| Reflection-based | 45.29 ns/op | 48 B/op | 2 allocs/op |

Generated code is **15-27x faster** than reflection with **zero memory allocations**.

### Using Generated Code

```go
// Use generated FetchFunc with Select
users, err := db.User.Select().QueryRows(models.FetchUser())

// Or use ScanFields directly
user := models.NewUser()
rows.Scan(user.ScanFields()...)
```

[Back to Contents](#content)
