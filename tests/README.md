```
go get -u -t
```

```
docker compose up -d
```

```
DB_TYPE=SQLite go test . -v -race -count=1
```

```
docker compose down
```

```
DB_TYPE=SQLite go test -bench Select -benchmem -run ^$

DB_TYPE=SQLite go test -bench Join -benchmem -run ^$
```