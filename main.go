package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strings"
)

var (
	config   Configuration
	defaults = Configuration{
		Host:       "localhost",
		Port:       3306,
		DbUser:     "db_user",
		DbPassword: "db_pw",
		DbName:     "bd_name",
		PkgName:    "DbStructs",
		TagLabel:   "db",
	}
	configFile = flag.String("json", "", "Config file")
	output     = flag.String("out", "-", "Output")
)

type Configuration struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	DbUser     string `json:"db_user"`
	DbPassword string `json:"db_password"`
	DbName     string `json:"db_name"`
	// PkgName gives name of the package using the stucts
	PkgName string `json:"pkg_name"`
	// TagLabel produces tags commonly used to match database field names with Go struct members
	TagLabel string `json:"tag_label"`
}

type ColumnSchema struct {
	TableName              string
	ColumnName             string
	IsNullable             string
	DataType               string
	CharacterMaximumLength sql.NullInt64
	NumericPrecision       sql.NullInt64
	NumericScale           sql.NullInt64
	ColumnType             string
	ColumnKey              string
}

func writeStructs(schemas []ColumnSchema) (int, error) {
	var buffer bytes.Buffer

	currentTable := ""
	neededImports := make(map[string]bool)

	for _, cs := range schemas {
		if cs.TableName != currentTable {
			if currentTable != "" {
				buffer.WriteString("}\n\n")
			}
			buffer.WriteString("type " + formatName(cs.TableName) + " struct{\n")
		}

		goType, requiredImport, err := goType(&cs)
		if requiredImport != "" {
			neededImports[requiredImport] = true
		}

		if err != nil {
			log.Fatal(err)
		}

		buffer.WriteString("\t" + formatName(cs.ColumnName) + " " + goType)

		if len(config.TagLabel) > 0 {
			buffer.WriteString("\t`" + config.TagLabel + ":\"" + cs.ColumnName + "\"`")
		}

		buffer.WriteString("\n")

		currentTable = cs.TableName

	}

	buffer.WriteString("}")

	// Now add the header section
	header := bytes.NewBufferString("package " + config.PkgName + "\n\n")

	if len(neededImports) > 0 {
		header.WriteString("import (\n")

		for imp := range neededImports {
			header.WriteString("\t\"" + imp + "\"\n")
		}

		header.WriteString(")\n\n")
	}

	header.Write(buffer.Bytes())

	fileLength := header.Len()

	if fileLength > 0 {
		var file *os.File
		var err error

		if *output != "-" {
			file, err = os.Create(*output)

			if err != nil {
				log.Fatal(err)
			}

			defer file.Close()
		} else {
			file = os.Stdout
		}

		header.WriteTo(file)
	}

	return fileLength, nil
}

func getSchema() []ColumnSchema {
	var host string

	if len(config.Host) > 0 && config.Port > 0 {
		host = fmt.Sprintf("tcp(%s:%d)", config.Host, config.Port)
	}

	conn, err := sql.Open("mysql", config.DbUser+":"+config.DbPassword+"@"+host+"/information_schema")

	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	q := "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, " +
		"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_TYPE, " +
		"COLUMN_KEY FROM COLUMNS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION"
	rows, err := conn.Query(q, config.DbName)
	if err != nil {
		log.Fatal(err)
	}
	columns := []ColumnSchema{}
	for rows.Next() {
		cs := ColumnSchema{}
		err := rows.Scan(&cs.TableName, &cs.ColumnName, &cs.IsNullable, &cs.DataType,
			&cs.CharacterMaximumLength, &cs.NumericPrecision, &cs.NumericScale,
			&cs.ColumnType, &cs.ColumnKey)
		if err != nil {
			log.Fatal(err)
		}
		columns = append(columns, cs)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return columns
}

func formatName(name string) string {
	parts := strings.Split(name, "_")
	newName := ""
	for _, p := range parts {
		newName = newName + strings.Replace(p, string(p[0]), strings.ToUpper(string(p[0])), 1)
	}
	return newName
}

func goType(col *ColumnSchema) (string, string, error) {
	requiredImport := ""
	if col.IsNullable == "YES" {
		requiredImport = "database/sql"
	}
	var gt string = ""
	switch col.DataType {
	case "varchar", "enum", "text", "longtext", "mediumtext":
		if col.IsNullable == "YES" {
			gt = "sql.NullString"
		} else {
			gt = "string"
		}
	case "blob", "mediumblob", "longblob":
		gt = "[]byte"
	case "date", "time", "datetime", "timestamp":
		gt, requiredImport = "time.Time", "time"
	case "tinyint", "smallint", "int", "mediumint", "bigint":
		if col.IsNullable == "YES" {
			gt = "sql.NullInt64"
		} else {
			gt = "int64"
		}
	case "float", "decimal", "double":
		if col.IsNullable == "YES" {
			gt = "sql.NullFloat64"
		} else {
			gt = "float64"
		}
	}
	if gt == "" {
		n := col.TableName + "." + col.ColumnName
		return "", "", errors.New("No compatible datatype for " + n + " found")
	}
	return gt, requiredImport, nil
}

func main() {
	flag.Parse()

	if len(*configFile) > 0 {
		f, err := os.Open(*configFile)
		if err != nil {
			log.Fatal(err)
		}
		err = json.NewDecoder(f).Decode(&config)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		config = defaults
	}

	columns := getSchema()
	bytes, err := writeStructs(columns)
	if err != nil {
		log.Fatal(err)
	}

	if *output != "-" {
		fmt.Printf("Ok %d\n", bytes)
	}
}
