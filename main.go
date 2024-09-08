package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"strconv"
	"time"
)

var db *sql.DB

type Row struct {
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func InsertDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Error:", http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "<h1>Method Not Allowed</h1>")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error:", http.StatusBadRequest)
		fmt.Fprintf(w, "<h1>Error with getting data</h1>")
		return
	}
	var row Row
	err = json.Unmarshal(data, &row)
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with parsing data</h1>")
		return
	}

	db, err = DbConnection()
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with connecting to database</h1>")
		return
	}
	defer db.Close()

	ifExists, err := Exists(row.Id)
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with exists checking</h1>")
		return
	}
	if ifExists {
		http.Error(w, "Error:", 400)
		fmt.Fprintf(w, "<h1>Person with this id already exists</h1>")
		return
	}

	insertPattern := fmt.Sprintf("INSERT INTO users (id, name, email) VALUES (%d, '%s', '%s')", row.Id, row.Name, row.Email)
	_, err = db.Exec(insertPattern)
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with inserting data to database</h1>")
		return
	}
	fmt.Fprintf(w, "<h1>Successfully added data</h1>")
}

func DbConnection() (*sql.DB, error) {
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", viper.GetString("host"), viper.GetInt("dbport"), viper.GetString("user"), viper.GetString("password"), viper.GetString("dbname"))
	db, err := sql.Open("postgres", conn)

	if err != nil {
		fmt.Println("<h1>Error connecting to database</h1>")
	}
	return db, err
}

func AuthHandler(w http.ResponseWriter, r *http.Request) {
	db, _ = DbConnection()
	defer db.Close()
	fmt.Fprintf(w, "<h1>You are connected</h1>")
	return
}

func Exists(id int) (bool, error) {
	pattern := fmt.Sprintf("select * from users where id=%d", id)
	rows, err := db.Query(pattern)
	if err != nil {
		return false, err
	}
	var row Row
	for rows.Next() {
		rows.Scan(&row.Id, &row.Name, &row.Email)
	}
	if row.Id != id {
		return false, nil
	}
	return true, nil
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Error:", http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "<h1>Method Not Allowed</h1>")
		return
	}
	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, "Error:", 400)
		fmt.Fprintf(w, "<h1>Error with parsing id</h1>")
		return
	}
	db, err = DbConnection()
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with connecting to database</h1>")
		return
	}
	defer db.Close()
	ifExists, err := Exists(id)
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with exists checking</h1>")
		return
	}
	if !ifExists {
		http.Error(w, "Error:", 404)
		fmt.Fprintf(w, "<h1>Id not found</h1>")
		return
	}
	getPattern := fmt.Sprintf("select * from users where id=%d", id)
	dbRow, err := db.Query(getPattern)
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with getting data from database</h1>")
		return
	}
	var row Row
	for dbRow.Next() {
		dbRow.Scan(&row.Id, &row.Name, &row.Email)
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(row)
	if err != nil {
		http.Error(w, "Error:", 500)
		fmt.Fprintf(w, "<h1>Error with parsing data to json</h1>")
		return
	}
	fmt.Fprintf(w, "<h1>Success</h1>")
}

func init() {
	pflag.IntP("dbport", "d", 5432, "Port for db")
	pflag.StringP("port", "w", ":8080", "Port for webservice")
	pflag.StringP("host", "h", "localhost", "Host for db")
	pflag.StringP("user", "u", "postgres", "Username")
	pflag.StringP("password", "p", "pgpwd4habr", "Password")
	pflag.StringP("dbname", "n", "postgres", "Database name")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
}

func main() {
	mux := http.NewServeMux()
	s := &http.Server{
		Addr:         viper.GetString("port"),
		Handler:      mux,
		IdleTimeout:  10 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}
	mux.Handle("/", http.HandlerFunc(AuthHandler))
	mux.Handle("/insert", http.HandlerFunc(InsertDataHandler))
	mux.Handle("/get", http.HandlerFunc(GetHandler))

	err := s.ListenAndServe()
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
}
