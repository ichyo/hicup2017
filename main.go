package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// User is the type of users.json in data.zip
type User struct {
	ID        int32  `json:"id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Gender    string `json:"gender"`
	BirthDate int64  `json:"birth_date"`
}

// Location is the type of locations.json in data.zip
type Location struct {
	ID       int32  `json:"id"`
	Place    string `json:"place"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Distance int64  `json:"distance"`
}

// Visit is the type of visits.json in data.zip
type Visit struct {
	ID        int32 `json:"id"`
	Location  int32 `json:"location"`
	User      int32 `json:"user"`
	VisitedAt int64 `json:"visited_at"`
	Mark      int8  `json:"mark"`
}

// Users is the json type in data.zip
type Users struct {
	Users []*User `json:"users"`
}

// Locations is the json type in data.zip
type Locations struct {
	Locations []*Location `json:"locations"`
}

// Visits is the json type in data.zip
type Visits struct {
	Visits []*Visit `json:"visits"`
}

// VisitPlace is the response type of /user/{id}/visits endpoint
type VisitPlace struct {
	Place     string `json:"place"`
	VisitedAt int64  `json:"visited_at"`
	Mark      int8   `json:"mark"`
}

// UserUpdate is the request type of POST /users/{id}
type UserUpdate struct {
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Gender    string `json:"gender"`
	BirthDate int64  `json:"birth_date"`
}

// LocationUpdate is the request type of POST /locations/{id}
type LocationUpdate struct {
	Place    string `json:"place"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Distance int64  `json:"distance"`
}

// VisitUpdate is the request type of POST /visits/{id}
type VisitUpdate struct {
	Location  int32 `json:"location"`
	User      int32 `json:"user"`
	VisitedAt int64 `json:"visited_at"`
	Mark      int8  `json:"mark"`
}

// InmemoryDB stores everything in memory
type InmemoryDB struct {
	mux       sync.RWMutex
	users     map[int32]*User
	locations map[int32]*Location
	visits    map[int32]*Visit
}

func newInmemoryDB() *InmemoryDB {
	db := InmemoryDB{}
	db.users = make(map[int32]*User)
	db.locations = make(map[int32]*Location)
	db.visits = make(map[int32]*Visit)
	return &db
}

var (
	errConflictID = errors.New("resource id is conflict")
)

var (
	db = newInmemoryDB()
)

func (d *InmemoryDB) addUser(user *User) error {
	d.mux.Lock()
	defer d.mux.Unlock()

	if _, ok := d.users[user.ID]; ok {
		return errConflictID
	}
	d.users[user.ID] = user
	return nil
}

func (d *InmemoryDB) addLocation(location *Location) error {
	d.mux.Lock()
	defer d.mux.Unlock()

	if _, ok := d.locations[location.ID]; ok {
		return errConflictID
	}
	d.locations[location.ID] = location
	return nil
}

func (d *InmemoryDB) addVisit(visit *Visit) error {
	d.mux.Lock()
	defer d.mux.Unlock()

	if _, ok := d.visits[visit.ID]; ok {
		return errConflictID
	}
	d.visits[visit.ID] = visit
	return nil
}

func (d *InmemoryDB) getUser(id int32) *User {
	d.mux.RLock()
	defer d.mux.RUnlock()

	return d.users[id]
}

func (d *InmemoryDB) getLocation(id int32) *Location {
	d.mux.RLock()
	defer d.mux.RUnlock()

	return d.locations[id]
}

func (d *InmemoryDB) getVisit(id int32) *Visit {
	d.mux.RLock()
	defer d.mux.RUnlock()

	return d.visits[id]
}

type visitsByTime []VisitPlace

func (a visitsByTime) Len() int           { return len(a) }
func (a visitsByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a visitsByTime) Less(i, j int) bool { return a[i].VisitedAt < a[j].VisitedAt }

func (d *InmemoryDB) queryVisits(userID int32, fromDate int64, toDate int64, country string, toDistance int64) []VisitPlace {
	d.mux.RLock()
	defer d.mux.RUnlock()

	visits := make([]VisitPlace, 0)

	for _, v := range d.visits {
		if userID != v.User {
			continue
		}
		if fromDate >= v.VisitedAt {
			continue
		}
		if toDate <= v.VisitedAt {
			continue
		}
		location := db.getLocation(v.Location)
		if len(country) != 0 && country != location.Country {
			continue
		}
		if toDistance <= location.Distance {
			continue
		}
		visit := VisitPlace{
			Mark:      v.Mark,
			VisitedAt: v.VisitedAt,
			Place:     location.Place,
		}
		visits = append(visits, visit)
	}

	sort.Sort(visitsByTime(visits))

	return visits
}

// TODO: int64 is too large for ages
func computeAge(birth int64) int64 {
	now := time.Now()
	birthTime := time.Unix(birth, 0)
	years := now.Year() - birthTime.Year()
	if now.Month() < birthTime.Month() ||
		now.Month() == birthTime.Month() && now.Day() < birthTime.Day() {
		years--
	}
	return int64(years)
}

func (d *InmemoryDB) queryAverage(locationID int32, fromDate int64, toDate int64, fromAge int64, toAge int64, gender string) float64 {
	d.mux.RLock()
	defer d.mux.RUnlock()

	count := int64(0)
	sum := int64(0)

	for _, v := range d.visits {
		if locationID != v.Location {
			continue
		}

		if fromDate >= v.VisitedAt {
			continue
		}
		if toDate <= v.VisitedAt {
			continue
		}
		user := db.getUser(v.User)

		if len(gender) != 0 && gender != user.Gender {
			continue
		}

		age := computeAge(user.BirthDate)
		if fromAge >= age {
			continue
		}
		if toAge <= age {
			continue
		}

		count++
		sum += int64(v.Mark)
	}

	return float64(sum) / float64(count)
}

func unmarshalFromFile(f *zip.File, v interface{}) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	bs, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bs, v)
	if err != nil {
		return err
	}
	return nil
}

func initializeData(dataDir string) error {
	zipPath := fmt.Sprintf("%s/data.zip", dataDir)
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		log.Println("Loading", f.Name)
		if strings.HasPrefix(f.Name, "users") {
			var users Users
			err := unmarshalFromFile(f, &users)
			if err != nil {
				return err
			}
			for _, u := range users.Users {
				db.addUser(u)
			}
		}
		if strings.HasPrefix(f.Name, "locations") {
			var locations Locations
			err := unmarshalFromFile(f, &locations)
			if err != nil {
				return err
			}
			for _, l := range locations.Locations {
				db.addLocation(l)
			}
		}
		if strings.HasPrefix(f.Name, "visits") {
			var visits Visits
			err := unmarshalFromFile(f, &visits)
			if err != nil {
				return err
			}
			for _, v := range visits.Visits {
				db.addVisit(v)
			}
		}
	}

	return nil
}

func parseInt32(s string) (int32, error) {
	id, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(id), nil
}

func parseInt64OrDefault(s string, d int64) (int64, error) {
	if len(s) == 0 {
		return d, nil
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return int64(id), nil
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := parseInt32(vars["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	user := db.getUser(id)
	if user == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(user)
	if err != nil {
		log.Println(err)
	}
}

func getLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := parseInt32(vars["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	location := db.getLocation(id)
	if location == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(location)
	if err != nil {
		log.Println(err)
	}
}

func getVisitHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := parseInt32(vars["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	visit := db.getVisit(id)
	if visit == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(visit)
	if err != nil {
		log.Println(err)
	}
}

func getUserVisitsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	userID, err := parseInt32(vars["userID"])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	user := db.getUser(userID)
	if user == nil {
		http.NotFound(w, r)
		return
	}

	query := r.URL.Query()
	fromDate, err := parseInt64OrDefault(query.Get("fromDate"), math.MinInt64)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	toDate, err := parseInt64OrDefault(query.Get("toDate"), math.MaxInt64)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	country := query.Get("country")
	toDistance, err := parseInt64OrDefault(query.Get("toDistance"), math.MaxInt64)

	visits := db.queryVisits(userID, fromDate, toDate, country, toDistance)

	response := struct {
		Visits []VisitPlace `json:"visits"`
	}{Visits: visits}

	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Println(err)
	}
}

func getLocationAverageHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	locationID, err := parseInt32(vars["locationID"])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	location := db.getLocation(locationID)
	if location == nil {
		http.NotFound(w, r)
		return
	}

	query := r.URL.Query()
	fromDate, err := parseInt64OrDefault(query.Get("fromDate"), math.MinInt64)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	toDate, err := parseInt64OrDefault(query.Get("toDate"), math.MaxInt64)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	fromAge, err := parseInt64OrDefault(query.Get("fromAge"), math.MinInt64)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	toAge, err := parseInt64OrDefault(query.Get("toAge"), math.MaxInt64)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	gender := query.Get("gender")

	average := db.queryAverage(locationID, fromDate, toDate, fromAge, toAge, gender)
	response := struct {
		Avg string `json:"avg"`
	}{Avg: fmt.Sprintf("%.5f", average)}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Println(err)
	}
}

func updateUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	userID, err := parseInt32(vars["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	user := db.getUser(userID)
	if user == nil {
		http.NotFound(w, r)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var userUpdate UserUpdate
	err = decoder.Decode(&userUpdate)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	// TODO: check null in json?

	user.Email = userUpdate.Email
	user.FirstName = userUpdate.FirstName
	user.LastName = userUpdate.LastName
	user.Gender = userUpdate.Gender
	user.BirthDate = userUpdate.BirthDate

	_, err := w.Write([]byte("{}"))
	if err != nil {
		log.Println(err)
	}
}

func updateLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	locationID, err := parseInt32(vars["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	location := db.getLocation(locationID)
	if location == nil {
		http.NotFound(w, r)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var locationUpdate LocationUpdate
	err = decoder.Decode(&locationUpdate)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	// TODO: check null in json?

	location.Place = locationUpdate.Place
	location.Country = locationUpdate.Country
	location.City = locationUpdate.City
	location.Distance = locationUpdate.Distance

	_, err := w.Write([]byte("{}"))
	if err != nil {
		log.Println(err)
	}
}

func updateVisitHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	visitID, err := parseInt32(vars["id"])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	visit := db.getVisit(visitID)
	if visit == nil {
		http.NotFound(w, r)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var visitUpdate VisitUpdate
	err = decoder.Decode(&visitUpdate)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}
	// TODO: check null in json?

	visit.Location = visitUpdate.Location
	visit.User = visitUpdate.User
	visit.VisitedAt = visitUpdate.VisitedAt
	visit.Mark = visitUpdate.Mark

	w.Write([]byte("{}"))
}

func newUserHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var user User
	err := decoder.Decode(&user)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}

	err = db.addUser(&user)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}

	w.Write([]byte("{}"))
}

func newLocationHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var location Location
	err := decoder.Decode(&location)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}

	err = db.addLocation(&location)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}

	w.Write([]byte("{}"))
}

func newVisitHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var visit Visit
	err := decoder.Decode(&visit)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}

	err = db.addVisit(&visit)
	if err != nil {
		http.Error(w, "Bad Request", 400)
		return
	}

	w.Write([]byte("{}"))
}

func main() {
	port := flag.Int("port", 8080, "port number")
	dataDir := flag.String("data", "./data/", "data directory for initialization")
	flag.Parse()

	err := initializeData(*dataDir)
	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/users/new", newUserHandler).Methods("GET", "POST")
	r.HandleFunc("/locations/new", newLocationHandler).Methods("GET", "POST")
	r.HandleFunc("/visits/new", newVisitHandler).Methods("GET", "POST")
	r.HandleFunc("/users/{id}", getUserHandler).Methods("GET")
	r.HandleFunc("/locations/{id}", getLocationHandler).Methods("GET")
	r.HandleFunc("/visits/{id}", getVisitHandler).Methods("GET")
	r.HandleFunc("/users/{userID}/visits", getUserVisitsHandler).Methods("GET")
	r.HandleFunc("/locations/{locationID}/avg", getLocationAverageHandler).Methods("GET")
	r.HandleFunc("/users/{id}", updateUserHandler).Methods("POST")
	r.HandleFunc("/locations/{id}", updateLocationHandler).Methods("POST")
	r.HandleFunc("/visits/{id}", updateVisitHandler).Methods("POST")

	http.Handle("/", r)

	addr := fmt.Sprintf(":%d", *port)
	log.Println("Start running on", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
