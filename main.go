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

	"github.com/gorilla/mux"
)

// User profile
type User struct {
	ID        int32  `json:"id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Gender    string `json:"gender"`
	BirthDate int64  `json:"birth_date"`
}

// Location info
type Location struct {
	ID       int32  `json:"id"`
	Place    string `json:"place"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Distance int64  `json:"distance"`
}

// Visit by a specific user to a particular place
type Visit struct {
	ID        int32 `json:"id"`
	Location  int32 `json:"location"`
	User      int32 `json:"user"`
	VisitedAt int64 `json:"visited_at"`
	Mark      int8  `json:"mark"`
}

// VisitPlace is the type for /user/{id}/visits endpoint
type VisitPlace struct {
	Place     string `json:"place"`
	VisitedAt int64  `json:"visited_at"`
	Mark      int8   `json:"mark"`
}

type users struct {
	Users []*User `json:"users"`
}

type locations struct {
	Locations []*Location `json:"locations"`
}

type visits struct {
	Visits []*Visit `json:"visits"`
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
		// TODO: use fromAge toAge

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
			var users users
			err := unmarshalFromFile(f, &users)
			if err != nil {
				return err
			}
			for _, u := range users.Users {
				db.addUser(u)
			}
		}
		if strings.HasPrefix(f.Name, "locations") {
			var locations locations
			err := unmarshalFromFile(f, &locations)
			if err != nil {
				return err
			}
			for _, l := range locations.Locations {
				db.addLocation(l)
			}
		}
		if strings.HasPrefix(f.Name, "visits") {
			var visits visits
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
	json.NewEncoder(w).Encode(user)
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
	json.NewEncoder(w).Encode(location)
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
	json.NewEncoder(w).Encode(visit)
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
	json.NewEncoder(w).Encode(response)
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
	json.NewEncoder(w).Encode(response)
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
	r.HandleFunc("/users/{id}", getUserHandler).Methods("GET")
	r.HandleFunc("/locations/{id}", getLocationHandler).Methods("GET")
	r.HandleFunc("/visits/{id}", getVisitHandler).Methods("GET")
	r.HandleFunc("/users/{userID}/visits", getUserVisitsHandler).Methods("GET")
	r.HandleFunc("/locations/{locationID}/avg", getLocationAverageHandler).Methods("GET")

	http.Handle("/", r)

	addr := fmt.Sprintf(":%d", *port)
	log.Println("Start running on", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
