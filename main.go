package main

import (
    
    "github.com/rs/cors"
    "fmt"
    "math/rand"
    "encoding/json"
    "net/http"
    "net/netip"
    "os"
    "regexp"
    "strings"
    "time"
    
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

const NAME_LENGTH = 2
const WORD_LIST = "/usr/share/dict/words"
const WORD_MIN_LENGTH = 4
const WORD_MAX_LENGTH = 8

const DB_HOST = "localhost"
const DB_USER = "partykitty"
const DB_PASSWORD = "Dh1KKsO/1KXXqS17"
const DB_DB = "party_kitty"
const DB_TABLE_PREFIX = "partykitty_"

// Period over which rate limits apply (seconds)
const RATE_LIMIT_PERIOD = 300
// Number of kitties that can be created in the rate limit period
const RATE_LIMIT_CREATE_LIMIT = 1 
// Number of kitties that can be updated in the rate limit period
const RATE_LIMIT_UPDATE_LIMIT = 2

const EXPIRATION_AFTER_LAST_UPDATE_MONTHS = 6
const EXPIRATION_AFTER_LAST_VIEW_MONTHS = 1
const EXPIRE_AFTER_NEW = true;
const EXPIRE_AFTER_UPDATE = true;
const EXPIRE_AFTER_GET = true;

var db *sql.DB

var stmtCheckBalance *sql.Stmt
var stmtExisting *sql.Stmt
var stmtInsert *sql.Stmt
var stmtLoadKitty *sql.Stmt
var stmtUpdate *sql.Stmt
var stmtExpireRateLimit *sql.Stmt
var stmtRecentUnnamedActions *sql.Stmt
var stmtRecentNamedActions *sql.Stmt
var stmtAddNamedAction *sql.Stmt
var stmtAddUnnamedAction *sql.Stmt
var stmtExpireOldData *sql.Stmt

var dict Dictionary

type Dictionary struct {
    words []string
}

func createDictionary() (Dictionary, error) {
    data, err := os.ReadFile(WORD_LIST)
    if err != nil {
        return Dictionary{}, err
    }
    words := strings.Split(string(data), "\n")
    filteredWords := make([]string, 0, len(words))
    
    validWord := regexp.MustCompile(`^[a-z]+$`)
    
    for i := 0; i < len(words); i++ {
        word := words[i]
        if len(word) >= WORD_MIN_LENGTH && len(word) <= WORD_MAX_LENGTH && validWord.MatchString(word) {
            filteredWords = append(filteredWords, word)
        }
    }
    
    return Dictionary{filteredWords}, nil
}

func (d Dictionary) randomKittyName() (string, error) {
    // Look up words that have already been used
    res, err := stmtExisting.Query()
    if err != nil {
        return "", err
    }
    
    // This map is equivalent to a set and easy to find if a name exists
    var existingNames = make(map[string]struct{})
    // Loop over the results to find all existing word combinations
    for res.Next() {
        var name string
        err = res.Scan(&name)
        if err != nil {
            return "", err
        }
        existingNames[name] = struct{}{}
    }
    
    // Loop until we have a word that hasn't been used before
    var newName string
    for {
        l := len(d.words)
        word1 := d.words[rand.Intn(l)]
        word2 := d.words[rand.Intn(l)]
        
        // Check if this word has already been used
        newName = fmt.Sprintf("%s-%s", word1, word2)
        
        _, exists := existingNames[newName]
        if !exists {
            break
        }
        
    }
    
    return newName, nil
}

func handleGet(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    name, err := kittyNameFromString(query.Get("name"))
    
    if err != nil {
        w.WriteHeader(http.StatusNotFound)
        return
    }
    
    kittyData, err := loadData(name)
    
    if err != nil {
        if err == sql.ErrNoRows {
            w.WriteHeader(http.StatusNotFound)
            return
        }
    }
    
    // TODO: Check if kitty has been modified since if_modified_since
    err = json.NewEncoder(w).Encode(kittyData)
    if err != nil {
        fmt.Printf("Error encoding kitty data from database - kitty %s\n", name)
        fmt.Fprintf(w, `{
                "error": "LOAD_DATA_ERROR",
                "errorParam": "%s",
            }`, name)
        w.WriteHeader(http.StatusInternalServerError)
    }
    
    if EXPIRE_AFTER_GET {
        expireData()
    }
}

func loadData(name KittyName) (KittyData, error) {
    var data KittyData
    var loadedName string
    
    row := stmtLoadKitty.QueryRow(name.format())
    
    err := row.Scan(
        &loadedName,
        &data.currencySet,
        &data.amount,
        &data.partySize,
        &data.splitRatio,
        &data.config,
        &data.last_update,
        &data.last_view,
    )
    if err != nil {
        return KittyData{}, err
    }
        
    data.name, err = kittyNameFromString(loadedName)
    if err != nil {
        return KittyData{}, err
    }
            
    return data, nil
}

func handlePut(w http.ResponseWriter, r *http.Request) {
    // PUT - create a new kitty with a random name
    var putData *IncomingData
    
    err := json.NewDecoder(r.Body).Decode(&putData)
    
    strName, err := dict.randomKittyName()
    name, err := kittyNameFromString(strName)
    
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, `{
            "error": "INVALID_NAME",
            "errorParam": "%s",
        }`, name)
        return
    }
    
    // Stop processing if rate limit reached
    if applyRateLimits(r, w, "create", nil) {
        w.WriteHeader(http.StatusTooManyRequests)
        return
    }
    
    jsonAmount, err := json.Marshal(putData.Amount)
    jsonConfig, err := json.Marshal(putData.Config)
    
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, `{
            "error": "INVALID_DATA"
        }`)
        return
    }
        
    _, err = stmtInsert.Exec(
        name.format(), 
        putData.Currency,
        jsonAmount,
        int(putData.PartySize),
        int(putData.SplitRatio),
        jsonConfig,
    )

    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Println("Error writing to database on PUT:")
        fmt.Println(err.Error())
        return
    }
    
    kittyData, err := loadData(name)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Println("Error reading from database on PUT:")
        fmt.Println(err.Error())
        return
    }
    
    // TODO: In PHP, we only return name, amount & lastUpdate
    err = json.NewEncoder(w).Encode(kittyData)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Println("Error returning saved data to browser on PUT:")
        fmt.Println(err.Error())
        return
    }
    
    // Expire old data
    
    if EXPIRE_AFTER_NEW {
        expireData()
    }
}

type IncomingData struct {
    // Fields must be exported, i.e. start with capital
    Amount           map[string]int     `json:"amount"`
    Currency         string             `json:"currency"`
    Config           map[string]string  `json:"config"`
    LastUpdate       time.Time          `json:"lastUpdate"`
    LastUpdateAmount map[string]int     `json:"lastUpdateAmount"`
    Name             string             `json:"name"`
    PartySize        int                `json:"partySize"`
    SplitRatio       int                `json:"splitRatio"`
}

func handlePost(w http.ResponseWriter, r *http.Request) {
    // POST - update an existing kitty
    var postData *IncomingData
    
    err := json.NewDecoder(r.Body).Decode(&postData)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, `{
            "error": "INVALID_DATA"
        }`)
        return
    }
    
    name, err := kittyNameFromString(postData.Name)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, `{
            "error": "INVALID_NAME",
            "errorParam": "%s",
        }`, name)
        return
    }
    
    // Stop processing if rate limit reached
    if applyRateLimits(r, w, "update", &name) {
        w.WriteHeader(http.StatusTooManyRequests)
        return
    }
    
    // Get balance before update
    row := stmtCheckBalance.QueryRow(name.format())
    
    var serverLastUpdate time.Time
    var clientLastUpdate time.Time
    var beforeAmountRaw string
    var beforeAmount map[string]int
        
    err = row.Scan(&serverLastUpdate, &beforeAmountRaw)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Println("Error loading existing balance for kitty "+name.format());
        fmt.Println(err.Error())
        return
    }
    
    json.Unmarshal([]byte(beforeAmountRaw), &beforeAmount)
        
    if clientLastUpdate.After(serverLastUpdate) {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, `{"error": "INVALID_LAST_UPDATE"}`)
        return
    }
    
    // Calculate the diff the client is sending
    newValue := make(map[string]int)
    
    for currency, serverValue := range beforeAmount {
        clientLastValue, ok := postData.LastUpdateAmount[currency]
        if !ok {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprintf(w, `{
                "error": "CURRENCY_MISSING_LAST_UPDATE",
                "errorParam": "%s",
            }`, currency)
        }
        clientValue, ok := postData.Amount[currency]
        if !ok {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprintf(w, `{
                "error": "CURRENCY_MISSING_AMOUNT",
                "errorParam": "%s"
            }`, currency)
        }
        
        // TODO: Maybe allow floats, but use ints if possible
        if (clientLastValue == serverValue) {
            newValue[currency] = clientValue
        } else {
            currencyDiff := clientValue - clientLastValue
            newValue[currency] = serverValue + currencyDiff
        }
        
    }
    
    jsonNewValue, err := json.Marshal(newValue)
    jsonConfig, err := json.Marshal(postData.Config)
    
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, `{
            "error": "INVALID_DATA",
        }`, name)
        return
    }
    
    _, err = stmtUpdate.Exec(
        postData.Currency,
        jsonNewValue,
        int(postData.PartySize),
        int(postData.SplitRatio),
        jsonConfig,
        name.format(),
    )
        
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        return
    }
    
    kittyData, err := loadData(name)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Println("Error reading from database on POST:")
        fmt.Println(err.Error())
        return
    }
    
    // TODO: In PHP, we only return name, amount & lastUpdate
    err = json.NewEncoder(w).Encode(kittyData)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        fmt.Println("Error returning saved data to browser on POST:")
        fmt.Println(err.Error())
        return
    }
    
    // Expire old data
    if EXPIRE_AFTER_UPDATE {
        expireData()
    }
}

func initDb() error {
    // Create connection pool
    var err error 
    db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s?parseTime=true", DB_USER, DB_PASSWORD, DB_HOST, DB_DB))
    if err != nil {
        return err
    }
    
    // Ping connection to check server
    err = db.Ping()
    if err != nil {
        return err
    }
    
    db.SetConnMaxLifetime(time.Minute * 3)
    db.SetMaxOpenConns(3)
    db.SetMaxIdleConns(3)
    
    stmtExisting, err = db.Prepare("SELECT name FROM " + DB_TABLE_PREFIX + "data")
    if err != nil {
        return err
    }
    
    // Load an existing kitty
    stmtLoadKitty, err = db.Prepare("SELECT name, currencySet, amount, partySize, splitRatio, config, last_update, last_view FROM "+DB_TABLE_PREFIX+"data WHERE name = ? LIMIT 1")
    if err != nil {
        return err
    }
    
    // Create a new kitty
    // TODO: Can I use named parameters?
    // TODO: Can I use linebreaks?
    stmtInsert, err = db.Prepare("INSERT INTO "+DB_TABLE_PREFIX+"data SET name=?, currencySet=?, amount=?, partySize=?, splitRatio=?, config=?, last_update=UTC_TIMESTAMP(), last_view=UTC_TIMESTAMP()")
    
    if err != nil {
        return err
    }
    
    // Get balance for an existing kitty
    stmtCheckBalance, err = db.Prepare("SELECT last_update, amount FROM "+DB_TABLE_PREFIX+"data WHERE name = ? LIMIT 1")
    if err != nil {
        return err
    }
    
    // Update an existing kitty
    // TODO: Can I use named parameters?
    // TODO: Can I use linebreaks?
    stmtUpdate, err = db.Prepare("UPDATE "+DB_TABLE_PREFIX+"data SET currencySet=?, amount=?, partySize=?, splitRatio=?, config=?, last_update=UTC_TIMESTAMP(), last_view=UTC_TIMESTAMP() WHERE name=?")
    
    if err != nil {
        return err
    }
    
    // Expire rate limits
    stmtExpireRateLimit, err = db.Prepare("DELETE FROM "+DB_TABLE_PREFIX+"ratelimit WHERE timestamp < ?")
    
    if err != nil {
        return err
    }
    
    // Recent actions by IP
    stmtRecentNamedActions, err = db.Prepare("SELECT COUNT(1) FROM "+DB_TABLE_PREFIX+"ratelimit WHERE IP = ? AND action = ? AND kitty = ?")
    
    if err != nil {
        return err
    }
    stmtRecentUnnamedActions, err = db.Prepare("SELECT COUNT(1) FROM "+DB_TABLE_PREFIX+"ratelimit WHERE IP = ? AND action = ?")
    
    if err != nil {
        return err
    }
    
    // Add to rate limit table
    stmtAddNamedAction, err = db.Prepare("REPLACE INTO "+DB_TABLE_PREFIX+"ratelimit SET ip=?, action=?, kitty=?, timestamp=UTC_TIMESTAMP()")
    
    if err != nil {
        return err
    }
    
    // Add to rate limit table
    stmtAddUnnamedAction, err = db.Prepare("REPLACE INTO "+DB_TABLE_PREFIX+"ratelimit SET ip=?, action=?, kitty=NULL, timestamp=UTC_TIMESTAMP()")
    
    if err != nil {
        return err
    }
    
    // Expire old data
    stmtExpireOldData, err = db.Prepare("DELETE FROM "+DB_TABLE_PREFIX+"data WHERE last_update < ? AND last_view < ?")
    
    if err != nil {
        return err
    }
    
    return nil
}

func main() {
    
    err := initDb()
    
    if err != nil {
        panic("Could not initialise database")
    }
    
    dict, err = createDictionary()
    
    if err != nil {
        panic("Could not initialise word list")
    }
    
    mux := http.NewServeMux()

    mux.HandleFunc("/", func (w http.ResponseWriter, r *http.Request) {
        switch r.Method {
            case http.MethodGet:
                handleGet(w, r)
            case http.MethodPut:
                handlePut(w, r)
            case http.MethodPost:
                handlePost(w, r)
        }
    });
    
    c := cors.New(cors.Options{
        AllowedOrigins: []string{"*"},
        AllowedMethods: []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodOptions},
        AllowedHeaders: []string{"HTTP_IF_MODIFIED_SINCE"},
    })
    
    http.ListenAndServe(":8000", c.Handler(mux))
}

func expireData() {
    lastUpdateTime := time.Now().AddDate(0, EXPIRATION_AFTER_LAST_UPDATE_MONTHS*-1, 0)
    lastViewTime := time.Now().AddDate(0, EXPIRATION_AFTER_LAST_VIEW_MONTHS*-1, 0)
    
    res, err := stmtExpireOldData.Exec(lastUpdateTime, lastViewTime)
    
    if err != nil {
        fmt.Println("Unable to expire data")
        fmt.Println(err.Error())
    }
    
    rowsAffected, err := res.RowsAffected()
    
    if err == nil {    
        fmt.Printf("%d kitty/ies were expired due to lack of activity", rowsAffected)
    }
}

/**
 * Check rate limits applying to the current request and terminate with appropriate error
 * 
 * Returns true if rate limits were applied and the request should be ignored
 */
func applyRateLimits(r *http.Request, w http.ResponseWriter, action string, kitty *KittyName) bool {
    addr, _, _ := strings.Cut(r.RemoteAddr, ":")
    ip, err := netip.ParseAddr(addr); // TODO: Handle proxies
    if err != nil {
        fmt.Println("Could not parse remote IP: "+r.RemoteAddr+", rejecting request")
        return true
    }
    
    if !checkRateLimits(ip, action, kitty) {
        retryAfter := RATE_LIMIT_PERIOD
        w.WriteHeader(429)
        w.Header().Add("Retry-After", string(retryAfter))
        var output = make(map[string]string)
        output["RetryAfter"] = string(retryAfter)
        jsonOutput, err := json.Marshal(output)
        if err == nil {
            w.Write(jsonOutput)
        }
        return true
    }
    
    return false
}

/**
 * Expire any rate limit data from before the rate limit period
 */
func expireRateLimits() {
    rateLimit := time.Duration(RATE_LIMIT_PERIOD * float64(time.Second) * -1)
    expiration := time.Now().Add(rateLimit)
    stmtExpireRateLimit.Exec(expiration)
}

/**
 * Checks the rate limits that apply to an action
 * 
 * Returns true or false to indicate if this action is allowed.
 * If the result is true, the action is stored in the rate limit table.
 * Outdated rate limit data is deleted
 */
func checkRateLimits(ip netip.Addr, action string, kitty *KittyName) bool {
    var appliedLimit int
    var result *sql.Row
    
    expireRateLimits()
    
    switch action {
        case "create":
            // TODO: Handle blank rate limit
            appliedLimit = RATE_LIMIT_CREATE_LIMIT
            result = stmtRecentUnnamedActions.QueryRow(ip.String(), action)
        case "update":
            appliedLimit = RATE_LIMIT_UPDATE_LIMIT
            result = stmtRecentNamedActions.QueryRow(ip.String(), action, kitty.format())
        default:
            return false
    }
    
    
    var recentActions int
    err := result.Scan(&recentActions)
    
    if err != nil {
        fmt.Println("Unable to check rate limits:")
        fmt.Println(err.Error())
    }
    
    if recentActions >= appliedLimit {
        return false // Apply a rate limit
    }
    
    // Rate limit NOT applied, add a new record for this action
    if kitty == nil {
        _, err = stmtAddUnnamedAction.Exec(ip.String(), action)
    } else {
        _, err = stmtAddNamedAction.Exec(ip.String(), action, kitty.format())
    }
    
    if err != nil {
        fmt.Println("Unable to track rate limits:")
        fmt.Println(err.Error())
    }
    return true
}
