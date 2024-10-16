package main

import (
    "github.com/rs/cors"
    "fmt"
    "math"
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

// TODO: negative config looks bad
const RATE_LIMIT_PERIOD = time.Duration(-300 * float64(time.Second))
const RATE_LIMIT_CREATE_LIMIT = 1
const RATE_LIMIT_UPDATE_LIMIT = 2

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

var dict Dictionary

func check(e error) {
    if e != nil {
        panic(e)
    }
}

type Dictionary struct {
    words []string
}

type KittyName struct {
    name [NAME_LENGTH]string
}

func (n KittyName) format() string {
    return fmt.Sprintf("%s-%s", n.name[0], n.name[1])
}

type KittyData struct {
    name KittyName
    currencySet string
    amount string
    partySize int
    splitRatio int
    config string
    last_update time.Time
    last_view time.Time
}

func (k KittyData) MarshalJSON() ([]byte, error) {
    name := k.name.format()
    return []byte(fmt.Sprintf(
        // Keys MUST be quoted, or Go will throw a tantrum and not say why
        `{"name": "%[1]s", "currencySet": "%[2]s", "amount": %[3]s, "partySize": %[4]d, "splitRatio": %[5]d, "config": %[6]s, "lastUpdate": "%[7]s", "lastView": "%[8]s"}`,
        name,
        k.currencySet,
        k.amount,
        k.partySize,
        k.splitRatio,
        k.config,
        k.last_update.Format(time.RFC3339),
        k.last_view.Format(time.RFC3339),
    )), nil
}

type InvalidNameError struct {
    name string
}

func (e InvalidNameError) Error() string {
    return fmt.Sprintf("Invalid name: %s", e.name)
}

func kittyNameFromString(input string) (KittyName, error) {
    tempName := strings.ToLower(input)
    arrName := strings.Split(tempName, "-")
    validName := regexp.MustCompile(`^[A-z]+-[A-z]+$`)
    if len(arrName) != NAME_LENGTH || !validName.MatchString(tempName) {
        return KittyName{}, InvalidNameError{name: input}
    }
    
    var parsed [NAME_LENGTH]string
    copy(parsed[:], arrName[0:NAME_LENGTH])
    return KittyName{parsed}, nil
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
        panic(err.Error())
    }
    
    // This map is equivalent to a set and easy to find if a name exists
    var existingNames = make(map[string]struct{})
    // Loop over the results to find all existing word combinations
    for res.Next() {
        var name string
        err = res.Scan(&name)
        if err != nil {
            panic(err.Error())
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
        
        fmt.Println("%s has already been used", newName)
        
    }
    fmt.Println("%s is a new name", newName)
    
    return newName, nil
}

func handleGet(w http.ResponseWriter, r *http.Request) {
    query := r.URL.Query()
    name, err := kittyNameFromString(query.Get("name"))
    
    if err != nil {
        fmt.Println("Error getting kitty name")
        w.WriteHeader(http.StatusNotFound)
        return
    }
    
    kittyData, err := loadData(name)
    
    if err != nil {
        if err == sql.ErrNoRows {
            fmt.Println("Empty result set")
            w.WriteHeader(404)
            return
        }
        panic(err.Error())
    }
    
    // TODO: Check if kitty has been modified since if_modified_since
    err = json.NewEncoder(w).Encode(kittyData)
    if err != nil {
        panic(err.Error())
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
        panic(err.Error())
    }
    
    // Stop processing if rate limit reached
    if applyRateLimits(r, w, "create", nil) {
        return
    }
    
    fmt.Println(putData)
    fmt.Println(name)
    
    jsonAmount, err := json.Marshal(putData.Amount)
    jsonConfig, err := json.Marshal(putData.Config)
    
    if err != nil {
        panic(err.Error())
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
        panic(err.Error())
    }
    
    kittyData, err := loadData(name)
    if err != nil {
        panic(err.Error())
    }
    
    // TODO: In PHP, we only return name, amount & lastUpdate
    err = json.NewEncoder(w).Encode(kittyData)
    if err != nil {
        panic(err.Error())
    }
    
    // TODO: Expire old data
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
        panic(err.Error())
    }
    fmt.Println(postData)
    
    name, err := kittyNameFromString(postData.Name)
    if err != nil {
        panic(err.Error())
    }
    
    // Stop processing if rate limit reached
    if applyRateLimits(r, w, "update", &name) {
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
        panic(err.Error())
    }
    
    json.Unmarshal([]byte(beforeAmountRaw), &beforeAmount)
    
    fmt.Println(serverLastUpdate)
    fmt.Println(clientLastUpdate)
    fmt.Println(beforeAmount)
    
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
        
        fmt.Printf("(%s): Client last: %d, server last: %d, client new: %d\n", currency, clientLastValue, serverValue, clientValue)
        // TODO: Maybe allow floats, but use ints if possible
        if (clientLastValue == serverValue) {
            newValue[currency] = clientValue
        } else {
            currencyDiff := clientValue - clientLastValue
            newValue[currency] = serverValue + currencyDiff
        }
        
        fmt.Printf("%s = %d; ", currency, serverValue)
    }
    
    fmt.Println(newValue)
    jsonNewValue, err := json.Marshal(newValue)
    fmt.Printf("JSON: %s\n",jsonNewValue)
    fmt.Println("^^^^")
    jsonConfig, err := json.Marshal(postData.Config)
    
    res, err := stmtUpdate.Exec(
        postData.Currency,
        jsonNewValue,
        int(postData.PartySize),
        int(postData.SplitRatio),
        jsonConfig,
        name.format(),
    )
    
    fmt.Println(res)
    
    if err != nil {
        panic(err.Error())
    }
    
    kittyData, err := loadData(name)
    if err != nil {
        panic(err.Error())
    }
    
    // TODO: In PHP, we only return name, amount & lastUpdate
    err = json.NewEncoder(w).Encode(kittyData)
    if err != nil {
        panic(err.Error())
    }
    
    // TODO: Expire old data
        
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
        panic(err.Error())
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
        retryAfter := int(math.Ceil(RATE_LIMIT_PERIOD.Seconds()))
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
    expiration := time.Now().Add(RATE_LIMIT_PERIOD)
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
        panic(err.Error())
    }
    
    if recentActions >= appliedLimit {
        fmt.Println("Applied rate limit")
        return false // Apply a rate limit
    }
    
    // Rate limit NOT applied, add a new record for this action
    fmt.Println("Rate limit OK, logging action")
    if kitty == nil {
        _, err = stmtAddUnnamedAction.Exec(ip.String(), action)
    } else {
        _, err = stmtAddNamedAction.Exec(ip.String(), action, kitty.format())
    }
    
    if err != nil {
        panic(err.Error())
    }
    return true
}
