package main

import (
    "fmt"
    "regexp"
    "strings"
    "time"
)

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
