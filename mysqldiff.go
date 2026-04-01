package main

import (
    "log"

    "github.com/camry/mysqldiff/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}
