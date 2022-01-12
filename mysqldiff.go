package main

import (
    "go-mysqldiff/cmd"
    "log"
)

func main() {
    if err := cmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}
